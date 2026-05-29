//! TCP echo demo built entirely from `rust_net`'s typed layers (Linux only).
//!
//! Both ends run on a *single* `IoTaskRunner` (one epoll thread):
//!
//!  - **Server** — a [`TcpServerSocket`] listens on an ephemeral port, accepts
//!    connections, and echoes whatever it reads back to the peer. `accept` is
//!    one-shot, so the accept loop re-arms itself from inside its own callback.
//!
//!  - **Client** — a [`TcpClientSocket`] connects, then runs three
//!    request/response rounds, kicking off the next send from the previous
//!    round's read callback.
//!
//! Everything is callback-driven on the IO thread, so there is no locking
//! between operations — only a `Barrier` to let `main` wait for completion and
//! an `mpsc` channel to hand the kernel-assigned port back to `main`.
//!
//! Run with:
//!   cargo run --example tcp_echo

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
    #[cfg(not(target_os = "linux"))]
    eprintln!("This example requires Linux (epoll + eventfd).");
}

#[cfg(target_os = "linux")]
mod linux {
    use rust_io::IoTaskRunner;
    use rust_net::{TcpClientSocket, TcpServerSocket};
    use rust_task::TaskRunner;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier, Mutex};

    // ── server ────────────────────────────────────────────────────────────────

    /// Echo one connection: read → write the same bytes back → read again.
    /// An empty read means the peer closed the connection, so the loop ends and
    /// the last `Arc<TcpClientSocket>` for this peer drops.
    fn echo_connection(peer: Arc<TcpClientSocket>) {
        let p = Arc::clone(&peer);
        peer.read(1024, move |result| match result {
            Ok(data) if !data.is_empty() => {
                println!("  [server] echoing {} bytes", data.len());
                let p2 = Arc::clone(&p);
                p.write(data, move |result| {
                    result.expect("server write failed");
                    echo_connection(p2);
                });
            }
            Ok(_) => println!("  [server] connection closed"),
            Err(e) => eprintln!("  [server] read error: {e}"),
        });
    }

    /// Accept one connection, start echoing it, then re-arm to accept the next.
    /// Accepted peers are parked in `peers` so they outlive their callbacks —
    /// `IoTaskRunner` only holds `Weak` references to watchers.
    fn accept_loop(server: Arc<TcpServerSocket>, peers: Arc<Mutex<Vec<Arc<TcpClientSocket>>>>) {
        let srv = Arc::clone(&server);
        let pe = Arc::clone(&peers);
        server.accept(move |result| match result {
            Ok(peer) => {
                println!("  [server] accepted a connection");
                let peer = Arc::new(peer);
                pe.lock().unwrap().push(Arc::clone(&peer));
                echo_connection(peer);
                accept_loop(srv, pe);
            }
            Err(e) => eprintln!("  [server] accept error: {e}"),
        });
    }

    // ── client ────────────────────────────────────────────────────────────────

    /// Send `msgs[idx]`, wait for the echo, then recurse for the next message.
    /// When every message has been echoed, release the barrier.
    fn send_round(client: Arc<TcpClientSocket>, msgs: Vec<String>, idx: usize, done: Arc<Barrier>) {
        if idx >= msgs.len() {
            done.wait();
            return;
        }
        let msg = msgs[idx].clone();
        println!("  [client] → {msg:?}");

        let c = Arc::clone(&client);
        client.write(msg.into_bytes(), move |result| {
            result.expect("client write failed");

            let c2 = Arc::clone(&c);
            c.read(1024, move |result| {
                let data = result.expect("client read failed");
                println!("  [client] ← {:?}", String::from_utf8_lossy(&data));
                send_round(c2, msgs, idx + 1, done);
            });
        });
    }

    // ── main ──────────────────────────────────────────────────────────────────

    pub fn run() {
        let io = IoTaskRunner::new();

        // Kept alive for the whole run — both sockets and the accepted peers.
        let server = Arc::new(TcpServerSocket::new());
        let peers: Arc<Mutex<Vec<Arc<TcpClientSocket>>>> = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(TcpClientSocket::new());

        // Bring up the server on the IO thread and report its address back.
        let (addr_tx, addr_rx) = mpsc::channel();
        let srv = Arc::clone(&server);
        let pe = Arc::clone(&peers);
        io.post_task(Box::new(move || {
            srv.listen("127.0.0.1:0".parse().unwrap(), 16).expect("listen failed");
            addr_tx.send(srv.local_addr().unwrap()).unwrap();
            accept_loop(srv, pe);
        }));

        let addr = addr_rx.recv().unwrap();
        println!("server listening on {addr}\n");

        // Connect the client and run the request/response rounds.
        let done = Arc::new(Barrier::new(2));
        let msgs = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];

        let c = Arc::clone(&client);
        let d = Arc::clone(&done);
        io.post_task(Box::new(move || {
            let c2 = Arc::clone(&c);
            c.connect(addr, move |result| {
                result.expect("connect failed");
                println!("  [client] connected\n");
                send_round(c2, msgs, 0, d);
            });
        }));

        done.wait();
        println!("\ndone");
        io.shutdown();

        // `server`, `peers`, and `client` stay alive until here.
        drop((server, peers, client));
    }
}
