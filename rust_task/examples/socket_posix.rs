//! SocketPosix demo (Linux only).
//!
//! Three patterns:
//!
//!  1. **Connect + Write + Read** — basic echo round-trip: connect to a local
//!     echo server, write a message, read the echo back.
//!
//!  2. **ReadIfReady** — notification-only read: `read_if_ready` registers
//!     interest and fires a callback when the fd is readable; the caller then
//!     calls `read()` to actually fetch the data.  This mirrors Chromium's
//!     `SocketPosix::ReadIfReady` / `ReadCompleted` pattern.
//!
//!  3. **Streaming** — three consecutive write→read rounds on one connection,
//!     each initiated from the previous round's read callback, showing how
//!     chained async operations compose naturally.
//!
//! Run with:
//!   cargo run --example socket_posix

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
    #[cfg(not(target_os = "linux"))]
    eprintln!("This example requires Linux (epoll + eventfd).");
}

#[cfg(target_os = "linux")]
mod linux {
    use rust_task::{IoTaskRunner, SocketPosix, TaskRunner};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::{Arc, Barrier};
    use std::thread;

    // ── echo server ───────────────────────────────────────────────────────────

    /// Spin up a TCP echo server on an ephemeral port.
    /// Each accepted connection is handled in its own thread.
    fn start_echo_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                thread::spawn(move || {
                    let mut stream = stream.unwrap();
                    let mut buf = [0u8; 1024];
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => stream.write_all(&buf[..n]).unwrap(),
                        }
                    }
                });
            }
        });
        addr
    }

    // ── 1. Connect + Write + Read ─────────────────────────────────────────────

    pub fn demo_echo(runner: &Arc<IoTaskRunner>) {
        println!("=== 1. Connect + Write + Read ===");

        let addr = start_echo_server();
        let done = Arc::new(Barrier::new(2));
        let socket = SocketPosix::new();

        let s = Arc::clone(&socket);
        let d = Arc::clone(&done);
        runner.post_task(Box::new(move || {
            s.open(&addr).unwrap();

            let s2 = Arc::clone(&s);
            s.connect(addr, move |result| {
                result.expect("connect failed");
                println!("  connected");

                let s3 = Arc::clone(&s2);
                s2.write(b"hello, SocketPosix!".to_vec(), move |result| {
                    let n = result.expect("write failed");
                    println!("  wrote {n} bytes");

                    s3.read(64, move |result| {
                        let data = result.expect("read failed");
                        println!("  echo: {:?}\n", String::from_utf8_lossy(&data));
                        d.wait();
                    });
                });
            });
        }));

        done.wait();
        socket.close();
    }

    // ── 2. ReadIfReady ────────────────────────────────────────────────────────
    //
    // read_if_ready() registers a one-shot read watch.  When the fd becomes
    // readable, the callback fires with Ok(()) — no data is read yet.  The
    // caller then decides when and how to read, mirroring the Chromium pattern
    // where the upper layer owns its own read buffer.

    pub fn demo_read_if_ready(runner: &Arc<IoTaskRunner>) {
        println!("=== 2. ReadIfReady (notification → caller reads) ===");

        let addr = start_echo_server();
        let done = Arc::new(Barrier::new(2));
        let socket = SocketPosix::new();

        let s = Arc::clone(&socket);
        let d = Arc::clone(&done);
        runner.post_task(Box::new(move || {
            s.open(&addr).unwrap();

            let s2 = Arc::clone(&s);
            s.connect(addr, move |result| {
                result.expect("connect failed");

                // Send "ping" so the echo server has something to send back.
                let s3 = Arc::clone(&s2);
                s2.write(b"ping".to_vec(), move |result| {
                    result.expect("write failed");

                    // Register readiness interest.  The callback fires when
                    // the fd is readable; no data arrives yet.
                    let s4 = Arc::clone(&s3);
                    s3.read_if_ready(move |result| {
                        result.expect("read_if_ready failed");
                        println!("  fd is readable — now calling read()");

                        // Data is available; call read() to actually fetch it.
                        s4.read(64, move |result| {
                            let data = result.expect("read failed");
                            println!("  data: {:?}\n", String::from_utf8_lossy(&data));
                            d.wait();
                        });
                    });
                });
            });
        }));

        done.wait();
        socket.close();
    }

    // ── 3. Streaming ──────────────────────────────────────────────────────────
    //
    // Each round sends one message and waits for the echo before kicking off
    // the next round.  The next send is initiated from the read callback, so
    // the entire pipeline runs on the IO thread without any extra
    // synchronization between rounds.

    fn send_round(socket: Arc<SocketPosix>, msgs: Vec<String>, idx: usize, done: Arc<Barrier>) {
        if idx >= msgs.len() {
            done.wait();
            return;
        }
        let msg = msgs[idx].clone();
        println!("  [{idx}] → {msg:?}");

        let s = Arc::clone(&socket);
        socket.write(msg.into_bytes(), move |result| {
            result.expect("write failed");

            let s2 = Arc::clone(&s);
            s.read(64, move |result| {
                let data = result.expect("read failed");
                println!("  [{idx}] ← {:?}", String::from_utf8_lossy(&data));
                send_round(s2, msgs, idx + 1, done);
            });
        });
    }

    pub fn demo_streaming(runner: &Arc<IoTaskRunner>) {
        println!("=== 3. Streaming (3 request-response rounds) ===");

        let addr = start_echo_server();
        let done = Arc::new(Barrier::new(2));
        let socket = SocketPosix::new();
        let msgs = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];

        let s = Arc::clone(&socket);
        let d = Arc::clone(&done);
        runner.post_task(Box::new(move || {
            s.open(&addr).unwrap();

            let s2 = Arc::clone(&s);
            s.connect(addr, move |result| {
                result.expect("connect failed");
                send_round(s2, msgs, 0, d);
            });
        }));

        done.wait();
        socket.close();
        println!();
    }

    // ── main ─────────────────────────────────────────────────────────────────

    pub fn run() {
        let runner = IoTaskRunner::new();

        demo_echo(&runner);
        demo_read_if_ready(&runner);
        demo_streaming(&runner);

        runner.shutdown();
    }
}
