use std::io;
use std::net::SocketAddr;

use crate::tcp_client_socket::TcpClientSocket;
use crate::tcp_socket::TcpSocket;

/// A listening TCP socket — the server-facing handle.
///
/// Mirrors Chromium's `net::TCPServerSocket`: owns a [`TcpSocket`], and
/// `listen` opens the fd, applies the server default options (`SO_REUSEADDR`),
/// binds, and starts listening.  `accept` hands back each connection as a
/// connected [`TcpClientSocket`].
///
/// Must be driven from the IO thread, and kept alive until its callbacks fire.
pub struct TcpServerSocket {
    socket: TcpSocket,
}

impl TcpServerSocket {
    /// Create a new, unbound server socket.
    pub fn new() -> Self {
        Self { socket: TcpSocket::new() }
    }

    /// Open the fd, enable `SO_REUSEADDR`, bind to `addr`, and listen.
    ///
    /// Bind to `addr:0` to let the kernel pick a port, then call
    /// [`TcpServerSocket::local_addr`] to discover it.  Must be called from the
    /// IO thread.
    pub fn listen(&self, addr: SocketAddr, backlog: i32) -> io::Result<()> {
        self.socket.open(&addr)?;
        self.socket.set_default_options_for_server()?;
        self.socket.bind(addr)?;
        self.socket.listen(backlog)
    }

    /// The address the server is listening on (`getsockname(2)`).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Accept one incoming connection, delivering it as a connected
    /// [`TcpClientSocket`] with client default options applied.
    ///
    /// One-shot — call `accept()` again inside the callback to keep accepting.
    /// Must be called from the IO thread.
    pub fn accept(&self, cb: impl FnOnce(io::Result<TcpClientSocket>) + Send + 'static) {
        self.socket.accept(move |result| {
            cb(result.map(|sock| {
                // Accepted connections are clients: prefer prompt small writes.
                let _ = sock.set_default_options_for_client();
                TcpClientSocket::from_connected(sock)
            }))
        });
    }
}

impl Default for TcpServerSocket {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TcpClientSocket;
    use rust_io::IoTaskRunner;
    use rust_task::TaskRunner;
    use std::sync::{Arc, Barrier, Mutex};

    /// Full loopback round trip: a server and a client on the same IO thread.
    /// The client connects, the server accepts and writes, the client reads.
    #[test]
    fn client_server_round_trip() {
        let io = IoTaskRunner::new();

        // Keep every socket alive past the closures — IoTaskRunner holds only
        // Weak refs to watchers.
        let server = Arc::new(TcpServerSocket::new());
        let client = Arc::new(TcpClientSocket::new());
        let accepted: Arc<Mutex<Option<TcpClientSocket>>> = Arc::new(Mutex::new(None));

        let received = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));

        let srv = Arc::clone(&server);
        let cli = Arc::clone(&client);
        let acc = Arc::clone(&accepted);
        let recv = Arc::clone(&received);
        let b = Arc::clone(&barrier);

        io.post_task(Box::new(move || {
            srv.listen("127.0.0.1:0".parse().unwrap(), 1).unwrap();
            let addr = srv.local_addr().unwrap();

            // Server: accept, then write a greeting to the peer.
            let acc2 = Arc::clone(&acc);
            srv.accept(move |result| {
                let peer = result.expect("accept failed");
                peer.write(b"ping".to_vec(), |r| {
                    r.expect("server write failed");
                });
                *acc2.lock().unwrap() = Some(peer); // keep the peer alive
            });

            // Client: connect, then read the greeting.
            let cli2 = Arc::clone(&cli);
            let recv2 = Arc::clone(&recv);
            let b2 = Arc::clone(&b);
            cli.connect(addr, move |result| {
                result.expect("connect failed");
                cli2.read(64, move |result| {
                    *recv2.lock().unwrap() = result.expect("client read failed");
                    b2.wait();
                });
            });
        }));

        barrier.wait();
        io.shutdown();
        assert_eq!(*received.lock().unwrap(), b"ping");
    }
}
