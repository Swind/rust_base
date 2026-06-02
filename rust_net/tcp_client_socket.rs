use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use crate::stream_socket::{ReadCallback, StreamSocket, WriteCallback};
use crate::tcp_socket::TcpSocket;

/// A connected TCP stream — the client-facing handle.
///
/// Mirrors Chromium's `net::TCPClientSocket`: owns a [`TcpSocket`], and on
/// `connect` it opens the fd, applies the client default options
/// (`TCP_NODELAY`), and starts the async connect.  A socket that is already
/// connected — e.g. one handed back by [`crate::TcpServerSocket::accept`] — is
/// adopted with [`TcpClientSocket::from_connected`].
///
/// Must be driven from the IO thread, and kept alive until its callbacks fire.
pub struct TcpClientSocket {
    socket: TcpSocket,
}

impl TcpClientSocket {
    /// Create an unconnected client socket.  Call `connect()` to use it.
    pub fn new() -> Self {
        Self { socket: TcpSocket::new() }
    }

    /// Adopt an already-connected `TcpSocket`.
    ///
    /// Used by [`crate::TcpServerSocket::accept`]; the accepted peer is already
    /// connected, so there is nothing to `connect`.
    pub fn from_connected(socket: TcpSocket) -> Self {
        Self { socket }
    }

    /// Open the fd, apply client defaults, then connect to `addr`.
    ///
    /// `TCP_NODELAY` is best-effort — a failure to set it does not abort the
    /// connection.  Must be called from the IO thread.
    pub fn connect(&self, addr: SocketAddr, cb: impl FnOnce(io::Result<()>) + Send + 'static) {
        if let Err(e) = self.socket.open(&addr) {
            cb(Err(e));
            return;
        }
        let _ = self.socket.set_default_options_for_client();
        self.socket.connect(addr, cb);
    }

    /// [`connect`](Self::connect) with a deadline.  If the handshake does not
    /// complete within `timeout`, the callback fires with
    /// [`io::ErrorKind::TimedOut`].  Must be called from the IO thread.
    pub fn connect_with_timeout(
        &self,
        addr: SocketAddr,
        timeout: Duration,
        cb: impl FnOnce(io::Result<()>) + Send + 'static,
    ) {
        if let Err(e) = self.socket.open(&addr) {
            cb(Err(e));
            return;
        }
        let _ = self.socket.set_default_options_for_client();
        self.socket.connect_with_timeout(addr, timeout, cb);
    }

    pub fn read(&self, len: usize, cb: impl FnOnce(io::Result<Vec<u8>>) + Send + 'static) {
        self.socket.read(len, cb);
    }

    pub fn read_with_timeout(
        &self,
        len: usize,
        timeout: Duration,
        cb: impl FnOnce(io::Result<Vec<u8>>) + Send + 'static,
    ) {
        self.socket.read_with_timeout(len, timeout, cb);
    }

    pub fn read_if_ready(&self, cb: impl FnOnce(io::Result<()>) + Send + 'static) {
        self.socket.read_if_ready(cb);
    }

    pub fn read_if_ready_with_timeout(
        &self,
        timeout: Duration,
        cb: impl FnOnce(io::Result<()>) + Send + 'static,
    ) {
        self.socket.read_if_ready_with_timeout(timeout, cb);
    }

    pub fn write(&self, buf: Vec<u8>, cb: impl FnOnce(io::Result<usize>) + Send + 'static) {
        self.socket.write(buf, cb);
    }

    pub fn write_with_timeout(
        &self,
        buf: Vec<u8>,
        timeout: Duration,
        cb: impl FnOnce(io::Result<usize>) + Send + 'static,
    ) {
        self.socket.write_with_timeout(buf, timeout, cb);
    }

    /// The local address of the connection (`getsockname(2)`).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Close the connection and cancel pending operations.
    pub fn disconnect(&self) {
        self.socket.close();
    }
}

impl Default for TcpClientSocket {
    fn default() -> Self {
        Self::new()
    }
}

/// A connected `TcpClientSocket` is a plaintext byte stream — the base case a
/// TLS layer will wrap. The inherent methods already match the trait shape, so
/// these just forward (a boxed `FnOnce` still satisfies the `impl FnOnce`
/// bound).
impl StreamSocket for TcpClientSocket {
    fn read(&self, len: usize, cb: ReadCallback) {
        TcpClientSocket::read(self, len, cb);
    }

    fn write(&self, buf: Vec<u8>, cb: WriteCallback) {
        TcpClientSocket::write(self, buf, cb);
    }

    fn disconnect(&self) {
        TcpClientSocket::disconnect(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_socket::StreamSocket;
    use rust_io::IoTaskRunner;
    use rust_task::TaskRunner;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::{Arc, Barrier, Mutex};

    /// Drive a real connection purely through `dyn StreamSocket`, proving the
    /// abstraction is usable without naming the concrete type. A std::net echo
    /// listener runs on a helper thread.
    #[test]
    fn usable_through_dyn_stream_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).unwrap();
            conn.write_all(&buf[..n]).unwrap();
        });

        let io = IoTaskRunner::new();
        let client = Arc::new(TcpClientSocket::new());
        let received = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));

        let c = Arc::clone(&client);
        let recv = Arc::clone(&received);
        let b = Arc::clone(&barrier);
        io.post_task(Box::new(move || {
            let c_inner = Arc::clone(&c);
            c.connect(addr, move |result| {
                result.expect("connect failed");
                // From here on, only the trait object is used.
                let stream: Arc<dyn StreamSocket> = c_inner;
                let s2 = Arc::clone(&stream);
                stream.write(
                    b"hello".to_vec(),
                    Box::new(move |w| {
                        w.expect("write failed");
                        s2.read(
                            64,
                            Box::new(move |r| {
                                *recv.lock().unwrap() = r.expect("read failed");
                                b.wait();
                            }),
                        );
                    }),
                );
            });
        }));

        barrier.wait();
        io.shutdown();
        assert_eq!(*received.lock().unwrap(), b"hello");
    }
}
