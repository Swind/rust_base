use std::io;
use std::net::SocketAddr;

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

    pub fn read(&self, len: usize, cb: impl FnOnce(io::Result<Vec<u8>>) + Send + 'static) {
        self.socket.read(len, cb);
    }

    pub fn read_if_ready(&self, cb: impl FnOnce(io::Result<()>) + Send + 'static) {
        self.socket.read_if_ready(cb);
    }

    pub fn write(&self, buf: Vec<u8>, cb: impl FnOnce(io::Result<usize>) + Send + 'static) {
        self.socket.write(buf, cb);
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
