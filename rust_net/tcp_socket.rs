use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use crate::socket_posix::SocketPosix;

/// A TCP socket: a thin layer over [`SocketPosix`] that owns the TCP-level
/// socket options.
///
/// Mirrors Chromium's `net::TCPSocket` (`TCPSocketPosix`).  The raw fd
/// lifecycle and the epoll-driven `connect`/`read`/`write`/`accept` primitives
/// live in `SocketPosix`; the TCP-specific configuration — `SO_REUSEADDR`,
/// `TCP_NODELAY` — lives here.  Higher layers ([`crate::TcpClientSocket`],
/// [`crate::TcpServerSocket`]) build on this.
///
/// Like the layers it wraps, every method that touches epoll **must be called
/// from the IO thread**, and the socket must be kept alive until its callbacks
/// fire (the `IoTaskRunner` holds only a `Weak` reference to the watcher).
pub struct TcpSocket {
    socket: Arc<SocketPosix>,
}

impl TcpSocket {
    /// Create a new, unopened TCP socket.  Call `open()` before using it.
    pub fn new() -> Self {
        Self { socket: SocketPosix::new() }
    }

    /// Adopt an already-connected fd (e.g. the result of `accept`).
    pub fn from_connected_fd(fd: RawFd) -> Self {
        Self { socket: SocketPosix::from_fd(fd) }
    }

    /// Wrap an existing `SocketPosix` (used internally by `accept`).
    pub(crate) fn from_socket(socket: Arc<SocketPosix>) -> Self {
        Self { socket }
    }

    /// Open the underlying fd; the address family is inferred from `addr`.
    pub fn open(&self, addr: &SocketAddr) -> io::Result<()> {
        self.socket.open(addr)
    }

    // ── TCP options ─────────────────────────────────────────────────────────

    /// Server-side defaults: enable `SO_REUSEADDR` so a listener can rebind
    /// immediately after a restart instead of waiting out `TIME_WAIT`.
    ///
    /// Call after `open()` and before `bind()`.
    pub fn set_default_options_for_server(&self) -> io::Result<()> {
        self.set_reuse_addr(true)
    }

    /// Client-side defaults: disable Nagle's algorithm (`TCP_NODELAY`) so small
    /// writes are sent promptly rather than coalesced.
    ///
    /// Call after `open()`.
    pub fn set_default_options_for_client(&self) -> io::Result<()> {
        self.set_no_delay(true)
    }

    /// Toggle `SO_REUSEADDR`.
    pub fn set_reuse_addr(&self, on: bool) -> io::Result<()> {
        setsockopt_bool(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_REUSEADDR, on)
    }

    /// Toggle `TCP_NODELAY`.
    pub fn set_no_delay(&self, on: bool) -> io::Result<()> {
        setsockopt_bool(self.as_raw_fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY, on)
    }

    /// The local address the socket is bound to (`getsockname(2)`).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    // ── Delegated operations ────────────────────────────────────────────────

    pub fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        self.socket.bind(addr)
    }

    pub fn listen(&self, backlog: i32) -> io::Result<()> {
        self.socket.listen(backlog)
    }

    pub fn connect(&self, addr: SocketAddr, cb: impl FnOnce(io::Result<()>) + Send + 'static) {
        self.socket.connect(addr, cb);
    }

    pub fn connect_with_timeout(
        &self,
        addr: SocketAddr,
        timeout: Duration,
        cb: impl FnOnce(io::Result<()>) + Send + 'static,
    ) {
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

    /// Accept one incoming connection, delivering a new `TcpSocket` for the
    /// peer.  One-shot — call again inside the callback to keep accepting.
    pub fn accept(&self, cb: impl FnOnce(io::Result<TcpSocket>) + Send + 'static) {
        self.socket.accept(move |result| cb(result.map(TcpSocket::from_socket)));
    }

    /// Close the socket and cancel all pending operations.
    pub fn close(&self) {
        self.socket.close();
    }
}

impl AsRawFd for TcpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

impl Default for TcpSocket {
    fn default() -> Self {
        Self::new()
    }
}

fn setsockopt_bool(fd: RawFd, level: libc::c_int, name: libc::c_int, on: bool) -> io::Result<()> {
    let val: libc::c_int = on as libc::c_int;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_apply_after_open() {
        let sock = TcpSocket::new();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        sock.open(&addr).unwrap();

        sock.set_default_options_for_server().unwrap();
        sock.set_default_options_for_client().unwrap();

        // Verify TCP_NODELAY actually took effect.
        let mut val: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &mut val as *mut _ as *mut libc::c_void,
                &mut len,
            )
        };
        assert_eq!(rc, 0);
        assert_ne!(val, 0, "TCP_NODELAY should be enabled");
    }

    #[test]
    fn local_addr_reports_bound_port() {
        let sock = TcpSocket::new();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        sock.open(&addr).unwrap();
        sock.set_default_options_for_server().unwrap();
        sock.bind(addr).unwrap();

        let bound = sock.local_addr().unwrap();
        assert!(bound.ip().is_loopback());
        assert_ne!(bound.port(), 0, "kernel should have assigned a port");
    }
}
