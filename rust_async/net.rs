//! Reactor-backed async networking: [`Async`] (TCP stream), [`TcpListener`],
//! and [`UdpSocket`].
//!
//! Non-blocking I/O syscalls are issued directly on the calling thread (any
//! thread may do that); only epoll *registration* is hopped onto the IO thread
//! by [`crate::reactor::Source`]. When a syscall returns `WouldBlock`, we await
//! readiness — and that await is where epoll → `Waker` happens.

use std::future::Future;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures_io::{AsyncRead, AsyncWrite};

use crate::reactor::Source;

// ── shared readiness futures ────────────────────────────────────────────────

struct Readable<'a> {
    source: &'a Arc<Source>,
}

impl Future for Readable<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.source.poll_readable(cx)
    }
}

struct Writable<'a> {
    source: &'a Arc<Source>,
}

impl Future for Writable<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.source.poll_writable(cx)
    }
}

// ── TCP stream ──────────────────────────────────────────────────────────────

struct Inner {
    io: TcpStream,
    source: Arc<Source>,
}

/// An async wrapper around a non-blocking [`TcpStream`], driven by the reactor.
///
/// `Async` is cheaply [`Clone`]: clones share the same fd and reactor
/// registration, so one clone can read while another writes concurrently (the
/// "split" use case). This mirrors `async-std`'s `TcpStream: Clone`.
#[derive(Clone)]
pub struct Async {
    inner: Arc<Inner>,
}

impl Async {
    /// Wrap an existing stream, setting it non-blocking and registering its fd
    /// with the reactor.
    pub fn new(io: TcpStream) -> io::Result<Self> {
        io.set_nonblocking(true)?;
        let source = Source::new(io.as_raw_fd());
        Ok(Self { inner: Arc::new(Inner { io, source }) })
    }

    /// Connect to `addr` (IPv4 or IPv6) without blocking the executor.
    pub async fn connect(addr: SocketAddr) -> io::Result<Async> {
        let stream = start_connect(addr)?;
        let conn = Async::new(stream)?;
        // The socket becomes writable once the connect completes (or fails).
        conn.writable().await;
        match take_socket_error(conn.io().as_raw_fd())? {
            Some(err) => Err(err),
            None => Ok(conn),
        }
    }

    /// The remote address of this connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io().peer_addr()
    }

    fn io(&self) -> &TcpStream {
        &self.inner.io
    }

    fn source(&self) -> &Arc<Source> {
        &self.inner.source
    }

    fn readable(&self) -> Readable<'_> {
        Readable { source: self.source() }
    }

    fn writable(&self) -> Writable<'_> {
        Writable { source: self.source() }
    }

    /// Read into `buf`, awaiting readability as needed. Returns 0 on EOF.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.io().read(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.readable().await,
                Err(e) => return Err(e),
            }
        }
    }

    /// Write `buf`, awaiting writability as needed.
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            match self.io().write(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.writable().await,
                Err(e) => return Err(e),
            }
        }
    }

    /// Write the whole buffer.
    pub async fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let n = self.write(buf).await?;
            if n == 0 {
                return Err(io::Error::new(ErrorKind::WriteZero, "write returned 0"));
            }
            buf = &buf[n..];
        }
        Ok(())
    }
}

// `futures_io::AsyncRead`/`AsyncWrite` are the de-facto common async I/O traits
// (NOT in std) — the same ones `async-std` re-exports. Implementing them lets
// `Async` plug into the `futures` ecosystem's combinators. We mirror
// async-std's pattern: the real impl is on `&Async`, and the owned `Async` impl
// forwards.

impl AsyncRead for &Async {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this: &Async = self.get_mut();
        loop {
            match this.io().read(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    ready!(this.source().poll_readable(cx));
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncWrite for &Async {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this: &Async = self.get_mut();
        loop {
            match this.io().write(buf) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    ready!(this.source().poll_writable(cx));
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this: &Async = self.get_mut();
        Poll::Ready(this.io().flush())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this: &Async = self.get_mut();
        Poll::Ready(this.io().shutdown(std::net::Shutdown::Write))
    }
}

impl AsyncRead for Async {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut r: &Async = self.get_mut();
        Pin::new(&mut r).poll_read(cx, buf)
    }
}

impl AsyncWrite for Async {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut r: &Async = self.get_mut();
        Pin::new(&mut r).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut r: &Async = self.get_mut();
        Pin::new(&mut r).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut r: &Async = self.get_mut();
        Pin::new(&mut r).poll_close(cx)
    }
}

// ── TCP listener ────────────────────────────────────────────────────────────

/// A reactor-backed async TCP listener.
///
/// `accept` becomes readable when a connection is pending; we drive it with the
/// same one-shot readiness mechanism as [`Async`].
pub struct TcpListener {
    io: std::net::TcpListener,
    source: Arc<Source>,
}

impl TcpListener {
    /// Bind a non-blocking listener and register it with the reactor.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let io = std::net::TcpListener::bind(addr)?;
        io.set_nonblocking(true)?;
        let source = Source::new(io.as_raw_fd());
        Ok(Self { io, source })
    }

    /// The local address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    /// Accept the next incoming connection, awaiting readiness as needed.
    pub async fn accept(&self) -> io::Result<(Async, SocketAddr)> {
        loop {
            match self.io.accept() {
                Ok((stream, addr)) => return Ok((Async::new(stream)?, addr)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    Readable { source: &self.source }.await
                }
                Err(e) => return Err(e),
            }
        }
    }
}

// ── UDP socket ──────────────────────────────────────────────────────────────

/// A reactor-backed async UDP socket.
pub struct UdpSocket {
    io: std::net::UdpSocket,
    source: Arc<Source>,
}

impl UdpSocket {
    /// Bind a non-blocking UDP socket and register it with the reactor.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let io = std::net::UdpSocket::bind(addr)?;
        io.set_nonblocking(true)?;
        let source = Source::new(io.as_raw_fd());
        Ok(Self { io, source })
    }

    /// The local address the socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    /// Set the default peer for [`send`](Self::send)/[`recv`](Self::recv).
    pub fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.io.connect(addr)
    }

    fn readable(&self) -> Readable<'_> {
        Readable { source: &self.source }
    }

    fn writable(&self) -> Writable<'_> {
        Writable { source: &self.source }
    }

    /// Send `buf` to `addr`, awaiting writability as needed.
    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        loop {
            match self.io.send_to(buf, addr) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.writable().await,
                Err(e) => return Err(e),
            }
        }
    }

    /// Receive a datagram into `buf`, returning the byte count and sender.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            match self.io.recv_from(buf) {
                Ok(pair) => return Ok(pair),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.readable().await,
                Err(e) => return Err(e),
            }
        }
    }

    /// Send `buf` to the connected peer.
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            match self.io.send(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.writable().await,
                Err(e) => return Err(e),
            }
        }
    }

    /// Receive a datagram from the connected peer into `buf`.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.io.recv(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.readable().await,
                Err(e) => return Err(e),
            }
        }
    }
}

// ── raw non-blocking connect helpers (no socket2 dependency) ────────────────

fn start_connect(addr: SocketAddr) -> io::Result<TcpStream> {
    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };

    let fd = unsafe {
        libc::socket(domain, libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK, 0)
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // Own the fd immediately so it's closed on any early return.
    let stream = unsafe { TcpStream::from_raw_fd(fd) };

    let rc = match addr {
        SocketAddr::V4(v4) => {
            let sa = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr { s_addr: u32::from_ne_bytes(v4.ip().octets()) },
                sin_zero: [0; 8],
            };
            unsafe {
                libc::connect(
                    fd,
                    &sa as *const libc::sockaddr_in as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            }
        }
        SocketAddr::V6(v6) => {
            let sa = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr { s6_addr: v6.ip().octets() },
                sin6_scope_id: v6.scope_id(),
            };
            unsafe {
                libc::connect(
                    fd,
                    &sa as *const libc::sockaddr_in6 as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            }
        }
    };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(err);
        }
    }
    Ok(stream)
}

fn take_socket_error(fd: RawFd) -> io::Result<Option<io::Error>> {
    let mut err: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &mut err as *mut libc::c_int as *mut libc::c_void,
            &mut len,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    if err != 0 { Ok(Some(io::Error::from_raw_os_error(err))) } else { Ok(None) }
}
