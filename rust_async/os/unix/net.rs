//! Reactor-backed async Unix-domain sockets, mirroring
//! `async_std::os::unix::net`: [`UnixStream`], [`UnixListener`], and
//! [`UnixDatagram`].
//!
//! These reuse the exact same machinery as the TCP/UDP types in
//! [`crate::net`] — `AF_UNIX` descriptors support epoll readiness just like
//! `AF_INET` ones, so the only difference is the address family. Non-blocking
//! syscalls run on the calling thread; only epoll registration is hopped onto
//! the IO thread by [`crate::reactor::Source`].
//!
//! The one exception is [`UnixStream::connect`], whose connect handshake we run
//! on the blocking pool (via [`offload`](crate::offload)) rather than
//! reimplementing the non-blocking `connect`/`SO_ERROR` dance for `AF_UNIX`.

use std::future::Future;
use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{
    SocketAddr, UnixDatagram as StdUnixDatagram, UnixListener as StdUnixListener,
    UnixStream as StdUnixStream,
};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures_core::Stream;
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

// ── UnixStream ───────────────────────────────────────────────────────────────

struct Inner {
    io: StdUnixStream,
    source: Arc<Source>,
}

/// An async wrapper around a non-blocking Unix-domain stream socket.
///
/// Like [`crate::net::Async`], it is cheaply [`Clone`] (clones share the fd and
/// reactor registration, so one can read while another writes), and
/// [`AsyncWrite::poll_close`] performs a write half-close rather than dropping
/// the fd.
#[derive(Clone)]
pub struct UnixStream {
    inner: Arc<Inner>,
}

impl UnixStream {
    /// Wrap an existing stream, setting it non-blocking and registering it.
    pub fn new(io: StdUnixStream) -> io::Result<Self> {
        io.set_nonblocking(true)?;
        let source = Source::new(io.as_raw_fd());
        Ok(Self { inner: Arc::new(Inner { io, source }) })
    }

    /// Connect to the socket at `path` (the connect runs on the blocking pool).
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<UnixStream> {
        let path = path.as_ref().to_path_buf();
        let stream = crate::offload(move || StdUnixStream::connect(&path)).await?;
        UnixStream::new(stream)
    }

    /// Create an unnamed, connected pair of streams.
    pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
        let (a, b) = StdUnixStream::pair()?;
        Ok((UnixStream::new(a)?, UnixStream::new(b)?))
    }

    /// This socket's address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io().local_addr()
    }

    /// The peer's address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io().peer_addr()
    }

    /// Shut down the read half, write half, or both.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.io().shutdown(how)
    }

    fn io(&self) -> &StdUnixStream {
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

impl AsyncRead for &UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this: &UnixStream = self.get_mut();
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

impl AsyncWrite for &UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this: &UnixStream = self.get_mut();
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
        Poll::Ready(self.get_mut().io().flush())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().io().shutdown(std::net::Shutdown::Write))
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut r: &UnixStream = self.get_mut();
        Pin::new(&mut r).poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut r: &UnixStream = self.get_mut();
        Pin::new(&mut r).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut r: &UnixStream = self.get_mut();
        Pin::new(&mut r).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut r: &UnixStream = self.get_mut();
        Pin::new(&mut r).poll_close(cx)
    }
}

// ── UnixListener ─────────────────────────────────────────────────────────────

/// A reactor-backed async Unix-domain listener.
pub struct UnixListener {
    io: StdUnixListener,
    source: Arc<Source>,
}

impl UnixListener {
    /// Bind a non-blocking listener to `path`.
    pub fn bind(path: impl Into<PathBuf>) -> io::Result<Self> {
        let io = StdUnixListener::bind(path.into())?;
        io.set_nonblocking(true)?;
        let source = Source::new(io.as_raw_fd());
        Ok(Self { io, source })
    }

    /// The address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    /// Accept the next incoming connection, awaiting readiness as needed.
    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        loop {
            match self.io.accept() {
                Ok((stream, addr)) => return Ok((UnixStream::new(stream)?, addr)),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    Readable { source: &self.source }.await
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// A stream that yields connections as they arrive.
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming { listener: self }
    }
}

/// Stream of incoming connections, returned by [`UnixListener::incoming`].
pub struct Incoming<'a> {
    listener: &'a UnixListener,
}

impl Stream for Incoming<'_> {
    type Item = io::Result<UnixStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let listener = self.listener;
        loop {
            match listener.io.accept() {
                Ok((stream, _addr)) => return Poll::Ready(Some(UnixStream::new(stream))),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    ready!(listener.source.poll_readable(cx));
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

// ── UnixDatagram ─────────────────────────────────────────────────────────────

/// A reactor-backed async Unix-domain datagram socket.
pub struct UnixDatagram {
    io: StdUnixDatagram,
    source: Arc<Source>,
}

impl UnixDatagram {
    fn from_std(io: StdUnixDatagram) -> io::Result<Self> {
        io.set_nonblocking(true)?;
        let source = Source::new(io.as_raw_fd());
        Ok(Self { io, source })
    }

    /// Bind a non-blocking datagram socket to `path`.
    pub fn bind(path: impl Into<PathBuf>) -> io::Result<Self> {
        Self::from_std(StdUnixDatagram::bind(path.into())?)
    }

    /// Create a datagram socket not bound to any address.
    pub fn unbound() -> io::Result<Self> {
        Self::from_std(StdUnixDatagram::unbound()?)
    }

    /// Create an unnamed, connected pair of datagram sockets.
    pub fn pair() -> io::Result<(UnixDatagram, UnixDatagram)> {
        let (a, b) = StdUnixDatagram::pair()?;
        Ok((Self::from_std(a)?, Self::from_std(b)?))
    }

    /// The address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    /// Set the default peer for [`send`](Self::send)/[`recv`](Self::recv).
    pub fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.io.connect(path)
    }

    fn readable(&self) -> Readable<'_> {
        Readable { source: &self.source }
    }

    fn writable(&self) -> Writable<'_> {
        Writable { source: &self.source }
    }

    /// Send `buf` to `path`, awaiting writability as needed.
    pub async fn send_to(&self, buf: &[u8], path: impl AsRef<Path>) -> io::Result<usize> {
        loop {
            match self.io.send_to(buf, path.as_ref()) {
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
