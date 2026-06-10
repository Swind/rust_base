//! Async I/O traits, buffering, and standard streams, mirroring
//! `async_std::io`.
//!
//! The traits are the de-facto-standard [`futures_io`] ones (the same ones
//! `async-std` re-exports), so types here interoperate with the wider `futures`
//! ecosystem. The buffering adapters and extension traits come straight from
//! [`futures_util`] — they are generic over any `AsyncRead`/`AsyncWrite`
//! (including our [`Async`](crate::net::Async) and [`File`](crate::fs::File))
//! and carry no runtime-specific behaviour, so we re-export rather than
//! re-implement them.
//!
//! Standard streams ([`stdin`], [`stdout`], [`stderr`]) are *not* epoll-driven:
//! the underlying fds may be files, pipes, or TTYs, so reads/writes are
//! offloaded to the blocking pool (the same approach `async-std` takes), each
//! `poll_*` driving an [`offload`](crate::offload) job to completion.

use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

pub use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
pub use futures_util::io::{
    AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, Cursor, copy,
};

use crate::offload;
use crate::task_impl::Offload;

// ── stdin ────────────────────────────────────────────────────────────────────

/// A handle to the standard input stream, readable via [`AsyncRead`].
///
/// Each read offloads a blocking `read` onto the blocking pool. Reads are *not*
/// coordinated across clones/handles; like `std`, concurrent readers see an
/// arbitrary interleaving.
pub struct Stdin {
    busy: Option<Offload<io::Result<Vec<u8>>>>,
    leftover: Vec<u8>,
}

/// Construct a handle to the standard input stream.
pub fn stdin() -> Stdin {
    Stdin { busy: None, leftover: Vec::new() }
}

impl AsyncRead for Stdin {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            if !this.leftover.is_empty() {
                let n = min(buf.len(), this.leftover.len());
                buf[..n].copy_from_slice(&this.leftover[..n]);
                this.leftover.drain(..n);
                return Poll::Ready(Ok(n));
            }
            match this.busy.take() {
                Some(mut job) => match Pin::new(&mut job).poll(cx) {
                    Poll::Pending => {
                        this.busy = Some(job);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        let bytes = res?;
                        if bytes.is_empty() {
                            return Poll::Ready(Ok(0)); // EOF
                        }
                        this.leftover = bytes;
                    }
                },
                None => {
                    let len = buf.len().max(1);
                    this.busy = Some(offload(move || {
                        use io::Read;
                        let mut v = vec![0u8; len];
                        let n = io::stdin().read(&mut v)?;
                        v.truncate(n);
                        Ok(v)
                    }));
                }
            }
        }
    }
}

// ── stdout / stderr ──────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Sink {
    Stdout,
    Stderr,
}

impl Sink {
    fn write_all(self, data: &[u8]) -> io::Result<()> {
        use io::Write;
        match self {
            Sink::Stdout => io::stdout().write_all(data),
            Sink::Stderr => io::stderr().write_all(data),
        }
    }

    fn flush(self) -> io::Result<()> {
        use io::Write;
        match self {
            Sink::Stdout => io::stdout().flush(),
            Sink::Stderr => io::stderr().flush(),
        }
    }
}

/// A handle to the standard output stream, writable via [`AsyncWrite`].
///
/// Each write offloads a blocking `write_all` onto the blocking pool, so a
/// `poll_write` accepting `buf` always reports the whole slice written once the
/// offloaded job completes.
pub struct Stdout(Writer);

/// A handle to the standard error stream, writable via [`AsyncWrite`].
pub struct Stderr(Writer);

/// Construct a handle to the standard output stream.
pub fn stdout() -> Stdout {
    Stdout(Writer::new(Sink::Stdout))
}

/// Construct a handle to the standard error stream.
pub fn stderr() -> Stderr {
    Stderr(Writer::new(Sink::Stderr))
}

struct Writer {
    sink: Sink,
    write_busy: Option<Offload<io::Result<usize>>>,
    flush_busy: Option<Offload<io::Result<()>>>,
}

impl Writer {
    fn new(sink: Sink) -> Self {
        Writer { sink, write_busy: None, flush_busy: None }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        loop {
            match self.write_busy.take() {
                Some(mut job) => match Pin::new(&mut job).poll(cx) {
                    Poll::Pending => {
                        self.write_busy = Some(job);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => return Poll::Ready(res),
                },
                None => {
                    let sink = self.sink;
                    let data = buf.to_vec();
                    let len = data.len();
                    self.write_busy = Some(offload(move || sink.write_all(&data).map(|()| len)));
                }
            }
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match self.flush_busy.take() {
                Some(mut job) => match Pin::new(&mut job).poll(cx) {
                    Poll::Pending => {
                        self.flush_busy = Some(job);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => return Poll::Ready(res),
                },
                None => {
                    let sink = self.sink;
                    self.flush_busy = Some(offload(move || sink.flush()));
                }
            }
        }
    }
}

macro_rules! impl_async_write {
    ($ty:ty) => {
        impl AsyncWrite for $ty {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.get_mut().0.poll_write(cx, buf)
            }

            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                self.get_mut().0.poll_flush(cx)
            }

            fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                // Closing a standard stream just flushes; the fd is not closed.
                self.get_mut().0.poll_flush(cx)
            }
        }
    };
}

impl_async_write!(Stdout);
impl_async_write!(Stderr);
