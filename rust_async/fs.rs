//! Async filesystem access, mirroring `async_std::fs`.
//!
//! This is the most direct stage of all: `rust_io::FileProxy` is *already* the
//! blocking-pool-offload model (it runs `pread`/`pwrite` on a thread pool and
//! delivers the result via callback). We only add the Future façade — post the
//! op onto the reactor's IO thread (where `FileProxy` requires to be called)
//! and turn its result callback into a `Waker` wake-up via a one-shot channel.
//!
//! ## Sizing the blocking pool
//!
//! Regular files have no epoll readiness signal, so file ops run as blocking
//! `pread`/`pwrite` calls on a dedicated thread pool. That pool's worker count
//! caps how many file operations run concurrently; it is resolved once, lazily,
//! on first use, in this order:
//!
//! 1. [`init_pool(n)`](init_pool) — call before the first `fs` use.
//! 2. the `RUST_ASYNC_FS_THREADS` environment variable.
//! 3. [`DEFAULT_FS_THREADS`] (4).
//!
//! ```no_run
//! // Run up to 16 file operations concurrently.
//! rust_async::fs::init_pool(16);
//! ```

use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use rust_io::FileProxy;
use rust_task::{TaskRunner, ThreadPool};

use crate::reactor::io_runner;

static FS_POOL: OnceLock<Arc<ThreadPool>> = OnceLock::new();

/// Default worker count for the blocking filesystem pool, used when neither
/// [`init_pool`] nor the `RUST_ASYNC_FS_THREADS` environment variable applies.
const DEFAULT_FS_THREADS: usize = 4;

/// Configure the number of worker threads backing the async filesystem API.
///
/// File operations are not epoll-driven (regular files have no readiness
/// signal); they run as blocking `pread`/`pwrite` calls on a dedicated thread
/// pool. This sets that pool's size, capping how many file operations run
/// concurrently.
///
/// Call this **before** the first use of any `fs` function ([`read`],
/// [`write`], [`File::open`], …). The pool is created lazily on first use, so
/// once it exists the size is fixed.
///
/// Returns `true` if the size took effect, or `false` if the pool was already
/// initialised (by an earlier `fs` call or a prior `init_pool`), in which case
/// the existing pool is left unchanged.
///
/// If this is never called, the size comes from the `RUST_ASYNC_FS_THREADS`
/// environment variable, falling back to [`DEFAULT_FS_THREADS`] (4).
pub fn init_pool(num_threads: usize) -> bool {
    let num_threads = num_threads.max(1);
    let mut created = false;
    FS_POOL.get_or_init(|| {
        created = true;
        ThreadPool::new(num_threads)
    });
    created
}

/// Worker count from `RUST_ASYNC_FS_THREADS`, or [`DEFAULT_FS_THREADS`] when
/// the variable is unset, empty, or not a positive integer.
fn configured_fs_threads() -> usize {
    parse_fs_threads(std::env::var("RUST_ASYNC_FS_THREADS").ok())
}

/// Pure parsing of the thread-count override; falls back to
/// [`DEFAULT_FS_THREADS`] for `None`, empty, non-numeric, or zero input.
fn parse_fs_threads(raw: Option<String>) -> usize {
    raw.and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_FS_THREADS)
}

fn fs_pool() -> Arc<ThreadPool> {
    FS_POOL.get_or_init(|| ThreadPool::new(configured_fs_threads())).clone()
}

// ── one-shot result channel (callback → Future) ─────────────────────────────

struct OneshotState<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

struct Setter<T> {
    state: Arc<Mutex<OneshotState<T>>>,
}

impl<T> Setter<T> {
    fn set(self, value: T) {
        let waker = {
            let mut s = self.state.lock().unwrap();
            s.value = Some(value);
            s.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

struct Oneshot<T> {
    state: Arc<Mutex<OneshotState<T>>>,
}

impl<T> Future for Oneshot<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.state.lock().unwrap();
        if let Some(v) = s.value.take() {
            return Poll::Ready(v);
        }
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

fn oneshot<T>() -> (Setter<T>, Oneshot<T>) {
    let state = Arc::new(Mutex::new(OneshotState { value: None, waker: None }));
    (Setter { state: state.clone() }, Oneshot { state })
}

// ── File ────────────────────────────────────────────────────────────────────

/// An async handle to a file path, backed by [`rust_io::FileProxy`].
///
/// Unlike `std`/`async-std`'s `File`, this does not hold an open descriptor or
/// a cursor; each method performs a self-contained positional or whole-file
/// operation (matching `FileProxy`). A cursor-based `AsyncRead`/`AsyncWrite`
/// `File` is left as future surface area.
#[derive(Clone)]
pub struct File {
    proxy: Arc<FileProxy>,
}

impl File {
    /// Create a handle for `path`. Cheap; no I/O happens until a method is
    /// awaited.
    pub fn open(path: impl Into<PathBuf>) -> File {
        File { proxy: Arc::new(FileProxy::new(path.into(), fs_pool())) }
    }

    /// Run a `FileProxy` op on the IO thread, awaiting its callback result.
    async fn run<T, F>(&self, op: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&FileProxy, Box<dyn FnOnce(io::Result<T>) + Send + 'static>) + Send + 'static,
    {
        let proxy = self.proxy.clone();
        let (setter, fut) = oneshot::<io::Result<T>>();
        io_runner().post_task(Box::new(move || {
            op(&proxy, Box::new(move |res| setter.set(res)));
        }));
        fut.await
    }

    /// Read the entire file.
    pub async fn read_all(&self) -> io::Result<Vec<u8>> {
        self.run(|p, cb| p.read_all(cb)).await
    }

    /// Read `len` bytes starting at `offset`.
    pub async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.run(move |p, cb| p.read(offset, len, cb)).await
    }

    /// Create or truncate the file and write `data` from byte 0.
    pub async fn write_all(&self, data: Vec<u8>) -> io::Result<()> {
        self.run(move |p, cb| p.write_all(data, cb)).await
    }

    /// Write `data` at `offset` without truncating the rest of the file.
    pub async fn write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<usize> {
        self.run(move |p, cb| p.write(offset, data, cb)).await
    }

    /// Append `data` to the end of the file.
    pub async fn append(&self, data: Vec<u8>) -> io::Result<usize> {
        self.run(move |p, cb| p.append(data, cb)).await
    }
}

// ── module-level convenience (à la async_std::fs::{read, write}) ────────────

/// Read the whole contents of a file.
pub async fn read(path: impl Into<PathBuf>) -> io::Result<Vec<u8>> {
    File::open(path).read_all().await
}

/// Write a slice as the entire contents of a file, creating/truncating it.
pub async fn write(path: impl Into<PathBuf>, data: Vec<u8>) -> io::Result<()> {
    File::open(path).write_all(data).await
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_FS_THREADS, parse_fs_threads};

    #[test]
    fn parse_fs_threads_overrides() {
        assert_eq!(parse_fs_threads(Some("8".into())), 8);
        assert_eq!(parse_fs_threads(Some("  16 ".into())), 16);
    }

    #[test]
    fn parse_fs_threads_falls_back() {
        assert_eq!(parse_fs_threads(None), DEFAULT_FS_THREADS);
        assert_eq!(parse_fs_threads(Some("".into())), DEFAULT_FS_THREADS);
        assert_eq!(parse_fs_threads(Some("abc".into())), DEFAULT_FS_THREADS);
        assert_eq!(parse_fs_threads(Some("0".into())), DEFAULT_FS_THREADS);
    }
}
