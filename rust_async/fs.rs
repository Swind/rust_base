//! Async filesystem access, mirroring `async_std::fs`.
//!
//! This is the most direct stage of all: `rust_io::FileProxy` is *already* the
//! blocking-pool-offload model (it runs `pread`/`pwrite` on a thread pool and
//! delivers the result via callback). We only add the Future façade — post the
//! op onto the reactor's IO thread (where `FileProxy` requires to be called)
//! and turn its result callback into a `Waker` wake-up via a one-shot channel.

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

fn fs_pool() -> Arc<ThreadPool> {
    FS_POOL.get_or_init(|| ThreadPool::new(4)).clone()
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
