//! Async filesystem access, mirroring `async_std::fs`.
//!
//! Regular files have no epoll readiness signal, so every operation is a
//! blocking syscall offloaded to a thread pool, its result delivered back
//! through a one-shot channel that wakes the awaiting task. [`File`] holds an
//! open descriptor and a cursor and implements the `futures_io`
//! `AsyncRead`/`AsyncWrite`/`AsyncSeek` traits (so it composes with
//! [`io::BufReader`](crate::io::BufReader), [`io::copy`](crate::io::copy), …),
//! alongside self-contained positional helpers.
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

use std::cmp::min;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use futures_io::{AsyncRead, AsyncSeek, AsyncWrite};
use rust_task::{TaskRunner, TaskTraits, ThreadPool};

use crate::stream::{self, Stream};

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

/// A parallel `TaskRunner` over [`fs_pool`] for standalone blocking filesystem
/// calls (directory/metadata ops) that have no per-path ordering requirement.
fn fs_runner() -> Arc<dyn TaskRunner> {
    static FS_RUNNER: OnceLock<Arc<dyn TaskRunner>> = OnceLock::new();
    FS_RUNNER
        .get_or_init(|| {
            fs_pool().create_task_runner(TaskTraits { may_block: true, ..TaskTraits::default() })
        })
        .clone()
}

/// Run a blocking `std::fs`/`pread`/`pwrite` call on the filesystem pool and
/// await its result via a one-shot channel. The shared primitive behind every
/// operation in this module ([`File`]'s methods, the directory/metadata
/// helpers, and the module-level functions).
fn fs_blocking<T, F>(f: F) -> Oneshot<io::Result<T>>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let (setter, fut) = oneshot::<io::Result<T>>();
    fs_runner().post_task(Box::new(move || setter.set(f())));
    fut
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

// ── OpenOptions ──────────────────────────────────────────────────────────────

/// Builder for opening a [`File`] with specific flags, mirroring
/// `async_std::fs::OpenOptions` (a thin async wrapper over
/// [`std::fs::OpenOptions`]).
#[derive(Clone, Debug)]
pub struct OpenOptions(std::fs::OpenOptions);

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions::new()
    }
}

impl OpenOptions {
    /// A new set of options with every flag off.
    pub fn new() -> OpenOptions {
        OpenOptions(std::fs::OpenOptions::new())
    }

    /// Allow reading.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.0.read(read);
        self
    }

    /// Allow writing.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.0.write(write);
        self
    }

    /// Open in append mode (writes go to the end of the file).
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.0.append(append);
        self
    }

    /// Truncate the file to length 0 on open.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.0.truncate(truncate);
        self
    }

    /// Create the file if it does not exist.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.0.create(create);
        self
    }

    /// Create the file, failing if it already exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.0.create_new(create_new);
        self
    }

    /// Open `path` with these options, off the executor.
    pub async fn open(&self, path: impl Into<PathBuf>) -> io::Result<File> {
        let opts = self.0.clone();
        let path = path.into();
        let file = fs_blocking(move || opts.open(&path)).await?;
        Ok(File::from_std(file))
    }
}

// ── File ────────────────────────────────────────────────────────────────────

/// An async handle to an open file with a read/write cursor.
///
/// Implements the `futures_io` `AsyncRead`/`AsyncWrite`/`AsyncSeek` traits (so
/// it plugs into [`io::BufReader`](crate::io::BufReader),
/// [`io::copy`](crate::io::copy), and the rest of the ecosystem), and also
/// offers self-contained positional helpers ([`read_at`](File::read_at),
/// [`write_at`](File::write_at), [`append`](File::append)) that ignore the
/// cursor. Each operation is a blocking `pread`/`pwrite` offloaded to the
/// configurable filesystem pool.
///
/// Not `Clone`: a `File` owns its cursor (like [`std::fs::File`]).
pub struct File {
    file: Arc<std::fs::File>,
    pos: u64,
    read_busy: Option<Oneshot<io::Result<Vec<u8>>>>,
    read_leftover: Vec<u8>,
    write_busy: Option<Oneshot<io::Result<usize>>>,
    seek_busy: Option<Oneshot<io::Result<u64>>>,
}

impl File {
    fn from_std(file: std::fs::File) -> File {
        File {
            file: Arc::new(file),
            pos: 0,
            read_busy: None,
            read_leftover: Vec::new(),
            write_busy: None,
            seek_busy: None,
        }
    }

    /// Open `path` read-only.
    pub async fn open(path: impl Into<PathBuf>) -> io::Result<File> {
        let path = path.into();
        let file = fs_blocking(move || std::fs::File::open(&path)).await?;
        Ok(File::from_std(file))
    }

    /// Create (or truncate) `path` for writing.
    pub async fn create(path: impl Into<PathBuf>) -> io::Result<File> {
        let path = path.into();
        let file = fs_blocking(move || std::fs::File::create(&path)).await?;
        Ok(File::from_std(file))
    }

    /// Metadata for the open file.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let file = self.file.clone();
        fs_blocking(move || file.metadata().map(Metadata)).await
    }

    /// Truncate or extend the file to `size` bytes.
    pub async fn set_len(&self, size: u64) -> io::Result<()> {
        let file = self.file.clone();
        fs_blocking(move || file.set_len(size)).await
    }

    /// Flush OS buffers for this file to disk.
    pub async fn sync_all(&self) -> io::Result<()> {
        let file = self.file.clone();
        fs_blocking(move || file.sync_all()).await
    }

    /// Read the entire file (from byte 0; does not move the cursor).
    pub async fn read_all(&self) -> io::Result<Vec<u8>> {
        let file = self.file.clone();
        fs_blocking(move || {
            let len = file.metadata()?.len() as usize;
            let mut buf = vec![0u8; len];
            let n = file.read_at(&mut buf, 0)?;
            buf.truncate(n);
            Ok(buf)
        })
        .await
    }

    /// Read up to `len` bytes starting at `offset` (does not move the cursor).
    pub async fn read_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let file = self.file.clone();
        fs_blocking(move || {
            let mut buf = vec![0u8; len];
            let n = file.read_at(&mut buf, offset)?;
            buf.truncate(n);
            Ok(buf)
        })
        .await
    }

    /// Write `data` at `offset` without truncating the rest of the file
    /// (does not move the cursor).
    pub async fn write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<usize> {
        let file = self.file.clone();
        let n = data.len();
        fs_blocking(move || file.write_all_at(&data, offset).map(|()| n)).await
    }

    /// Append `data` to the end of the file (does not move the cursor).
    pub async fn append(&self, data: Vec<u8>) -> io::Result<usize> {
        let file = self.file.clone();
        let n = data.len();
        fs_blocking(move || {
            let end = file.metadata()?.len();
            file.write_all_at(&data, end).map(|()| n)
        })
        .await
    }
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            if !this.read_leftover.is_empty() {
                let n = min(buf.len(), this.read_leftover.len());
                buf[..n].copy_from_slice(&this.read_leftover[..n]);
                this.read_leftover.drain(..n);
                return Poll::Ready(Ok(n));
            }
            match this.read_busy.take() {
                Some(mut job) => match Pin::new(&mut job).poll(cx) {
                    Poll::Pending => {
                        this.read_busy = Some(job);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        let bytes = res?;
                        if bytes.is_empty() {
                            return Poll::Ready(Ok(0)); // EOF
                        }
                        this.pos += bytes.len() as u64;
                        this.read_leftover = bytes;
                    }
                },
                None => {
                    let file = this.file.clone();
                    let pos = this.pos;
                    let len = buf.len().max(1);
                    this.read_busy = Some(fs_blocking(move || {
                        let mut v = vec![0u8; len];
                        let n = file.read_at(&mut v, pos)?;
                        v.truncate(n);
                        Ok(v)
                    }));
                }
            }
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match this.write_busy.take() {
                Some(mut job) => match Pin::new(&mut job).poll(cx) {
                    Poll::Pending => {
                        this.write_busy = Some(job);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        let n = res?;
                        this.pos += n as u64;
                        return Poll::Ready(Ok(n));
                    }
                },
                None => {
                    let file = this.file.clone();
                    let pos = this.pos;
                    let data = buf.to_vec();
                    let n = data.len();
                    this.write_busy =
                        Some(fs_blocking(move || file.write_all_at(&data, pos).map(|()| n)));
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Writes go straight to the fd via pwrite; nothing is buffered here.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The fd is closed when the last Arc to it is dropped.
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        loop {
            if let Some(mut job) = this.seek_busy.take() {
                match Pin::new(&mut job).poll(cx) {
                    Poll::Pending => {
                        this.seek_busy = Some(job);
                        return Poll::Pending;
                    }
                    Poll::Ready(res) => {
                        let new_pos = res?;
                        this.pos = new_pos;
                        this.read_leftover.clear();
                        return Poll::Ready(Ok(new_pos));
                    }
                }
            }
            match pos {
                SeekFrom::Start(n) => {
                    this.pos = n;
                    this.read_leftover.clear();
                    return Poll::Ready(Ok(n));
                }
                SeekFrom::Current(delta) => {
                    let new_pos = this.pos.checked_add_signed(delta).ok_or_else(invalid_seek)?;
                    this.pos = new_pos;
                    this.read_leftover.clear();
                    return Poll::Ready(Ok(new_pos));
                }
                SeekFrom::End(delta) => {
                    let file = this.file.clone();
                    this.seek_busy = Some(fs_blocking(move || {
                        let len = file.metadata()?.len();
                        len.checked_add_signed(delta).ok_or_else(invalid_seek)
                    }));
                    // Loop to poll the freshly-armed job and register the
                    // waker.
                }
            }
        }
    }
}

fn invalid_seek() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid seek to a negative or overflowing position",
    )
}

// ── module-level convenience (à la async_std::fs::{read, write}) ────────────

/// Read the whole contents of a file.
pub async fn read(path: impl Into<PathBuf>) -> io::Result<Vec<u8>> {
    File::open(path).await?.read_all().await
}

/// Write a slice as the entire contents of a file, creating/truncating it.
pub async fn write(path: impl Into<PathBuf>, data: Vec<u8>) -> io::Result<()> {
    use futures_util::io::AsyncWriteExt;
    let mut file = File::create(path).await?;
    file.write_all(&data).await?;
    file.flush().await
}

// ── directory operations (à la async_std::fs) ───────────────────────────────

/// Create a directory.
pub async fn create_dir(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    fs_blocking(move || std::fs::create_dir(&path)).await
}

/// Recursively create a directory and all of its missing parents.
pub async fn create_dir_all(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    fs_blocking(move || std::fs::create_dir_all(&path)).await
}

/// Remove an empty directory.
pub async fn remove_dir(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    fs_blocking(move || std::fs::remove_dir(&path)).await
}

/// Remove a directory and all of its contents.
pub async fn remove_dir_all(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    fs_blocking(move || std::fs::remove_dir_all(&path)).await
}

/// Remove a file.
pub async fn remove_file(path: impl Into<PathBuf>) -> io::Result<()> {
    let path = path.into();
    fs_blocking(move || std::fs::remove_file(&path)).await
}

/// Rename (move) `from` to `to`, replacing `to` if it exists.
pub async fn rename(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> io::Result<()> {
    let (from, to) = (from.into(), to.into());
    fs_blocking(move || std::fs::rename(&from, &to)).await
}

/// Copy the contents of `from` to `to`, returning the number of bytes copied.
pub async fn copy(from: impl Into<PathBuf>, to: impl Into<PathBuf>) -> io::Result<u64> {
    let (from, to) = (from.into(), to.into());
    fs_blocking(move || std::fs::copy(&from, &to)).await
}

/// Create a hard link `link` pointing at `original`.
pub async fn hard_link(original: impl Into<PathBuf>, link: impl Into<PathBuf>) -> io::Result<()> {
    let (original, link) = (original.into(), link.into());
    fs_blocking(move || std::fs::hard_link(&original, &link)).await
}

/// Canonicalize a path, resolving symlinks and `.`/`..` components.
pub async fn canonicalize(path: impl Into<PathBuf>) -> io::Result<PathBuf> {
    let path = path.into();
    fs_blocking(move || std::fs::canonicalize(&path)).await
}

/// Read the target of a symbolic link.
pub async fn read_link(path: impl Into<PathBuf>) -> io::Result<PathBuf> {
    let path = path.into();
    fs_blocking(move || std::fs::read_link(&path)).await
}

// ── metadata (à la async_std::fs) ───────────────────────────────────────────

/// Re-exported from `std::fs`; describes a file's type (file/dir/symlink).
pub use std::fs::FileType;
/// Re-exported from `std::fs`; a file's read-only/permission bits.
pub use std::fs::Permissions;

/// Metadata for a filesystem entry, wrapping [`std::fs::Metadata`].
///
/// All accessors are synchronous (the underlying `stat` already happened when
/// the `Metadata` was fetched); only fetching it is async.
#[derive(Clone)]
pub struct Metadata(std::fs::Metadata);

impl Metadata {
    /// The file type (file, directory, or symlink).
    pub fn file_type(&self) -> FileType {
        self.0.file_type()
    }

    /// `true` if this is a directory.
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    /// `true` if this is a regular file.
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// `true` if this is a symbolic link (only meaningful via
    /// [`symlink_metadata`]).
    pub fn is_symlink(&self) -> bool {
        self.0.is_symlink()
    }

    /// File size in bytes.
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    /// `true` if the file has zero length.
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    /// The permission bits.
    pub fn permissions(&self) -> Permissions {
        self.0.permissions()
    }

    /// Last modification time, if the platform supports it.
    pub fn modified(&self) -> io::Result<std::time::SystemTime> {
        self.0.modified()
    }

    /// Last access time, if the platform supports it.
    pub fn accessed(&self) -> io::Result<std::time::SystemTime> {
        self.0.accessed()
    }

    /// Creation time, if the platform supports it.
    pub fn created(&self) -> io::Result<std::time::SystemTime> {
        self.0.created()
    }
}

/// Metadata for `path`, following symlinks.
pub async fn metadata(path: impl Into<PathBuf>) -> io::Result<Metadata> {
    let path = path.into();
    fs_blocking(move || std::fs::metadata(&path).map(Metadata)).await
}

/// Metadata for `path` *without* following symlinks.
pub async fn symlink_metadata(path: impl Into<PathBuf>) -> io::Result<Metadata> {
    let path = path.into();
    fs_blocking(move || std::fs::symlink_metadata(&path).map(Metadata)).await
}

/// Change the permissions of `path`.
pub async fn set_permissions(path: impl Into<PathBuf>, perm: Permissions) -> io::Result<()> {
    let path = path.into();
    fs_blocking(move || std::fs::set_permissions(&path, perm.clone())).await
}

// ── directory listing (à la async_std::fs::read_dir) ─────────────────────────

/// A single entry yielded by [`read_dir`], wrapping [`std::fs::DirEntry`].
pub struct DirEntry(std::fs::DirEntry);

impl DirEntry {
    /// The full path to this entry.
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    /// The bare file name of this entry.
    pub fn file_name(&self) -> std::ffi::OsString {
        self.0.file_name()
    }

    /// Metadata for this entry (does not follow symlinks).
    pub async fn metadata(&self) -> io::Result<Metadata> {
        symlink_metadata(self.0.path()).await
    }

    /// The file type of this entry.
    pub async fn file_type(&self) -> io::Result<FileType> {
        Ok(self.metadata().await?.file_type())
    }
}

/// Return a stream over the entries of the directory at `path`.
///
/// Unlike `async_std`'s lazily-streamed `ReadDir`, this eagerly reads the whole
/// directory on the blocking pool, then streams the collected entries. For very
/// large directories that trades memory for simplicity.
pub async fn read_dir(
    path: impl Into<PathBuf>,
) -> io::Result<impl Stream<Item = io::Result<DirEntry>>> {
    let path = path.into();
    let entries = fs_blocking(move || {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&path)? {
            out.push(entry.map(DirEntry));
        }
        Ok(out)
    })
    .await?;
    Ok(stream::from_iter(entries))
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
