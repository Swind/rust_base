use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io_task_runner::IoTaskRunner;
use rust_task::task_runner::TaskRunner;
use rust_task::task_traits::TaskTraits;
use rust_task::thread_pool::thread_pool::ThreadPool;

/// Async file I/O backed by a blocking thread pool.
///
/// Mirrors Chromium's `base::FileProxy`: wraps a file path and a
/// `TaskRunner` that offloads blocking `pread`/`pwrite` calls so the
/// IO thread is never stalled.  Each result callback is posted back to
/// the `IoTaskRunner` that was current at call time, making it safe to
/// chain further file or socket operations from inside a callback.
///
/// All methods **must be called from the IO thread**.
///
/// # Usage
///
/// ```ignore
/// let pool = ThreadPool::new(4);
///
/// // From a task on the IO thread:
/// let file = FileProxy::new("/tmp/data.bin", Arc::clone(&pool));
///
/// file.read_all(move |result| {
///     println!("read {} bytes", result.unwrap().len());
///     // callback runs on the IO thread
/// });
/// ```
///
/// # Concurrency
///
/// `new()` creates a `SequencedTaskRunner` so that multiple operations
/// posted to the same `FileProxy` are serialised on the thread pool.
/// If you need a custom scheduling policy use [`FileProxy::with_runner`].
pub struct FileProxy {
    path: PathBuf,
    runner: Arc<dyn TaskRunner>,
}

impl FileProxy {
    /// Create a `FileProxy` backed by `pool`.
    ///
    /// A `SequencedTaskRunner` is created internally so concurrent calls on
    /// the same `FileProxy` are serialised.  Multiple `FileProxy` instances
    /// can share the same `pool`.
    pub fn new(path: impl Into<PathBuf>, pool: Arc<ThreadPool>) -> Self {
        let runner = pool.create_sequenced_task_runner(
            TaskTraits { may_block: true, ..TaskTraits::default() },
        );
        Self { path: path.into(), runner }
    }

    /// Create a `FileProxy` with a custom `TaskRunner`.
    ///
    /// Use this when you need a specific scheduling policy or want to share
    /// a runner across multiple `FileProxy` instances.
    pub fn with_runner(path: impl Into<PathBuf>, runner: Arc<dyn TaskRunner>) -> Self {
        Self { path: path.into(), runner }
    }

    /// Read `len` bytes starting at `offset`.  Callback runs on the IO thread.
    pub fn read(
        &self,
        offset: u64,
        len: usize,
        cb: impl FnOnce(io::Result<Vec<u8>>) + Send + 'static,
    ) {
        let path = self.path.clone();
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        self.runner.post_task(Box::new(move || {
            let result = blocking_pread(&path, offset, len);
            io.post_task(Box::new(move || cb(result)));
        }));
    }

    /// Read the entire file.  Callback runs on the IO thread.
    pub fn read_all(
        &self,
        cb: impl FnOnce(io::Result<Vec<u8>>) + Send + 'static,
    ) {
        let path = self.path.clone();
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        self.runner.post_task(Box::new(move || {
            let result = std::fs::read(&path);
            io.post_task(Box::new(move || cb(result)));
        }));
    }

    /// Write `data` at `offset` without truncating the rest of the file.
    /// Creates the file if it does not exist.
    /// Callback receives bytes written; runs on the IO thread.
    pub fn write(
        &self,
        offset: u64,
        data: Vec<u8>,
        cb: impl FnOnce(io::Result<usize>) + Send + 'static,
    ) {
        let path = self.path.clone();
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        self.runner.post_task(Box::new(move || {
            let result = blocking_pwrite(&path, offset, &data);
            io.post_task(Box::new(move || cb(result)));
        }));
    }

    /// Create or truncate the file and write `data` from byte 0.
    /// Callback runs on the IO thread.
    pub fn write_all(
        &self,
        data: Vec<u8>,
        cb: impl FnOnce(io::Result<()>) + Send + 'static,
    ) {
        let path = self.path.clone();
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        self.runner.post_task(Box::new(move || {
            let result = std::fs::write(&path, &data);
            io.post_task(Box::new(move || cb(result)));
        }));
    }

    /// Append `data` to the end of the file; creates it if it does not exist.
    /// Callback receives bytes written; runs on the IO thread.
    pub fn append(
        &self,
        data: Vec<u8>,
        cb: impl FnOnce(io::Result<usize>) + Send + 'static,
    ) {
        let path = self.path.clone();
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        self.runner.post_task(Box::new(move || {
            let result = blocking_append(&path, &data);
            io.post_task(Box::new(move || cb(result)));
        }));
    }
}

// ── Blocking helpers ──────────────────────────────────────────────────────────

fn to_cpath(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))
}

fn blocking_pread(path: &Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let cpath = to_cpath(path)?;
    let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buf = vec![0u8; len];
    let n = unsafe {
        libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, len, offset as libc::off_t)
    };
    unsafe { libc::close(fd) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    buf.truncate(n as usize);
    Ok(buf)
}

fn blocking_pwrite(path: &Path, offset: u64, data: &[u8]) -> io::Result<usize> {
    let cpath = to_cpath(path)?;
    let fd = unsafe {
        libc::open(
            cpath.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_CLOEXEC,
            0o666 as libc::mode_t,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let n = unsafe {
        libc::pwrite(fd, data.as_ptr() as *const libc::c_void, data.len(), offset as libc::off_t)
    };
    unsafe { libc::close(fd) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

fn blocking_append(path: &Path, data: &[u8]) -> io::Result<usize> {
    let cpath = to_cpath(path)?;
    let fd = unsafe {
        libc::open(
            cpath.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND | libc::O_CLOEXEC,
            0o666 as libc::mode_t,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let n = unsafe { libc::write(fd, data.as_ptr() as *const libc::c_void, data.len()) };
    unsafe { libc::close(fd) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(n as usize)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_task_runner::IoTaskRunner;
    use rust_task::thread_pool::thread_pool::ThreadPool;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rust_task_fp_{}_{}", std::process::id(), n))
    }

    fn make_pool() -> Arc<ThreadPool> {
        ThreadPool::new(2)
    }

    #[test]
    fn read_all_delivers_file_contents() {
        let path = temp_path();
        std::fs::write(&path, b"hello world").unwrap();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));

        let r = Arc::clone(&received);
        let b = Arc::clone(&barrier);
        let file = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file.read_all(move |result| {
                *r.lock().unwrap() = result.unwrap();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();
        std::fs::remove_file(&path).ok();

        assert_eq!(*received.lock().unwrap(), b"hello world");
    }

    #[test]
    fn read_at_offset_returns_partial_data() {
        let path = temp_path();
        std::fs::write(&path, b"abcdefghij").unwrap();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));

        let r = Arc::clone(&received);
        let b = Arc::clone(&barrier);
        let file = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file.read(3, 4, move |result| {
                *r.lock().unwrap() = result.unwrap();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();
        std::fs::remove_file(&path).ok();

        assert_eq!(*received.lock().unwrap(), b"defg");
    }

    #[test]
    fn write_at_offset_modifies_file() {
        let path = temp_path();
        std::fs::write(&path, b"0000000000").unwrap();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let barrier = Arc::new(Barrier::new(2));

        let b = Arc::clone(&barrier);
        let file = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file.write(3, b"XYZ".to_vec(), move |result| {
                result.unwrap();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();

        let contents = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(&contents[..3], b"000");
        assert_eq!(&contents[3..6], b"XYZ");
        assert_eq!(&contents[6..], b"0000");
    }

    #[test]
    fn write_all_creates_and_truncates_file() {
        let path = temp_path();
        std::fs::write(&path, b"old long content here").unwrap();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let barrier = Arc::new(Barrier::new(2));

        let b = Arc::clone(&barrier);
        let file = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file.write_all(b"new".to_vec(), move |result| {
                result.unwrap();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();

        let contents = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(contents, b"new");
    }

    #[test]
    fn append_grows_file_across_calls() {
        let path = temp_path();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let barrier = Arc::new(Barrier::new(2));

        let b = Arc::clone(&barrier);
        let file1 = FileProxy::new(&path, Arc::clone(&pool));
        let file2 = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file1.append(b"foo".to_vec(), move |result| {
                result.unwrap();
                file2.append(b"bar".to_vec(), move |result| {
                    result.unwrap();
                    b.wait();
                });
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();

        let contents = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).ok();

        assert_eq!(contents, b"foobar");
    }

    #[test]
    fn callback_fires_on_io_thread() {
        let path = temp_path();
        std::fs::write(&path, b"check").unwrap();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let on_io = Arc::new(Mutex::new(false));
        let barrier = Arc::new(Barrier::new(2));

        let f = Arc::clone(&on_io);
        let b = Arc::clone(&barrier);
        let file = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file.read_all(move |result| {
                result.unwrap();
                *f.lock().unwrap() = IoTaskRunner::current().is_some();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();
        std::fs::remove_file(&path).ok();

        assert!(*on_io.lock().unwrap(), "callback must run on the IO thread");
    }

    #[test]
    fn read_nonexistent_file_returns_error() {
        let path = temp_path();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let got_error = Arc::new(Mutex::new(false));
        let barrier = Arc::new(Barrier::new(2));

        let e = Arc::clone(&got_error);
        let b = Arc::clone(&barrier);
        let file = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file.read_all(move |result| {
                *e.lock().unwrap() = result.is_err();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();

        assert!(*got_error.lock().unwrap());
    }

    #[test]
    fn chained_write_then_read() {
        let path = temp_path();

        let pool = make_pool();
        let io = IoTaskRunner::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));

        let r = Arc::clone(&received);
        let b = Arc::clone(&barrier);
        let file_w = FileProxy::new(&path, Arc::clone(&pool));
        let file_r = FileProxy::new(&path, Arc::clone(&pool));
        io.post_task(Box::new(move || {
            file_w.write_all(b"round-trip".to_vec(), move |result| {
                result.unwrap();
                file_r.read_all(move |result| {
                    *r.lock().unwrap() = result.unwrap();
                    b.wait();
                });
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();
        std::fs::remove_file(&path).ok();

        assert_eq!(*received.lock().unwrap(), b"round-trip");
    }
}
