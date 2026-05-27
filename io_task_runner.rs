use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

use crate::sequence_token::SequenceToken;
use crate::sequenced_task_runner::{CurrentDefaultHandle, SequencedTaskRunner};
use crate::task_runner::TaskRunner;
use crate::thread_pool::worker_thread::ScopedSequenceToken;

// ── Public types
// ──────────────────────────────────────────────────────────────

/// Which I/O direction(s) to watch on a file descriptor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WatchMode {
    Read,
    Write,
    ReadWrite,
}

/// Callback trait for FD readiness notifications.
///
/// Implement this on your connection/socket struct.  The methods are called on
/// the `IoTaskRunner`'s IO thread when the fd becomes readable or writable.
///
/// Mirrors `MessagePumpForIO::FdWatcher` in Chromium.
pub trait FdWatcher: Send + Sync + 'static {
    /// Called when `fd` can be read without blocking.
    fn on_file_can_read_without_blocking(&self, fd: RawFd);
    /// Called when `fd` can be written without blocking.
    fn on_file_can_write_without_blocking(&self, fd: RawFd);
}

// ── FdWatchController
// ─────────────────────────────────────────────────────────

/// RAII handle for a single FD watch registration.
///
/// Create one per watched operation (one for read, one for write) as a member
/// variable of the struct that implements `FdWatcher`, then pass a `&mut`
/// reference to `IoTaskRunner::watch_file_descriptor`.  The watch is
/// automatically cancelled when this controller is dropped.
///
/// Mirrors `MessagePumpForIO::FdWatchController` in Chromium.
pub struct FdWatchController {
    runner: Option<Weak<IoTaskRunner>>,
    fd: RawFd,
    generation: u64,
}

impl FdWatchController {
    pub fn new() -> Self {
        Self { runner: None, fd: -1, generation: 0 }
    }

    /// Cancel the watch explicitly. Returns `true` if a watch was active.
    pub fn stop_watching_file_descriptor(&mut self) -> bool {
        let Some(weak) = self.runner.take() else { return false };
        let Some(runner) = weak.upgrade() else { return false };
        runner.unregister_fd(self.fd, self.generation)
    }

    /// Returns `true` if this controller currently holds an active watch.
    pub fn is_watching(&self) -> bool {
        self.runner.is_some()
    }
}

impl Default for FdWatchController {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FdWatchController {
    fn drop(&mut self) {
        self.stop_watching_file_descriptor();
    }
}

// ── IoTaskRunner
// ──────────────────────────────────────────────────────────────

thread_local! {
    static CURRENT_IO_RUNNER: std::cell::RefCell<Option<Weak<IoTaskRunner>>> =
        const { std::cell::RefCell::new(None) };
}

// Internal per-registration state.
struct WatchEntry {
    watcher: Weak<dyn FdWatcher + Send + Sync>,
    persistent: bool,
    mode: WatchMode,
    generation: u64,
}

struct DelayedTask {
    deadline: Instant,
    callback: Box<dyn FnOnce() + Send>,
}

// Min-heap ordering (BinaryHeap is max-heap by default).
impl PartialEq for DelayedTask {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}
impl Eq for DelayedTask {}
impl PartialOrd for DelayedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DelayedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deadline.cmp(&self.deadline)
    }
}

/// A `SequencedTaskRunner` backed by a dedicated IO thread and an epoll loop.
///
/// Combines task posting (`post_task`, `post_delayed_task`) with non-blocking
/// FD readiness monitoring (`watch_file_descriptor`), matching Chromium's
/// `SingleThreadTaskRunner` + `MessagePumpForIO` design.
///
/// # Usage pattern (Chromium `SocketPosix` style)
///
/// ```ignore
/// struct Connection {
///     fd: RawFd,
///     read_watcher: FdWatchController,
///     read_callback: Option<Box<dyn FnOnce(Result<usize>) + Send>>,
/// }
///
/// impl Connection {
///     fn read_if_ready(&mut self, buf: &mut [u8], cb: impl FnOnce(Result<usize>) + Send + 'static) {
///         match syscall_read(self.fd, buf) {
///             Err(e) if e.kind() == WouldBlock => {
///                 self.read_callback = Some(Box::new(cb));
///                 IoTaskRunner::current().unwrap()
///                     .watch_file_descriptor(self.fd, false, WatchMode::Read,
///                                           &mut self.read_watcher, self_arc.clone());
///             }
///             result => cb(result),
///         }
///     }
/// }
///
/// impl FdWatcher for Connection {
///     fn on_file_can_read_without_blocking(&self, _fd: RawFd) {
///         // watch was non-persistent: already removed
///         let cb = self.read_callback.take().unwrap();
///         cb(Ok(0)); // user calls read_if_ready again to re-arm
///     }
///     fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
/// }
/// ```
pub struct IoTaskRunner {
    epoll_fd: RawFd,
    wake_fd: RawFd, // eventfd: written by post_task to interrupt epoll_wait
    tasks: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,
    delayed_tasks: Mutex<BinaryHeap<DelayedTask>>,
    watches: Mutex<HashMap<RawFd, WatchEntry>>,
    generation_counter: AtomicU64,
    shutdown: AtomicBool,
    token: SequenceToken,
    thread_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl IoTaskRunner {
    /// Create a new `IoTaskRunner` and immediately start its IO thread.
    pub fn new() -> Arc<Self> {
        let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        assert!(epoll_fd >= 0, "epoll_create1 failed");

        let wake_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(wake_fd >= 0, "eventfd failed");

        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: wake_fd as u64 };
        let rc = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, wake_fd, &mut ev) };
        assert_eq!(rc, 0, "epoll_ctl(ADD wake_fd) failed");

        let runner = Arc::new(Self {
            epoll_fd,
            wake_fd,
            tasks: Mutex::new(VecDeque::new()),
            delayed_tasks: Mutex::new(BinaryHeap::new()),
            watches: Mutex::new(HashMap::new()),
            generation_counter: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            token: SequenceToken::create(),
            thread_handle: Mutex::new(None),
        });

        let cloned = Arc::clone(&runner);
        let handle = thread::spawn(move || run_loop(cloned));
        *runner.thread_handle.lock().unwrap() = Some(handle);
        runner
    }

    /// Returns the `IoTaskRunner` bound to the current thread, if any.
    ///
    /// Mirrors `CurrentIOThread::Get()` in Chromium.
    pub fn current() -> Option<Arc<Self>> {
        CURRENT_IO_RUNNER.with(|r| r.borrow().as_ref().and_then(|w| w.upgrade()))
    }

    /// Register `fd` for readiness notifications via `watcher`.
    ///
    /// - `persistent = false`: the watch fires once and is automatically
    ///   removed (Chromium's per-operation pattern).  The user re-arms it on
    ///   the next `read_if_ready` / `write_if_ready` call.
    /// - `persistent = true`: the watch fires on every readiness event until
    ///   `controller.stop_watching_file_descriptor()` is called.
    ///
    /// Any previous watch on `controller` is cancelled before the new one is
    ///  registered.  Returns `true` on success.
    ///
    /// Mirrors `MessagePumpForIO::WatchFileDescriptor` in Chromium.
    pub fn watch_file_descriptor(
        self: &Arc<Self>,
        fd: RawFd,
        persistent: bool,
        mode: WatchMode,
        controller: &mut FdWatchController,
        watcher: Arc<dyn FdWatcher + Send + Sync>,
    ) -> bool {
        controller.stop_watching_file_descriptor();

        let generation = self.generation_counter.fetch_add(1, Ordering::Relaxed);
        let epoll_events = mode_to_epoll_events(mode);
        let mut ev = libc::epoll_event { events: epoll_events, u64: fd as u64 };

        let rc = unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if rc < 0 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::EEXIST {
                let rc2 =
                    unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
                if rc2 < 0 {
                    return false;
                }
            } else {
                return false;
            }
        }

        self.watches.lock().unwrap().insert(
            fd,
            WatchEntry { watcher: Arc::downgrade(&watcher), persistent, mode, generation },
        );

        controller.fd = fd;
        controller.generation = generation;
        controller.runner = Some(Arc::downgrade(self));
        true
    }

    /// Shut down the IO thread.  Pending tasks are abandoned.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.wake();
        if let Some(handle) = self.thread_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    fn wake(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(self.wake_fd, &raw const val as *const libc::c_void, 8);
        }
    }

    fn unregister_fd(&self, fd: RawFd, generation: u64) -> bool {
        let mut watches = self.watches.lock().unwrap();
        match watches.get(&fd) {
            Some(e) if e.generation == generation => {
                watches.remove(&fd);
                let mut dummy = libc::epoll_event { events: 0, u64: 0 };
                unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, &mut dummy) };
                true
            }
            _ => false,
        }
    }
}

impl Drop for IoTaskRunner {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.epoll_fd);
            libc::close(self.wake_fd);
        }
    }
}

impl TaskRunner for IoTaskRunner {
    fn post_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        self.tasks.lock().unwrap().push_back(callback);
        self.wake();
        true
    }

    fn post_delayed_task(
        &self,
        callback: Box<dyn FnOnce() + Send + 'static>,
        delay: Duration,
    ) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let deadline = Instant::now() + delay;
        self.delayed_tasks.lock().unwrap().push(DelayedTask { deadline, callback });
        self.wake();
        true
    }

    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool {
        let reply_runner = crate::sequenced_task_runner::current_default();
        self.post_task(Box::new(move || {
            task();
            if let Some(r) = reply_runner {
                r.post_task(reply);
            }
        }))
    }
}

impl SequencedTaskRunner for IoTaskRunner {
    fn post_non_nestable_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        self.post_task(callback)
    }

    fn runs_tasks_in_current_sequence(&self) -> bool {
        SequenceToken::current() == Some(self.token)
    }

    fn sequence_token(&self) -> SequenceToken {
        self.token
    }
}

// ── Helpers
// ───────────────────────────────────────────────────────────────────

fn mode_to_epoll_events(mode: WatchMode) -> u32 {
    match mode {
        WatchMode::Read => libc::EPOLLIN as u32,
        WatchMode::Write => libc::EPOLLOUT as u32,
        WatchMode::ReadWrite => (libc::EPOLLIN | libc::EPOLLOUT) as u32,
    }
}

// ── IO event loop
// ─────────────────────────────────────────────────────────────

fn run_loop(runner: Arc<IoTaskRunner>) {
    let _token_guard = ScopedSequenceToken::new(runner.token);
    let _default_handle =
        CurrentDefaultHandle::new(Arc::clone(&runner) as Arc<dyn SequencedTaskRunner>);
    CURRENT_IO_RUNNER.with(|r| *r.borrow_mut() = Some(Arc::downgrade(&runner)));

    loop {
        // Drain immediate tasks.
        loop {
            let task = runner.tasks.lock().unwrap().pop_front();
            let Some(task) = task else { break };
            task();
        }

        // Run delayed tasks whose deadline has passed.
        loop {
            let task = {
                let now = Instant::now();
                let mut q = runner.delayed_tasks.lock().unwrap();
                if q.peek().is_some_and(|t| t.deadline <= now) { q.pop() } else { None }
            };
            let Some(t) = task else { break };
            (t.callback)();
        }

        if runner.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Calculate epoll_wait timeout from the next delayed task's deadline.
        let timeout_ms: i32 = {
            let q = runner.delayed_tasks.lock().unwrap();
            match q.peek() {
                None => -1,
                Some(next) => {
                    let now = Instant::now();
                    if next.deadline <= now {
                        0
                    } else {
                        let ms = (next.deadline - now).as_millis();
                        ms.min(i32::MAX as u128) as i32
                    }
                }
            }
        };

        let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
        let n = unsafe { libc::epoll_wait(runner.epoll_fd, events.as_mut_ptr(), 64, timeout_ms) };

        if n < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            break;
        }

        for ev in &events[..n as usize] {
            let fd = ev.u64 as RawFd;
            let ev_flags = ev.events;

            if fd == runner.wake_fd {
                let mut buf = [0u8; 8];
                unsafe {
                    libc::read(runner.wake_fd, buf.as_mut_ptr() as *mut libc::c_void, 8);
                };
                continue;
            }

            // Hold the lock only long enough to extract the watcher and decide
            // whether to remove the entry.  Release before calling callbacks so
            // the callback can call watch_file_descriptor without deadlocking.
            let (watcher_opt, can_read, can_write) = {
                let mut watches = runner.watches.lock().unwrap();
                let Some(entry) = watches.get(&fd) else { continue };
                let can_read = (ev_flags & libc::EPOLLIN as u32) != 0
                    && matches!(entry.mode, WatchMode::Read | WatchMode::ReadWrite);
                let can_write = (ev_flags & libc::EPOLLOUT as u32) != 0
                    && matches!(entry.mode, WatchMode::Write | WatchMode::ReadWrite);
                let watcher = entry.watcher.upgrade();
                if !entry.persistent {
                    watches.remove(&fd);
                    let mut dummy = libc::epoll_event { events: 0, u64: 0 };
                    unsafe {
                        libc::epoll_ctl(runner.epoll_fd, libc::EPOLL_CTL_DEL, fd, &mut dummy)
                    };
                }
                (watcher, can_read, can_write)
            };

            if let Some(w) = watcher_opt {
                if can_read {
                    w.on_file_can_read_without_blocking(fd);
                }
                if can_write {
                    w.on_file_can_write_without_blocking(fd);
                }
            }
        }
    }

    CURRENT_IO_RUNNER.with(|r| *r.borrow_mut() = None);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    fn pipe() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        assert_eq!(rc, 0);
        (fds[0], fds[1])
    }

    fn close_fd(fd: RawFd) {
        unsafe { libc::close(fd) };
    }

    fn write_byte(fd: RawFd) {
        let buf = [1u8; 1];
        unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, 1) };
    }

    // ── post_task ────────────────────────────────────────────────────────────

    #[test]
    fn post_task_executes() {
        let runner = IoTaskRunner::new();
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        runner.post_task(Box::new(move || {
            b.wait();
        }));
        barrier.wait();
        runner.shutdown();
    }

    #[test]
    fn runs_tasks_in_current_sequence_true_inside_task() {
        let runner = IoTaskRunner::new();
        let result = Arc::new(Mutex::new(false));
        let r = Arc::clone(&result);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let runner_clone = Arc::clone(&runner);
        runner.post_task(Box::new(move || {
            *r.lock().unwrap() = runner_clone.runs_tasks_in_current_sequence();
            b.wait();
        }));
        barrier.wait();
        runner.shutdown();
        assert!(*result.lock().unwrap());
    }

    #[test]
    fn post_delayed_task_fires_after_delay() {
        let runner = IoTaskRunner::new();
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let fired = Arc::new(Mutex::new(false));
        let f = Arc::clone(&fired);
        runner.post_delayed_task(
            Box::new(move || {
                *f.lock().unwrap() = true;
                b.wait();
            }),
            Duration::from_millis(20),
        );
        barrier.wait();
        runner.shutdown();
        assert!(*fired.lock().unwrap());
    }

    // ── watch_file_descriptor ────────────────────────────────────────────────

    struct PipeWatcher {
        read_count: Mutex<usize>,
        barrier: Arc<Barrier>,
    }

    impl FdWatcher for PipeWatcher {
        fn on_file_can_read_without_blocking(&self, fd: RawFd) {
            let mut buf = [0u8; 4];
            unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 4) };
            let mut g = self.read_count.lock().unwrap();
            *g += 1;
            if *g == 1 {
                self.barrier.wait();
            }
        }
        fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
    }

    #[test]
    fn watch_read_fires_when_data_available() {
        let runner = IoTaskRunner::new();
        let (read_fd, write_fd) = pipe();

        let barrier = Arc::new(Barrier::new(2));
        let watcher =
            Arc::new(PipeWatcher { read_count: Mutex::new(0), barrier: Arc::clone(&barrier) });

        let runner_clone = Arc::clone(&runner);
        let w = Arc::clone(&watcher);
        runner.post_task(Box::new(move || {
            let mut ctrl = FdWatchController::new();
            runner_clone.watch_file_descriptor(read_fd, true, WatchMode::Read, &mut ctrl, w);
            // Intentionally leak ctrl here: the test drives the lifecycle.
            std::mem::forget(ctrl);
        }));

        write_byte(write_fd);
        barrier.wait();

        runner.shutdown();
        assert!(*watcher.read_count.lock().unwrap() >= 1);
        close_fd(read_fd);
        close_fd(write_fd);
    }

    #[test]
    fn non_persistent_watch_fires_once() {
        let runner = IoTaskRunner::new();
        let (read_fd, write_fd) = pipe();

        let fire_count = Arc::new(Mutex::new(0usize));
        let barrier = Arc::new(Barrier::new(2));

        struct OneShotWatcher {
            count: Arc<Mutex<usize>>,
            barrier: Arc<Barrier>,
        }
        impl FdWatcher for OneShotWatcher {
            fn on_file_can_read_without_blocking(&self, fd: RawFd) {
                let mut buf = [0u8; 1];
                unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
                let mut g = self.count.lock().unwrap();
                *g += 1;
                if *g == 1 {
                    self.barrier.wait();
                }
            }
            fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
        }

        let watcher = Arc::new(OneShotWatcher {
            count: Arc::clone(&fire_count),
            barrier: Arc::clone(&barrier),
        });

        let runner_clone = Arc::clone(&runner);
        let w = Arc::clone(&watcher);
        runner.post_task(Box::new(move || {
            let mut ctrl = FdWatchController::new();
            // persistent = false: fires once then self-removes
            runner_clone.watch_file_descriptor(read_fd, false, WatchMode::Read, &mut ctrl, w);
            std::mem::forget(ctrl);
        }));

        write_byte(write_fd);
        barrier.wait();

        // Write more data; the watch is gone so no second fire.
        write_byte(write_fd);
        std::thread::sleep(Duration::from_millis(50));

        runner.shutdown();
        assert_eq!(*fire_count.lock().unwrap(), 1);
        close_fd(read_fd);
        close_fd(write_fd);
    }

    #[test]
    fn stop_watching_cancels_future_callbacks() {
        let runner = IoTaskRunner::new();
        let (read_fd, write_fd) = pipe();

        let fire_count = Arc::new(Mutex::new(0usize));
        let registered = Arc::new(Barrier::new(2));

        struct CountWatcher(Arc<Mutex<usize>>);
        impl FdWatcher for CountWatcher {
            fn on_file_can_read_without_blocking(&self, fd: RawFd) {
                let mut buf = [0u8; 1];
                unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
                *self.0.lock().unwrap() += 1;
            }
            fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
        }

        let watcher = Arc::new(CountWatcher(Arc::clone(&fire_count)));
        let ctrl_holder: Arc<Mutex<Option<FdWatchController>>> = Arc::new(Mutex::new(None));
        let ch = Arc::clone(&ctrl_holder);

        let runner_clone = Arc::clone(&runner);
        let w = Arc::clone(&watcher);
        let reg = Arc::clone(&registered);
        runner.post_task(Box::new(move || {
            let mut ctrl = FdWatchController::new();
            runner_clone.watch_file_descriptor(read_fd, true, WatchMode::Read, &mut ctrl, w);
            *ch.lock().unwrap() = Some(ctrl);
            reg.wait();
        }));

        registered.wait(); // watch is now active

        // Cancel the watch from the main thread via the controller.
        ctrl_holder.lock().unwrap().as_mut().unwrap().stop_watching_file_descriptor();

        // Write data; should not fire since watch was cancelled.
        write_byte(write_fd);
        std::thread::sleep(Duration::from_millis(50));

        runner.shutdown();
        assert_eq!(*fire_count.lock().unwrap(), 0);
        close_fd(read_fd);
        close_fd(write_fd);
    }

    #[test]
    fn dropping_watcher_silences_callbacks() {
        let runner = IoTaskRunner::new();
        let (read_fd, write_fd) = pipe();

        let fire_count = Arc::new(Mutex::new(0usize));
        let registered = Arc::new(Barrier::new(2));

        struct CountWatcher(Arc<Mutex<usize>>);
        impl FdWatcher for CountWatcher {
            fn on_file_can_read_without_blocking(&self, fd: RawFd) {
                let mut buf = [0u8; 1];
                unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
                *self.0.lock().unwrap() += 1;
            }
            fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
        }

        let watcher = Arc::new(CountWatcher(Arc::clone(&fire_count)));
        let runner_clone = Arc::clone(&runner);
        let w = Arc::clone(&watcher);
        let reg = Arc::clone(&registered);
        runner.post_task(Box::new(move || {
            let mut ctrl = FdWatchController::new();
            runner_clone.watch_file_descriptor(read_fd, true, WatchMode::Read, &mut ctrl, w);
            std::mem::forget(ctrl);
            reg.wait();
        }));

        registered.wait();

        // Drop the Arc — the epoll loop holds only Weak, so the object is freed.
        let weak = Arc::downgrade(&watcher);
        drop(watcher);
        assert!(weak.upgrade().is_none());

        // Write data; callback is a no-op because watcher is gone.
        write_byte(write_fd);
        std::thread::sleep(Duration::from_millis(50));

        runner.shutdown();
        assert_eq!(*fire_count.lock().unwrap(), 0);
        close_fd(read_fd);
        close_fd(write_fd);
    }
}
