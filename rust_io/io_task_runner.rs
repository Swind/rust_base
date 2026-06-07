use std::collections::{BinaryHeap, VecDeque};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rust_task::sequence_token::SequenceToken;
use rust_task::sequenced_task_runner::{CurrentDefaultHandle, SequencedTaskRunner};
use rust_task::task_monitor::{TaskMonitor, WorkerSlot};
use rust_task::task_runner::TaskRunner;
use rust_task::thread_pool::worker_thread::ScopedSequenceToken;

use crate::epoll_pump::EpollMessagePump;
use crate::message_pump::{
    FdWatchController, FdWatcher, MessagePumpDelegate, MessagePumpForIo, WatchMode,
};

// ── IoTaskRunner
// ──────────────────────────────────────────────────────────────

thread_local! {
    static CURRENT_IO_RUNNER: std::cell::RefCell<Option<std::sync::Weak<IoTaskRunner>>> =
        const { std::cell::RefCell::new(None) };
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

/// A `SequencedTaskRunner` backed by a dedicated IO thread and a
/// [`MessagePumpForIo`].
///
/// Combines task posting (`post_task`, `post_delayed_task`) with non-blocking
/// FD readiness monitoring (`watch_file_descriptor`), matching Chromium's
/// `SingleThreadTaskRunner` + `MessagePumpForIO` design.  The task-runner
/// concerns live here; the platform event loop is delegated to the pump
/// (`EpollMessagePump` on Linux), so this type carries no epoll specifics.
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
    pump: Arc<dyn MessagePumpForIo>,
    tasks: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,
    delayed_tasks: Mutex<BinaryHeap<DelayedTask>>,
    shutdown: AtomicBool,
    token: SequenceToken,
    thread_handle: Mutex<Option<thread::JoinHandle<()>>>,
    monitor: Option<Arc<TaskMonitor>>,
    // The IO thread's monitor slot, set on the IO thread when the loop starts.
    // Used to bracket both task execution (in do_work) and FD callbacks.
    monitor_slot: Mutex<Option<WorkerSlot>>,
}

impl IoTaskRunner {
    /// Create a new `IoTaskRunner` (backed by an epoll pump) and immediately
    /// start its IO thread.
    pub fn new() -> Arc<Self> {
        Self::with_pump_impl(EpollMessagePump::new(), None)
    }

    /// Create a new `IoTaskRunner` with task monitoring enabled.
    ///
    /// Regular tasks posted via `post_task` / `post_delayed_task` are timed
    /// (queue + execution).  The IO thread is registered with the monitor for
    /// hang detection; IO callbacks (`on_file_can_*`) are also bracketed by
    /// `WorkerSlot::task_started/finished`.
    pub fn new_with_monitor(monitor: Arc<TaskMonitor>) -> Arc<Self> {
        Self::with_pump_impl(EpollMessagePump::new(), Some(monitor))
    }

    /// Create a new `IoTaskRunner` driven by a custom [`MessagePumpForIo`]
    /// backend, and start its IO thread.
    pub fn with_pump(pump: Arc<dyn MessagePumpForIo>) -> Arc<Self> {
        Self::with_pump_impl(pump, None)
    }

    fn with_pump_impl(
        pump: Arc<dyn MessagePumpForIo>,
        monitor: Option<Arc<TaskMonitor>>,
    ) -> Arc<Self> {
        let runner = Self::build(pump, monitor);
        let runner_for_thread = Arc::clone(&runner);
        let handle = thread::spawn(move || runner_for_thread.drive());
        *runner.thread_handle.lock().unwrap() = Some(handle);
        runner
    }

    /// Build a runner **without** starting an IO thread.
    fn build(pump: Arc<dyn MessagePumpForIo>, monitor: Option<Arc<TaskMonitor>>) -> Arc<Self> {
        Arc::new(Self {
            pump,
            tasks: Mutex::new(VecDeque::new()),
            delayed_tasks: Mutex::new(BinaryHeap::new()),
            shutdown: AtomicBool::new(false),
            token: SequenceToken::create(),
            thread_handle: Mutex::new(None),
            monitor,
            monitor_slot: Mutex::new(None),
        })
    }

    /// Create an `IoTaskRunner` **without** spawning an IO thread; you drive
    /// its loop yourself on the calling thread via
    /// [`run_on_current_thread`].
    ///
    /// This is the "make the current thread the message loop" mode — Chromium's
    /// pattern of running a `MessageLoop`/`RunLoop` on an existing thread (e.g.
    /// the main thread) rather than a dedicated one.
    ///
    /// [`run_on_current_thread`]: Self::run_on_current_thread
    pub fn without_thread() -> Arc<Self> {
        Self::build(EpollMessagePump::new(), None)
    }

    /// Drive this runner's pump loop **on the calling thread**, returning when
    /// [`shutdown`](Self::shutdown) (or the pump's `quit`) is called —
    /// typically from a task running on this very loop.
    ///
    /// Establishes the calling thread's sequence identity and "current IO
    /// runner" for the duration, so [`IoTaskRunner::current`] works and fds are
    /// watched here. Intended for runners from
    /// [`without_thread`](Self::without_thread).
    pub fn run_on_current_thread(self: &Arc<Self>) {
        self.drive();
    }

    /// Thread setup + pump loop. Shared by the spawned-thread path and
    /// [`run_on_current_thread`](Self::run_on_current_thread).
    fn drive(self: &Arc<Self>) {
        // Task-layer thread setup: establish this thread's sequence identity
        // and "current IO runner" before handing control to the pump loop.
        let _token_guard = ScopedSequenceToken::new(self.token);
        let _default_handle =
            CurrentDefaultHandle::new(Arc::clone(self) as Arc<dyn SequencedTaskRunner>);
        CURRENT_IO_RUNNER.with(|r| *r.borrow_mut() = Some(Arc::downgrade(self)));

        let pump = Arc::clone(&self.pump);
        pump.run(Arc::clone(self) as Arc<dyn MessagePumpDelegate>);

        CURRENT_IO_RUNNER.with(|r| *r.borrow_mut() = None);
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
    /// registered.  Returns `true` on success.
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

        match self.pump.register_fd(fd, persistent, mode, Arc::downgrade(&watcher)) {
            Some(generation) => {
                let weak_pump: std::sync::Weak<dyn MessagePumpForIo> = Arc::downgrade(&self.pump);
                controller.attach(weak_pump, fd, generation);
                true
            }
            None => false,
        }
    }

    /// Shut down the IO thread.  Pending tasks are abandoned.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.pump.quit();
        if let Some(handle) = self.thread_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for IoTaskRunner {
    fn drop(&mut self) {
        // Ensure the IO thread is stopped and joined even if `shutdown` was not
        // called explicitly, so the pump (and its fds) are released cleanly.
        if let Some(handle) = self.thread_handle.lock().unwrap().take() {
            self.pump.quit();
            let _ = handle.join();
        }
    }
}

impl MessagePumpDelegate for IoTaskRunner {
    fn do_work(&self) -> Option<Instant> {
        let slot = self.monitor_slot.lock().unwrap();

        // Drain immediate tasks.
        loop {
            let task = self.tasks.lock().unwrap().pop_front();
            let Some(task) = task else { break };
            if let Some(ref s) = *slot {
                s.task_started();
            }
            task();
            if let Some(ref s) = *slot {
                s.task_finished();
            }
        }

        // Run delayed tasks whose deadline has passed.
        loop {
            let task = {
                let now = Instant::now();
                let mut q = self.delayed_tasks.lock().unwrap();
                if q.peek().is_some_and(|t| t.deadline <= now) { q.pop() } else { None }
            };
            let Some(t) = task else { break };
            if let Some(ref s) = *slot {
                s.task_started();
            }
            (t.callback)();
            if let Some(ref s) = *slot {
                s.task_finished();
            }
        }

        // Report the next delayed deadline so the pump can size its wait.
        self.delayed_tasks.lock().unwrap().peek().map(|t| t.deadline)
    }

    fn on_run_start(&self) {
        if let Some(m) = self.monitor.as_ref() {
            *self.monitor_slot.lock().unwrap() = Some(m.register_worker());
        }
    }

    fn on_run_end(&self) {
        *self.monitor_slot.lock().unwrap() = None;
    }

    fn begin_work_item(&self) {
        if let Some(ref s) = *self.monitor_slot.lock().unwrap() {
            s.task_started();
        }
    }

    fn end_work_item(&self) {
        if let Some(ref s) = *self.monitor_slot.lock().unwrap() {
            s.task_finished();
        }
    }
}

impl TaskRunner for IoTaskRunner {
    fn post_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let callback = match self.monitor.as_ref() {
            Some(m) => m.wrap_task(callback),
            None => callback,
        };
        self.tasks.lock().unwrap().push_back(callback);
        self.pump.schedule_work();
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
        let callback = match self.monitor.as_ref() {
            Some(m) => m.wrap_task(callback),
            None => callback,
        };
        let deadline = Instant::now() + delay;
        self.delayed_tasks.lock().unwrap().push(DelayedTask { deadline, callback });
        self.pump.schedule_work();
        true
    }

    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool {
        let reply_runner = rust_task::sequenced_task_runner::current_default();
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
