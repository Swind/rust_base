//! IO task runner demo (Linux only — requires epoll + eventfd).
//!
//! Three usage patterns:
//!
//!  1. **One-shot watch + re-arming** — reads 3 messages from a pipe,
//!     re-registering a one-shot read watch after each read.  Mirrors
//!     Chromium's `SocketPosix::ReadIfReady` → `ReadCompleted` cycle.
//!  2. **Persistent watch** — stays active across reads until
//!     `stop_watching_file_descriptor` is called explicitly.
//!  3. **Lifetime-safe watch** — dropping the watcher object silences future
//!     callbacks immediately; `IoTaskRunner` holds only `Weak`, so the object
//!     is freed the moment the last strong `Arc` is dropped.
//!
//! Run with:
//!   cargo run --example io_task_runner

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
    #[cfg(not(target_os = "linux"))]
    eprintln!("This example requires Linux (epoll + eventfd).");
}

#[cfg(target_os = "linux")]
mod linux {
    use rust_task::{FdWatchController, FdWatcher, IoTaskRunner, TaskRunner, WatchMode};
    use std::os::unix::io::RawFd;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex, Weak};
    use std::time::Duration;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_pipe() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        assert_eq!(rc, 0, "pipe2 failed");
        (fds[0], fds[1])
    }

    fn write_msg(fd: RawFd, msg: &str) {
        let b = msg.as_bytes();
        unsafe { libc::write(fd, b.as_ptr() as *const libc::c_void, b.len()) };
    }

    fn close_fd(fd: RawFd) {
        unsafe { libc::close(fd) };
    }

    // ── 1. One-shot watch + re-arming ────────────────────────────────────────

    /// Reads `target` messages from a pipe, re-registering a one-shot read
    /// watch after each message.
    ///
    /// The key Chromium pattern here:
    ///   - `watch_file_descriptor(..., persistent=false, ...)` → watch
    ///     auto-removed on first fire.
    ///   - `on_file_can_read_without_blocking` → read data → re-arm via
    ///     `arm()`.
    ///
    /// `self_ref: Mutex<Weak<Self>>` provides a self-`Arc` inside the callback
    /// so we can pass `self` as the watcher again without the caller having to
    /// thread it in.
    struct ReArmingReader {
        fd: RawFd,
        controller: Mutex<FdWatchController>,
        count: AtomicUsize,
        target: usize,
        done: Arc<Barrier>,
        self_ref: Mutex<Weak<Self>>,
    }

    impl ReArmingReader {
        fn new(fd: RawFd, target: usize, done: Arc<Barrier>) -> Arc<Self> {
            let r = Arc::new(Self {
                fd,
                controller: Mutex::new(FdWatchController::new()),
                count: AtomicUsize::new(0),
                target,
                done,
                self_ref: Mutex::new(Weak::new()),
            });
            *r.self_ref.lock().unwrap() = Arc::downgrade(&r);
            r
        }

        /// Register a one-shot read watch. Must be called from the IO thread.
        fn arm(&self) {
            let io = IoTaskRunner::current().expect("must be called on the IO thread");
            let self_arc = self.self_ref.lock().unwrap().upgrade().expect("alive");
            let mut ctrl = self.controller.lock().unwrap();
            io.watch_file_descriptor(
                self.fd,
                false, // one-shot: auto-removed after firing once
                WatchMode::Read,
                &mut *ctrl,
                self_arc as Arc<dyn FdWatcher + Send + Sync>,
            );
        }
    }

    impl FdWatcher for ReArmingReader {
        fn on_file_can_read_without_blocking(&self, fd: RawFd) {
            let mut buf = [0u8; 64];
            let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 64) };
            if n <= 0 {
                return;
            }
            let msg = String::from_utf8_lossy(&buf[..n as usize]).trim().to_string();
            let idx = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            println!("  [{idx}] {msg:?}");

            if idx < self.target {
                self.arm(); // re-arm: the one-shot watch was already removed
            } else {
                self.done.wait();
            }
        }

        fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
    }

    pub fn demo_one_shot_rearm(runner: &Arc<IoTaskRunner>) {
        println!("=== 1. One-shot watch + re-arming (3 messages) ===");

        let (read_fd, write_fd) = make_pipe();
        let done = Arc::new(Barrier::new(2));
        let reader = ReArmingReader::new(read_fd, 3, Arc::clone(&done));

        // Arm the first watch from the IO thread.  Use a barrier to ensure the
        // watch is registered before the main thread writes the first message.
        let r = Arc::clone(&reader);
        let ready = Arc::new(Barrier::new(2));
        let rd = Arc::clone(&ready);
        runner.post_task(Box::new(move || {
            r.arm();
            rd.wait();
        }));
        ready.wait();

        for msg in ["hello", "world", "done"] {
            std::thread::sleep(Duration::from_millis(30));
            write_msg(write_fd, msg);
        }

        done.wait();
        println!("  total received: {}\n", reader.count.load(Ordering::Relaxed));

        close_fd(read_fd);
        close_fd(write_fd);
    }

    // ── 2. Persistent watch ──────────────────────────────────────────────────

    /// Reads from a pipe on every readiness event.  The watch stays active
    /// until `stop_watching_file_descriptor` is called explicitly.
    struct PersistentReader {
        fd: RawFd,
        controller: Mutex<FdWatchController>,
        count: AtomicUsize,
        target: usize,
        done: Arc<Barrier>,
    }

    impl PersistentReader {
        fn new(fd: RawFd, target: usize, done: Arc<Barrier>) -> Arc<Self> {
            Arc::new(Self {
                fd,
                controller: Mutex::new(FdWatchController::new()),
                count: AtomicUsize::new(0),
                target,
                done,
            })
        }
    }

    impl FdWatcher for PersistentReader {
        fn on_file_can_read_without_blocking(&self, fd: RawFd) {
            let mut buf = [0u8; 1];
            unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
            let idx = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            println!("  tick {idx}");

            if idx == self.target {
                // Cancel from inside the callback — safe because the watches
                // lock has already been released by the epoll loop at this point.
                self.controller.lock().unwrap().stop_watching_file_descriptor();
                self.done.wait();
            }
            // Otherwise the persistent watch fires again on the next readiness
            // event.
        }

        fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
    }

    pub fn demo_persistent_watch(runner: &Arc<IoTaskRunner>) {
        println!("=== 2. Persistent watch (5 ticks) ===");

        let (read_fd, write_fd) = make_pipe();
        let done = Arc::new(Barrier::new(2));
        let reader = PersistentReader::new(read_fd, 5, Arc::clone(&done));

        let r = Arc::clone(&reader);
        let runner_clone = Arc::clone(runner);
        let ready = Arc::new(Barrier::new(2));
        let rd = Arc::clone(&ready);
        runner.post_task(Box::new(move || {
            runner_clone.watch_file_descriptor(
                r.fd,
                true, // persistent: stays registered across firings
                WatchMode::Read,
                &mut *r.controller.lock().unwrap(),
                Arc::clone(&r) as Arc<dyn FdWatcher + Send + Sync>,
            );
            rd.wait();
        }));
        ready.wait();

        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(30));
            write_msg(write_fd, "x");
        }

        done.wait();
        println!(
            "  is_watching after stop = {}\n",
            reader.controller.lock().unwrap().is_watching()
        );

        close_fd(read_fd);
        close_fd(write_fd);
    }

    // ── 3. Lifetime-safe watch ───────────────────────────────────────────────

    /// A handler whose lifetime is unrelated to the watch.
    /// The controller is held outside this struct to demonstrate that the
    /// `Weak` stored inside `IoTaskRunner` — not the controller's drop — is
    /// what silences the callbacks.
    struct SilenceableHandler {
        count: AtomicUsize,
    }

    impl FdWatcher for SilenceableHandler {
        fn on_file_can_read_without_blocking(&self, fd: RawFd) {
            let mut buf = [0u8; 1];
            unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
            let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            println!("  callback {n}");
        }

        fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
    }

    pub fn demo_lifetime_safe(runner: &Arc<IoTaskRunner>) {
        println!("=== 3. Lifetime-safe watch (drop handler → callbacks silenced) ===");

        let (read_fd, write_fd) = make_pipe();
        let handler = Arc::new(SilenceableHandler { count: AtomicUsize::new(0) });

        // Keep the controller outside the handler so the two lifetimes are
        // independent.  This clearly shows that callback silencing comes from
        // the Weak<handler> inside IoTaskRunner, not from the controller drop.
        let ctrl = Arc::new(Mutex::new(FdWatchController::new()));

        let h = Arc::clone(&handler);
        let c = Arc::clone(&ctrl);
        let runner_clone = Arc::clone(runner);
        let ready = Arc::new(Barrier::new(2));
        let rd = Arc::clone(&ready);
        runner.post_task(Box::new(move || {
            runner_clone.watch_file_descriptor(
                read_fd,
                true,
                WatchMode::Read,
                &mut *c.lock().unwrap(),
                Arc::clone(&h) as Arc<dyn FdWatcher + Send + Sync>,
            );
            rd.wait();
        }));
        ready.wait();

        // First write — handler is alive, callback fires.
        write_msg(write_fd, "a");
        std::thread::sleep(Duration::from_millis(50));

        let count_before = handler.count.load(Ordering::Relaxed);
        let weak = Arc::downgrade(&handler);

        // Drop the only strong reference.  Because IoTaskRunner holds Weak,
        // not Arc, the object is freed immediately.
        drop(handler);
        assert!(weak.upgrade().is_none(), "handler freed immediately");
        println!("  handler dropped after {count_before} callback(s)");

        // Second write — Weak::upgrade() returns None → silenced.
        write_msg(write_fd, "b");
        std::thread::sleep(Duration::from_millis(50));
        println!("  no further callbacks after drop ✓\n");

        ctrl.lock().unwrap().stop_watching_file_descriptor();
        close_fd(read_fd);
        close_fd(write_fd);
    }

    // ── main ─────────────────────────────────────────────────────────────────

    pub fn run() {
        let runner = IoTaskRunner::new();

        demo_one_shot_rearm(&runner);
        demo_persistent_watch(&runner);
        demo_lifetime_safe(&runner);

        runner.shutdown();
    }
}
