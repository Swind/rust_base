use std::os::unix::io::RawFd;
use std::sync::Weak;
use std::time::Instant;

// ── WatchMode / FdWatcher
// ─────────────────────────────────────────────────────

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
/// the message pump's IO thread when the fd becomes readable or writable.
///
/// Mirrors `MessagePumpForIO::FdWatcher` in Chromium.
pub trait FdWatcher: Send + Sync + 'static {
    /// Called when `fd` can be read without blocking.
    fn on_file_can_read_without_blocking(&self, fd: RawFd);
    /// Called when `fd` can be written without blocking.
    fn on_file_can_write_without_blocking(&self, fd: RawFd);
}

// ── MessagePumpDelegate
// ───────────────────────────────────────────────────

/// The task-layer callback interface a [`MessagePumpForIo`] drives.
///
/// The pump owns the platform event loop; on each iteration it asks the
/// delegate to run any ready work via [`do_work`](Self::do_work), then blocks
/// waiting for fd readiness or a wake-up.  This is the seam that keeps the
/// task-runner layer (queues, monitoring, sequencing) free of platform
/// specifics.
///
/// Mirrors `base::MessagePump::Delegate` in Chromium.
pub trait MessagePumpDelegate: Send + Sync + 'static {
    /// Run every task that is ready *now* (immediate tasks plus delayed tasks
    /// whose deadline has passed).
    ///
    /// Returns the deadline of the earliest not-yet-due delayed task so the
    /// pump can size its wait timeout, or `None` to wait indefinitely.
    fn do_work(&self) -> Option<Instant>;

    /// Called once, on the pump thread, just before the loop starts.
    fn on_run_start(&self) {}
    /// Called once, on the pump thread, just after the loop exits.
    fn on_run_end(&self) {}

    /// Bracket a single FD-readiness callback.  Used for monitor work-item
    /// scoping; the pump calls `begin`/`end` around each `FdWatcher` callback.
    fn begin_work_item(&self) {}
    /// See [`begin_work_item`](Self::begin_work_item).
    fn end_work_item(&self) {}
}

// ── MessagePumpForIo
// ──────────────────────────────────────────────────────

/// A platform IO event loop: blocks waiting for fd readiness, dispatches to
/// [`FdWatcher`]s, and runs task-layer work via a [`MessagePumpDelegate`].
///
/// This is the abstraction that decouples the rest of `rust_io` from a specific
/// OS primitive.  The Linux backend is
/// [`EpollMessagePump`](crate::EpollMessagePump); other platforms (kqueue,
/// IOCP) could provide their own implementation without touching the
/// task-runner layer.
///
/// Mirrors `base::MessagePumpForIO` (specifically `MessagePumpEpoll`) in
/// Chromium.
pub trait MessagePumpForIo: Send + Sync + 'static {
    /// Run the event loop until [`quit`](Self::quit) is called.  Invoked on the
    /// dedicated IO thread.
    fn run(&self, delegate: std::sync::Arc<dyn MessagePumpDelegate>);

    /// Ask the loop to exit.  May be called from any thread; wakes a blocked
    /// loop.
    fn quit(&self);

    /// Wake the loop if it is blocked, so freshly posted work runs promptly.
    /// May be called from any thread.
    fn schedule_work(&self);

    /// Register `fd` for readiness notifications.  **IO thread only.**
    ///
    /// Returns a generation token on success (`None` on failure).  The token is
    /// stored in the caller's [`FdWatchController`] so a stale controller can't
    /// cancel a watch that was re-registered on the same fd.
    fn register_fd(
        &self,
        fd: RawFd,
        persistent: bool,
        mode: WatchMode,
        watcher: Weak<dyn FdWatcher + Send + Sync>,
    ) -> Option<u64>;

    /// Cancel the watch on `fd` if its generation still matches.  Returns
    /// `true` if a watch was removed.
    fn unregister_fd(&self, fd: RawFd, generation: u64) -> bool;
}

// ── FdWatchController
// ─────────────────────────────────────────────────────

/// RAII handle for a single FD watch registration.
///
/// Create one per watched operation (one for read, one for write) as a member
/// variable of the struct that implements `FdWatcher`, then pass a `&mut`
/// reference to `IoTaskRunner::watch_file_descriptor`.  The watch is
/// automatically cancelled when this controller is dropped.
///
/// Mirrors `MessagePumpForIO::FdWatchController` in Chromium.
pub struct FdWatchController {
    pump: Option<Weak<dyn MessagePumpForIo>>,
    fd: RawFd,
    generation: u64,
}

impl FdWatchController {
    pub fn new() -> Self {
        Self { pump: None, fd: -1, generation: 0 }
    }

    /// Bind this controller to an active watch.  Called by
    /// `IoTaskRunner::watch_file_descriptor` after the pump registers the fd.
    pub(crate) fn attach(&mut self, pump: Weak<dyn MessagePumpForIo>, fd: RawFd, generation: u64) {
        self.pump = Some(pump);
        self.fd = fd;
        self.generation = generation;
    }

    /// Cancel the watch explicitly. Returns `true` if a watch was active.
    pub fn stop_watching_file_descriptor(&mut self) -> bool {
        let Some(weak) = self.pump.take() else { return false };
        let Some(pump) = weak.upgrade() else { return false };
        pump.unregister_fd(self.fd, self.generation)
    }

    /// Returns `true` if this controller currently holds an active watch.
    pub fn is_watching(&self) -> bool {
        self.pump.is_some()
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
