//! The reactor: a single epoll thread (`rust_io::IoTaskRunner`) whose
//! fd-readiness callbacks are turned into `Waker` wake-ups.
//!
//! This is the load-bearing piece of the whole spike. In `rust_io` an
//! [`FdWatcher`] fires a *callback* on the IO thread when an fd becomes ready;
//! here that callback instead **wakes the task** that is parked on the fd. That
//! single substitution ("call a `Waker` instead of running a closure") is the
//! entire difference between the callback model and the Future model.
//!
//! ## Why one-shot re-arm
//!
//! `rust_io`'s epoll backend is **level-triggered**. A `persistent` watch would
//! therefore keep firing while the fd stays readable but the task hasn't been
//! re-polled yet — a busy spin on the IO thread. So instead we arm a
//! **one-shot** watch (`persistent = false`), which auto-removes itself from
//! epoll after it fires. Each `.await` that hits `WouldBlock` re-arms. This
//! mirrors the `read_if_ready` pattern documented in the `rust-base` skill.
//!
//! ## Read and write at the same time
//!
//! `rust_io` keeps a single watch entry per fd, but a `Source` can still have a
//! reader and a writer waiting at once (needed for a split/cloned stream): we
//! track the union of the pending directions and (re-)arm the fd with the
//! combined [`WatchMode`]. When a one-shot fires for one direction, the entry
//! is removed and we re-arm whatever direction is still pending.

use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll, Waker};

use rust_io::{FdWatchController, FdWatcher, IoTaskRunner, WatchMode};
use rust_task::TaskRunner;

pub(crate) struct Reactor {
    pub(crate) io: Arc<IoTaskRunner>,
}

static REACTOR: OnceLock<Reactor> = OnceLock::new();

/// The process-wide reactor, started lazily on first use.
pub(crate) fn reactor() -> &'static Reactor {
    REACTOR.get_or_init(|| Reactor { io: IoTaskRunner::new() })
}

/// The `IoTaskRunner` a future should register I/O / timers with: **the reactor
/// of the [`Runtime`](crate::Runtime) the current task is bound to**, or the
/// global [`reactor`] singleton if we are outside any task.
///
/// This is the forward half of the runtime pairing: a task carries its runtime
/// (via [`crate::local`]), so `await`ing I/O arms the reactor that runtime was
/// configured with — regardless of which thread the task happens to be polled
/// on. That decoupling is what lets a parallel-pool executor share, or not
/// share, a reactor with others, and what makes thread-per-core fall out (each
/// lane's runtime points its reactor at itself).
pub(crate) fn io_runner() -> Arc<IoTaskRunner> {
    crate::runtime::current_or_global().reactor()
}

struct SourceState {
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    /// Readiness latched by the IO-thread callback, consumed by the next poll.
    read_ready: bool,
    write_ready: bool,
    /// Current one-shot interest registered in epoll, if any.
    armed: Option<WatchMode>,
    /// The reactor this fd is bound to, latched on the first arm. Reused for
    /// every later (re-)arm — crucially the re-arm issued from the IO-thread
    /// callback, where no task context is installed and `io_runner()` would
    /// otherwise fall back to the global reactor.
    reactor: Option<Arc<IoTaskRunner>>,
}

/// Per-fd reactor state. Implements [`FdWatcher`]; the epoll loop holds only a
/// `Weak` to it, so the owning I/O object must keep the `Arc` alive.
///
/// A `Source` is identified by its raw `fd` number, which is only meaningful
/// while the owning I/O object (e.g. [`crate::net::Async`]) is alive and holds
/// the descriptor open. Re-arming is asynchronous (posted to the IO thread), so
/// if the object is dropped — closing the fd — between a re-arm being posted
/// and applied, the stale arm could in principle register a watch on a fd
/// number the OS has since reused. This is self-healing in practice: the watch
/// is one-shot and carries only a `Weak<Source>`, so once this `Source` is gone
/// the callback no-ops and the entry is removed. The invariant a caller must
/// uphold is the usual `rust_io` one — keep the I/O object alive until its
/// awaited operations resolve.
pub(crate) struct Source {
    fd: RawFd,
    /// A weak self-reference so callbacks (which only get `&self`) can rebuild
    /// an `Arc<Source>` to re-arm the watch.
    me: Weak<Source>,
    state: Mutex<SourceState>,
    /// Kept across re-arms; the watch registration object lives here.
    controller: Arc<Mutex<FdWatchController>>,
}

impl Source {
    pub(crate) fn new(fd: RawFd) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            fd,
            me: me.clone(),
            state: Mutex::new(SourceState {
                read_waker: None,
                write_waker: None,
                read_ready: false,
                write_ready: false,
                armed: None,
                reactor: None,
            }),
            controller: Arc::new(Mutex::new(FdWatchController::new())),
        })
    }

    pub(crate) fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut s = self.state.lock().unwrap();
        if s.read_ready {
            s.read_ready = false;
            return Poll::Ready(());
        }
        s.read_waker = Some(cx.waker().clone());
        self.update_arm(&mut s);
        Poll::Pending
    }

    pub(crate) fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut s = self.state.lock().unwrap();
        if s.write_ready {
            s.write_ready = false;
            return Poll::Ready(());
        }
        s.write_waker = Some(cx.waker().clone());
        self.update_arm(&mut s);
        Poll::Pending
    }

    /// Re-arm epoll to match the directions currently waited on. A one-shot
    /// watch is (re-)registered only when the desired interest changes.
    fn update_arm(&self, s: &mut SourceState) {
        let desired = match (s.read_waker.is_some(), s.write_waker.is_some()) {
            (true, true) => Some(WatchMode::ReadWrite),
            (true, false) => Some(WatchMode::Read),
            (false, true) => Some(WatchMode::Write),
            (false, false) => None,
        };
        if desired != s.armed {
            s.armed = desired;
            if let Some(mode) = desired {
                // Bind to the current task's reactor on the first arm; reuse it
                // for every subsequent (re-)arm, including from the IO-thread
                // callback where no task context is installed.
                let io = s.reactor.get_or_insert_with(io_runner).clone();
                if let Some(me) = self.me.upgrade() {
                    me.arm(io, mode);
                }
            }
        }
    }

    /// Register a one-shot epoll watch for `mode` on `io`. Must run on the IO
    /// thread, so we hop there via `post_task` — exactly the "get onto the IO
    /// thread first" rule from `rust_io`.
    fn arm(self: Arc<Self>, io: Arc<IoTaskRunner>, mode: WatchMode) {
        let io_inner = io.clone();
        let controller = self.controller.clone();
        let fd = self.fd;
        let watcher: Arc<dyn FdWatcher + Send + Sync> = self;
        io.post_task(Box::new(move || {
            let mut ctrl = controller.lock().unwrap();
            io_inner
                .watch_file_descriptor(fd, /* persistent= */ false, mode, &mut ctrl, watcher);
        }));
    }
}

impl FdWatcher for Source {
    fn on_file_can_read_without_blocking(&self, _fd: RawFd) {
        let waker = {
            let mut s = self.state.lock().unwrap();
            s.armed = None; // the one-shot watch removed itself from epoll
            s.read_ready = true;
            let w = s.read_waker.take();
            self.update_arm(&mut s); // re-arm the writer if it is still waiting
            w
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    fn on_file_can_write_without_blocking(&self, _fd: RawFd) {
        let waker = {
            let mut s = self.state.lock().unwrap();
            s.armed = None;
            s.write_ready = true;
            let w = s.write_waker.take();
            self.update_arm(&mut s); // re-arm the reader if it is still waiting
            w
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}
