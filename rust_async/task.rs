//! Task-layer conveniences mirroring `async_std::task`: [`sleep`], [`timeout`],
//! [`yield_now`], and [`spawn_blocking`].
//!
//! Each one is a thin Future façade over a primitive `rust_task`/`rust_io`
//! already provides:
//!
//! - [`sleep`]/[`timeout`] → `IoTaskRunner::post_delayed_task` (the reactor's
//!   timer), turned into a `Waker` wake-up.
//! - [`spawn_blocking`] → a dedicated `rust_task::ThreadPool`, so a blocking
//!   syscall never occupies an executor worker — the same split async-std makes
//!   between its executor and its `blocking` pool.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use rust_task::{TaskRunner, TaskTraits, ThreadPool};

use crate::executor::{JoinHandle, spawn};
use crate::reactor::io_runner;

// ── yield_now ───────────────────────────────────────────────────────────────

/// Yield once, letting other ready tasks run before this one continues.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Future returned by [`yield_now`].
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            // Re-schedule immediately so we resume after others get a turn.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

// ── sleep ───────────────────────────────────────────────────────────────────

/// Sleep for `dur`, driven by the reactor's delayed-task timer.
///
/// Not cancellable: dropping the returned [`Timer`] before it fires leaves the
/// delayed task queued on the reactor until `dur` elapses (it then fires into
/// an empty waker slot and is dropped). See the crate-level "Known limitations".
pub fn sleep(dur: Duration) -> Timer {
    Timer {
        dur,
        state: Arc::new(Mutex::new(TimerState { fired: false, waker: None })),
        scheduled: false,
    }
}

struct TimerState {
    fired: bool,
    waker: Option<Waker>,
}

/// Future returned by [`sleep`]; also reused internally by [`timeout`].
pub struct Timer {
    dur: Duration,
    state: Arc<Mutex<TimerState>>,
    scheduled: bool,
}

impl Future for Timer {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        {
            let mut s = this.state.lock().unwrap();
            if s.fired {
                return Poll::Ready(());
            }
            s.waker = Some(cx.waker().clone());
        }
        if !this.scheduled {
            this.scheduled = true;
            let state = this.state.clone();
            io_runner().post_delayed_task(
                Box::new(move || {
                    let waker = {
                        let mut s = state.lock().unwrap();
                        s.fired = true;
                        s.waker.take()
                    };
                    if let Some(w) = waker {
                        w.wake();
                    }
                }),
                this.dur,
            );
        }
        Poll::Pending
    }
}

// ── timeout ─────────────────────────────────────────────────────────────────

/// Error returned by [`timeout`] when the deadline elapses first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutError;

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "future timed out")
    }
}

impl std::error::Error for TimeoutError {}

impl From<TimeoutError> for std::io::Error {
    fn from(_: TimeoutError) -> Self {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "future timed out")
    }
}

/// Await `future`, but give up with [`TimeoutError`] after `dur`.
pub fn timeout<F: Future>(dur: Duration, future: F) -> Timeout<F> {
    Timeout { future, timer: sleep(dur) }
}

/// Future returned by [`timeout`].
pub struct Timeout<F> {
    future: F,
    timer: Timer,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, TimeoutError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: standard pin projection — we never move out of `future`, and
        // `timer` is `Unpin` so it is safe to re-pin by reference.
        let this = unsafe { self.get_unchecked_mut() };
        let fut = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(v) = fut.poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut this.timer).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(TimeoutError)),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ── offload / spawn_blocking ────────────────────────────────────────────────

static BLOCKING_POOL: OnceLock<Arc<ThreadPool>> = OnceLock::new();

fn blocking_pool() -> &'static Arc<ThreadPool> {
    BLOCKING_POOL.get_or_init(|| ThreadPool::new(4))
}

/// Offload a closure onto a **separate parallel pool**, awaitable from any
/// lane.
///
/// This is the primitive behind the "send heavy work out, stay on the main
/// runner" pattern. It is *not* a thread-per-call: the closure is posted to a
/// dedicated parallel [`ThreadPool`] (so several offloads run concurrently),
/// and when it finishes the result is delivered through a waker — which
/// re-schedules the **awaiting** task back onto *its own* lane. So if you
/// `offload(..).await` from a single reactor lane (a [`Runtime`](crate::Runtime)
/// whose task runner is its own `IoTaskRunner`), the CPU work happens on the
/// pool and execution resumes on that lane automatically; you never manually
/// "post the result back".
///
/// This is exactly the Future shape of "`await` == post to another task runner,
/// resume when it replies": the [`Offload`] future *is* that wiring.
pub fn offload<F, T>(f: F) -> Offload<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Offload {
        job: Some(Box::new(f)),
        state: Arc::new(Mutex::new(OffloadState { result: None, waker: None })),
    }
}

/// Run a blocking/CPU-heavy closure off the executor, on a dedicated parallel
/// pool, returning a [`JoinHandle`] (mirrors
/// `async_std::task::spawn_blocking`).
///
/// Equivalent to `spawn(offload(f))`. Prefer [`offload`] directly when you want
/// the work to resume on the *current* lane rather than the spawn pool.
pub fn spawn_blocking<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn(offload(f))
}

struct OffloadState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

/// Future returned by [`offload`]; resolves with the closure's return value
/// once the parallel pool finishes running it.
pub struct Offload<T> {
    job: Option<Box<dyn FnOnce() -> T + Send>>,
    state: Arc<Mutex<OffloadState<T>>>,
}

impl<T: Send + 'static> Future for Offload<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        if let Some(job) = this.job.take() {
            let state = this.state.clone();
            blocking_pool().post_task(
                TaskTraits::default(),
                Box::new(move || {
                    let result = job();
                    let waker = {
                        let mut s = state.lock().unwrap();
                        s.result = Some(result);
                        s.waker.take()
                    };
                    if let Some(w) = waker {
                        w.wake();
                    }
                }),
            );
        }
        let mut s = this.state.lock().unwrap();
        if let Some(result) = s.result.take() {
            return Poll::Ready(result);
        }
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
