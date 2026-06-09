//! The unified runtime: a **(task runner, io task runner) pair**.
//!
//! A [`Runtime`] is the one configurable abstraction this crate offers, and it
//! is deliberately tiny — two fields:
//!
//! - a **task runner** (where a woken task is posted to be polled — any
//!   [`rust_task::TaskRunner`]: parallel, sequenced, or an `IoTaskRunner`), and
//! - a **reactor** (`rust_io::IoTaskRunner`, where `await`ing I/O arms epoll
//!   and from where readiness wakes the task).
//!
//! These two directions are what previously hard-coded the "topology". Pairing
//! them in one value, and carrying that value with the task (see
//! [`crate::local`]), decouples *where a task runs* from *which reactor it
//! arms* — so every topology becomes a choice of these two arguments rather
//! than a separate module:
//!
//! | want…                  | task runner is…                | `reactor`            |
//! |------------------------|--------------------------------|----------------------|
//! | one fused lane         | an `IoTaskRunner` (itself)     | that same runner     |
//! | a parallel pool        | `pool.create_task_runner(..)`  | a shared/own reactor |
//! | an ordered lane        | `pool.create_sequenced_..(..)` | a shared/own reactor |
//! | thread-per-core        | lane *k*                       | lane *k*             |
//!
//! Any [`rust_task::TaskRunner`] works as the executor: a parallel or sequenced
//! runner created from a `ThreadPool`, or an `IoTaskRunner` itself (it
//! implements `TaskRunner`) for a fused lane. You configure traits/monitoring
//! when you *create* the runner; [`Runtime::new`] just wraps its `post_task` to
//! run woken [`Runnable`](async_task::Runnable)s.
//!
//! ```no_run
//! use rust_async::{block_on, Runtime};
//! use rust_io::IoTaskRunner;
//! use rust_task::{TaskTraits, ThreadPool};
//!
//! // A parallel-pool executor paired with its own dedicated reactor.
//! let pool = ThreadPool::new(4);
//! let rt = Runtime::new(pool.create_task_runner(TaskTraits::default()), IoTaskRunner::new());
//!
//! // Drive a root to completion on the calling thread; tasks it spawns inherit `rt`.
//! let n = block_on(rt.spawn(async { 1 + 2 }));
//! assert_eq!(n, 3);
//! ```

use std::future::Future;
use std::sync::{Arc, OnceLock};

use async_task::Runnable;
use rust_io::IoTaskRunner;
use rust_task::{TaskRunner, TaskTraits};

use crate::executor::{JoinHandle, pool};
use crate::local::{current_runtime, tag_with};
use crate::reactor::reactor;

/// A (task runner, io task runner) pair onto which futures are spawned. Cheap
/// to [`Clone`] (just two `Arc`s); clone it to hand the same runtime to several
/// producers.
///
/// ## Scheduling cost
///
/// There is no dedicated run-queue or executor loop: every wake-up posts the
/// task's [`Runnable`](async_task::Runnable) to the task runner as a fresh unit
/// of work (see [`Runtime::new`]). On a parallel pool that is one dispatch per
/// poll, and the task may resume on a different worker each time (no affinity
/// to the waking thread). This keeps the runtime a thin layer over `rust_task`,
/// but is heavier than a purpose-built scheduler's run queue; it is the main
/// place to look first if throughput matters.
#[derive(Clone)]
pub struct Runtime {
    schedule: Arc<dyn Fn(Runnable) + Send + Sync>,
    reactor: Arc<IoTaskRunner>,
}

impl Runtime {
    /// Pair a task `runner` (where woken futures are polled) with a `reactor`
    /// (where `await`ed I/O is armed and woken from).
    ///
    /// `runner` is any [`TaskRunner`]: a parallel or sequenced runner created
    /// from a [`ThreadPool`](rust_task::ThreadPool), or an `IoTaskRunner`
    /// itself for a fused lane. Each woken task's
    /// [`Runnable`](async_task::Runnable) is posted to it.
    pub fn new(runner: Arc<dyn TaskRunner>, reactor: Arc<IoTaskRunner>) -> Runtime {
        let schedule = move |r: Runnable| {
            runner.post_task(Box::new(move || {
                r.run();
            }));
        };
        Runtime { schedule: Arc::new(schedule), reactor }
    }

    /// Spawn `future` onto this runtime. Its wakeups post to this runtime's
    /// task runner, its `await`ed I/O arms this runtime's reactor, and any
    /// nested [`crate::spawn`] inherits this same runtime.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let schedule = self.schedule.clone();
        let (runnable, task) =
            async_task::spawn(tag_with(future, self.clone()), move |r| schedule(r));
        runnable.schedule();
        JoinHandle::from_task(task)
    }

    /// The reactor this runtime arms I/O on.
    pub(crate) fn reactor(&self) -> Arc<IoTaskRunner> {
        self.reactor.clone()
    }
}

/// The process-wide default runtime: the global parallel [`ThreadPool`] paired
/// with the global epoll [`reactor`]. Used as the fallback when a task is
/// spawned (or I/O is armed) outside of any runtime — e.g. a top-level
/// [`crate::block_on`] or a bare [`crate::spawn`].
pub(crate) fn global() -> Runtime {
    static GLOBAL: OnceLock<Runtime> = OnceLock::new();
    GLOBAL
        .get_or_init(|| {
            Runtime::new(pool().create_task_runner(TaskTraits::default()), reactor().io.clone())
        })
        .clone()
}

/// The runtime the current task is bound to, or the [`global`] one if we are
/// outside any task.
pub(crate) fn current_or_global() -> Runtime {
    current_runtime().unwrap_or_else(global)
}
