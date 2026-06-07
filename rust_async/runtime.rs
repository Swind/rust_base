//! The unified runtime: a **(task runner, io task runner) pair**.
//!
//! A [`Runtime`] is the one configurable abstraction this crate offers, and it
//! is deliberately tiny — two fields:
//!
//! - a **schedule** function (where a task's `Runnable` is posted when it is
//!   woken — i.e. *which task runner* polls the futures), and
//! - a **reactor** (`rust_io::IoTaskRunner`, where `await`ing I/O arms epoll
//!   and from where readiness wakes the task).
//!
//! These two directions are what previously hard-coded the "topology". Pairing
//! them in one value, and carrying that value with the task (see
//! [`crate::local`]), decouples *where a task runs* from *which reactor it
//! arms* — so every topology becomes a choice of these two arguments rather
//! than a separate module:
//!
//! | want…                  | `schedule` posts to            | `reactor`            |
//! |------------------------|--------------------------------|----------------------|
//! | one fused lane         | an `IoTaskRunner` (itself)     | that same runner     |
//! | a parallel pool        | a `ThreadPool`                 | a shared/own reactor |
//! | an ordered lane        | a `SequencedTaskRunner`        | a shared/own reactor |
//! | thread-per-core        | lane *k*                       | lane *k*             |
//!
//! The `schedule` closure *is* the adapter: `TaskRunner::post_task` and
//! `SequencedTaskRunner::post_task` have different signatures, but both
//! collapse to `Fn(Runnable)`. Build whichever runner you want (configuring its
//! monitoring/traits at creation) and wrap its `post_task` in the closure.
//!
//! ```no_run
//! use std::sync::Arc;
//! use rust_async::{block_on, Runnable, Runtime};
//! use rust_io::IoTaskRunner;
//! use rust_task::{TaskTraits, ThreadPool};
//!
//! // A parallel-pool executor paired with its own dedicated reactor.
//! let pool = ThreadPool::new(4);
//! let reactor = IoTaskRunner::new();
//! let rt = Runtime::new(
//!     move |r: Runnable| {
//!         pool.post_task(TaskTraits::default(), Box::new(move || { r.run(); }));
//!     },
//!     reactor,
//! );
//!
//! // Drive a root to completion on the calling thread; tasks it spawns inherit `rt`.
//! let n = block_on(rt.spawn(async { 1 + 2 }));
//! assert_eq!(n, 3);
//! ```

use std::future::Future;
use std::sync::{Arc, OnceLock};

use async_task::Runnable;
use rust_io::IoTaskRunner;
use rust_task::TaskTraits;

use crate::executor::{JoinHandle, pool};
use crate::local::{current_runtime, tag_with};
use crate::reactor::reactor;

/// A (task runner, io task runner) pair onto which futures are spawned. Cheap
/// to [`Clone`] (just two `Arc`s); clone it to hand the same runtime to several
/// producers.
#[derive(Clone)]
pub struct Runtime {
    schedule: Arc<dyn Fn(Runnable) + Send + Sync>,
    reactor: Arc<IoTaskRunner>,
}

impl Runtime {
    /// Pair a `schedule` function (where woken tasks are posted) with a
    /// `reactor` (where `await`ed I/O is armed and woken from).
    ///
    /// `schedule` receives the task's [`Runnable`]; run it on whatever task
    /// runner you want by wrapping its `post_task`, e.g.
    /// `move |r| my_runner.post_task(Box::new(move || r.run()))`.
    pub fn new(
        schedule: impl Fn(Runnable) + Send + Sync + 'static,
        reactor: Arc<IoTaskRunner>,
    ) -> Runtime {
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
            Runtime::new(
                |r: Runnable| {
                    pool().post_task(
                        TaskTraits::default(),
                        Box::new(move || {
                            r.run();
                        }),
                    );
                },
                reactor().io.clone(),
            )
        })
        .clone()
}

/// The runtime the current task is bound to, or the [`global`] one if we are
/// outside any task.
pub(crate) fn current_or_global() -> Runtime {
    current_runtime().unwrap_or_else(global)
}
