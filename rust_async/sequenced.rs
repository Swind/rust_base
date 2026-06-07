//! Sequenced executor: drive async tasks on `SequencedTaskRunner`s over a
//! shared pool — Chromium's "sequences on a pool" model applied to
//! `async`/`.await`.
//!
//! This is the separated topology you usually want for ordered async code:
//!
//! - The **reactor** (the global IO thread) only *wakes* — it never polls a
//!   future. `wake()` posts the task's runnable onto its [`Sequence`].
//! - The **executor** is the shared [`crate::spawn`] thread pool. A worker
//!   pulls the runnable and polls the future.
//! - A [`Sequence`] guarantees its tasks run **strictly in order and never
//!   concurrently** (so they can share state without locks), while
//!   **different** sequences run **in parallel** on the pool's workers. A
//!   multi-thread pool is not a contradiction — it is exactly how many
//!   sequences get parallelism.
//!
//! Use one [`Sequence`] per actor / per connection / per unit of state you want
//! serialized:
//!
//! ```no_run
//! use rust_async::{block_on, sequenced::Sequence};
//!
//! block_on(async {
//!     let seq = Sequence::new();
//!     // These two run in order, never overlapping, sharing `seq`'s lane:
//!     let a = seq.spawn(async { 1 });
//!     let b = seq.spawn(async { 2 });
//!     assert_eq!(a.await + b.await, 3);
//! });
//! ```

use std::future::Future;
use std::sync::Arc;

use async_task::Runnable;
use rust_task::{SequencedTaskRunner, TaskTraits};

use crate::executor::{JoinHandle, pool};
use crate::local::tag;

/// A handle to one ordered lane on the shared executor pool. Tasks spawned onto
/// the same `Sequence` execute FIFO and never concurrently; clone it to hand
/// the same lane to several producers.
#[derive(Clone)]
pub struct Sequence {
    runner: Arc<dyn SequencedTaskRunner>,
}

impl Sequence {
    /// Create a new ordered lane backed by the shared executor pool.
    pub fn new() -> Sequence {
        Sequence { runner: pool().create_sequenced_task_runner(TaskTraits::default()) }
    }

    /// Spawn `future` onto this lane. Its wakeups are scheduled onto this
    /// sequence, so it is polled in order with — and never concurrently with —
    /// every other task on the same `Sequence`.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runner = Arc::clone(&self.runner);
        let schedule = move |runnable: Runnable| {
            runner.post_task(Box::new(move || {
                runnable.run();
            }));
        };
        let (runnable, task) = async_task::spawn(tag(future), schedule);
        runnable.schedule();
        JoinHandle::from_task(task)
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Self::new()
    }
}
