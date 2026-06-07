//! A thread-per-core runtime: N independent single-thread lanes.
//!
//! [`crate::current_thread`] is one sequenced reactor+executor lane. A
//! [`Runtime`] is simply `N` of them — each an independent `IoTaskRunner` with
//! its own epoll, its own task queue, and its own [`TaskMonitor`]. You shard
//! work across the lanes; within a lane everything is strictly ordered and
//! never concurrent (so no locks needed for per-lane state), while different
//! lanes run truly in parallel on different cores.
//!
//! This is the nginx / glommio / Tokio-`current_thread`-per-core architecture,
//! and it falls out for free here because [`IoTaskRunner`] is already a
//! self-contained single-threaded runtime, and the I/O futures arm on
//! **whichever lane they run on** (via [`crate::reactor::io_runner`], which
//! consults `IoTaskRunner::current()`). So a connection spawned onto lane `k`
//! has its socket watched and woken on lane `k` — no cross-lane hand-off.
//!
//! ```no_run
//! use std::sync::Arc;
//! use rust_async::runtime::Runtime;
//!
//! let rt = Runtime::new(4);
//! let rt2 = Arc::clone(&rt);
//! rt.run(async move {
//!     // round-robin work across the 4 lanes
//!     let handles: Vec<_> = (0..8).map(|i| rt2.spawn(async move { i * i })).collect();
//!     let mut sum = 0;
//!     for h in handles { sum += h.await; }
//!     assert_eq!(sum, 140);
//! });
//! ```

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

use async_task::Runnable;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;

use crate::executor::JoinHandle;
use crate::local::tag;

/// A pool of `N` single-thread lanes (each its own reactor + executor).
pub struct Runtime {
    lanes: Vec<Arc<IoTaskRunner>>,
    next: AtomicUsize,
}

impl Runtime {
    /// Create a runtime with `threads` lanes (one IO thread each). Panics if
    /// `threads == 0`.
    pub fn new(threads: usize) -> Arc<Runtime> {
        assert!(threads >= 1, "Runtime needs at least one lane");
        let lanes = (0..threads).map(|_| IoTaskRunner::new()).collect();
        Arc::new(Runtime { lanes, next: AtomicUsize::new(0) })
    }

    /// Number of lanes.
    pub fn lanes(&self) -> usize {
        self.lanes.len()
    }

    /// "Run this task on lane `lane`" == "post it onto that lane's IO thread".
    fn schedule_for(&self, lane: usize) -> impl Fn(Runnable) + Send + Sync + 'static {
        let runner = Arc::clone(&self.lanes[lane]);
        move |runnable| {
            runner.post_task(Box::new(move || {
                runnable.run();
            }));
        }
    }

    /// Spawn `future` onto a specific lane.
    pub fn spawn_on<F>(&self, lane: usize, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (runnable, task) = async_task::spawn(tag(future), self.schedule_for(lane));
        runnable.schedule();
        JoinHandle::from_task(task)
    }

    /// Spawn `future` onto the next lane, round-robin.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let lane = self.next.fetch_add(1, Ordering::Relaxed) % self.lanes.len();
        self.spawn_on(lane, future)
    }

    /// Drive `future` to completion on lane 0, blocking the calling thread
    /// until it resolves. For a server, `future` is typically an accept
    /// loop that never returns, so this blocks for the lifetime of the
    /// process.
    pub fn run<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (tx, rx) = mpsc::sync_channel::<F::Output>(1);
        let wrapped = async move {
            let _ = tx.send(future.await);
        };
        // Detach: completion is observed through the channel, not the handle.
        self.spawn_on(0, wrapped).detach();
        rx.recv().expect("Runtime::run: root task dropped without completing")
    }
}
