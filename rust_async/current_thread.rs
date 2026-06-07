//! A single-threaded executor fused with the reactor.
//!
//! Where [`crate::spawn`] schedules tasks onto a parallel [`ThreadPool`] (so a
//! task can be polled on any of N worker threads, with a cross-thread hop from
//! the reactor when an fd wakes), this module schedules every task **onto the
//! reactor's own IO thread** — `rust_io::IoTaskRunner`, which is itself a
//! `SequencedTaskRunner` that interleaves posted tasks with epoll readiness on
//! one thread (Chromium's `SingleThreadTaskRunner` + `MessagePumpForIO`).
//!
//! The consequences, all of which match the "async/await as sugar over a task
//! runner" model:
//!
//! - **Strict ordering.** Exactly one future is polled at a time, FIFO.
//! - **No cross-thread hand-off.** When epoll reports readiness, we are already
//!   on the thread that will re-poll the woken task; `wake()` just re-posts
//!   onto the same queue. No parking, no thread hop.
//! - **Free monitoring.** `IoTaskRunner` already brackets its work with
//!   `TaskMonitor` (hang detection), so tasks are observable out of the box.
//!
//! The cost is that the executor and the reactor share one thread: a blocking
//! or CPU-heavy poll stalls **every** connection *and* epoll itself. Offload
//! such work with [`crate::offload`] (it runs on a separate parallel pool and
//! resumes the awaiting task back here automatically).
//!
//! ```no_run
//! use rust_async::current_thread;
//!
//! let total = current_thread::run(async {
//!     let a = current_thread::spawn(async { 1 + 2 });
//!     let b = current_thread::spawn(async { 3 + 4 });
//!     a.await + b.await
//! });
//! assert_eq!(total, 10);
//! ```

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;

use async_task::Runnable;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;

use crate::executor::JoinHandle;
use crate::local::tag;
use crate::reactor::io_runner;

/// "Run this task" == "post it onto the current reactor lane". This is the
/// whole difference from the pool-based executor: same machinery, different
/// lane. Resolving via [`io_runner`] means a task re-schedules onto whichever
/// lane it is running on (the global reactor, or a [`run_here`] /
/// [`crate::runtime`] lane), keeping a task pinned to one lane.
fn schedule(runnable: Runnable) {
    io_runner().post_task(Box::new(move || {
        runnable.run();
    }));
}

/// Spawn `future` onto the reactor's single IO thread (the current-thread
/// lane).
///
/// Awaiting the returned [`JoinHandle`] yields the output; dropping it detaches
/// the task (it keeps running), as with [`crate::spawn`].
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(tag(future), schedule);
    runnable.schedule();
    JoinHandle::from_task(task)
}

/// Drive `future` to completion on the reactor lane, blocking the calling
/// thread until it resolves.
///
/// This is the [`crate::block_on`] of this executor flavor, but instead of
/// parking the caller and driving the root *on the caller's thread*, it posts
/// the root onto the IO thread (where all other tasks and epoll also live) and
/// the caller simply waits for the result. The IO thread does the work; the
/// caller is just a rendezvous point.
pub fn run<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel::<F::Output>(1);
    let wrapped = async move {
        let _ = tx.send(future.await);
    };
    let (runnable, task) = async_task::spawn(tag(wrapped), schedule);
    runnable.schedule();
    // Let the task own its own lifetime on the IO lane; we observe completion
    // through the channel rather than by polling the handle.
    task.detach();
    rx.recv().expect("current_thread::run: root task dropped without completing")
}

/// Like [`run`], but the **calling thread itself becomes the reactor lane**
/// instead of parking while a separate IO thread does the work.
///
/// This builds a dedicated [`IoTaskRunner::without_thread`] and drives its pump
/// loop on the caller via [`IoTaskRunner::run_on_current_thread`]. The root,
/// all tasks spawned with [`spawn`], and the epoll loop all run on this one
/// thread; when the root completes it quits the loop and the call returns. A
/// pure `run_here` program uses exactly **one** thread (plus the
/// [`crate::offload`] pool only if you offload), never touching the global
/// reactor singleton.
pub fn run_here<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runner = IoTaskRunner::without_thread();
    let (tx, rx) = mpsc::sync_channel::<F::Output>(1);

    let stop = Arc::clone(&runner);
    let wrapped = async move {
        let out = future.await;
        let _ = tx.send(out);
        // Break the pump loop so `run_on_current_thread` (below) returns.
        stop.shutdown();
    };

    // The root must be posted onto *this* runner explicitly: at this point the
    // caller is not yet pumping, so `io_runner()` would resolve to the global
    // reactor. Once we are pumping, nested `spawn`s resolve to this lane.
    let sched = Arc::clone(&runner);
    let (runnable, task) = async_task::spawn(tag(wrapped), move |r: Runnable| {
        sched.post_task(Box::new(move || {
            r.run();
        }));
    });
    runnable.schedule();
    task.detach();

    runner.run_on_current_thread();
    rx.recv().expect("current_thread::run_here: root task dropped without completing")
}
