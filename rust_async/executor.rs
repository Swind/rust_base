//! `spawn`: schedule a future onto a `rust_task::ThreadPool`.
//!
//! This is the whole "executor" of the spike, and it is tiny — because
//! [`async_task`] already provides the `Runnable`/`Task` machinery, and
//! `rust_task` already provides the thread pool. The only glue is the
//! **schedule function**: "re-run this task" is literally "post a closure to
//! the pool". That is the exact same substitution async-global-executor makes;
//! here the queue happens to be a `rust_task` `TaskRunner`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use async_task::Runnable;
use rust_task::{TaskTraits, ThreadPool};

use crate::local::tag;

/// Handle to a spawned task; awaiting it yields the task's output.
///
/// Dropping the handle **detaches** the task (it keeps running to completion),
/// matching `async-std`/Tokio semantics — unlike a bare `async_task::Task`,
/// which cancels on drop.
pub struct JoinHandle<T>(Option<async_task::Task<T>>);

impl<T> Future for JoinHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        let task = this.0.as_mut().expect("JoinHandle polled after completion");
        Pin::new(task).poll(cx)
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.detach();
        }
    }
}

static POOL: OnceLock<Arc<ThreadPool>> = OnceLock::new();

fn pool() -> &'static Arc<ThreadPool> {
    POOL.get_or_init(|| ThreadPool::new(4))
}

/// Spawn `future` onto the global thread pool.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let schedule = move |runnable: Runnable| {
        // Waker fired -> put the task back on the pool. A `Runnable` *is* a
        // `Box<dyn FnOnce>` in spirit, so the pool never knows it's a future.
        pool().post_task(
            TaskTraits::default(),
            Box::new(move || {
                runnable.run();
            }),
        );
    };
    // Wrap so the task carries its own task-local storage.
    let (runnable, task) = async_task::spawn(tag(future), schedule);
    runnable.schedule();
    JoinHandle(Some(task))
}
