//! `spawn`: schedule a future onto the current (or global) [`Runtime`].
//!
//! [`JoinHandle`] and the global [`pool`] live here; the actual scheduling is
//! [`Runtime::spawn`](crate::Runtime::spawn). The free [`spawn`] just forwards
//! to whichever runtime the calling task is bound to (inheriting it), or the
//! global one outside any task.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rust_task::ThreadPool;

/// Handle to a spawned task; awaiting it yields the task's output.
///
/// Dropping the handle **detaches** the task (it keeps running to completion),
/// matching `async-std`/Tokio semantics — unlike a bare `async_task::Task`,
/// which cancels on drop.
pub struct JoinHandle<T>(Option<async_task::Task<T>>);

impl<T> JoinHandle<T> {
    /// Wrap an `async_task::Task` (used by the alternative schedulers, e.g.
    /// [`crate::current_thread`]).
    pub(crate) fn from_task(task: async_task::Task<T>) -> Self {
        JoinHandle(Some(task))
    }

    /// Detach the task: it keeps running to completion independently of this
    /// handle (the same effect as dropping the handle, but explicit).
    pub fn detach(self) {
        // `Drop` calls `task.detach()`.
    }
}

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

/// The global default executor thread pool, backing the [`global`
/// runtime`](crate::runtime::global).
pub(crate) fn pool() -> &'static Arc<ThreadPool> {
    POOL.get_or_init(|| ThreadPool::new(4))
}

/// Spawn `future` onto the runtime the calling task is bound to (inheriting
/// it), or the global runtime if called outside any task.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    crate::runtime::current_or_global().spawn(future)
}
