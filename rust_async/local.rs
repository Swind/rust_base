//! Task-local storage, mirroring `async_std::task_local!` / `task::LocalKey`.
//!
//! Each spawned (or `block_on`'d) future is wrapped in a [`Tagged`] future
//! that, on every poll, installs its own [`TaskLocals`] as the current one (a
//! thread-local pointer) and restores the previous on the way out. This is the
//! same trick async-std's `SupportTaskLocals` uses: the "current task" must be
//! known during a poll so [`LocalKey::with`] can find the right storage even as
//! the task migrates between pool workers.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Per-task storage: a type/key-addressed map of values.
pub(crate) struct TaskLocals {
    map: Mutex<HashMap<usize, Box<dyn Any + Send>>>,
}

impl TaskLocals {
    fn new() -> Arc<Self> {
        Arc::new(Self { map: Mutex::new(HashMap::new()) })
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<TaskLocals>>> = const { RefCell::new(None) };
}

/// Installs `locals` as current for its lifetime, restoring the previous on
/// drop.
struct CurrentGuard(Option<Arc<TaskLocals>>);

impl CurrentGuard {
    fn set(locals: Arc<TaskLocals>) -> Self {
        let prev = CURRENT.with(|c| c.borrow_mut().replace(locals));
        CurrentGuard(prev)
    }
}

impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.0.take());
    }
}

/// A future carrying its own task-local storage.
pub(crate) struct Tagged<F> {
    locals: Arc<TaskLocals>,
    future: F,
}

/// Wrap a future so it carries fresh task-local storage.
pub(crate) fn tag<F>(future: F) -> Tagged<F> {
    Tagged { locals: TaskLocals::new(), future }
}

impl<F: Future> Future for Tagged<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        // SAFETY: standard pin projection; we never move out of `future`.
        let this = unsafe { self.get_unchecked_mut() };
        let _guard = CurrentGuard::set(this.locals.clone());
        let fut = unsafe { Pin::new_unchecked(&mut this.future) };
        fut.poll(cx)
    }
}

/// A key into task-local storage, created by the [`task_local!`] macro.
pub struct LocalKey<T: Send + 'static> {
    #[doc(hidden)]
    pub init: fn() -> T,
}

impl<T: Send + 'static> LocalKey<T> {
    /// Access this task's value for the key, initializing it on first use.
    ///
    /// Panics if called outside of a `rust_async` task (i.e. not within a
    /// `spawn`/`block_on` future).
    pub fn with<R>(&'static self, f: impl FnOnce(&T) -> R) -> R {
        let locals = CURRENT
            .with(|c| c.borrow().clone())
            .expect("task-local accessed outside of a rust_async task");
        let key = self as *const Self as usize;
        let mut map = locals.map.lock().unwrap();
        let entry = map.entry(key).or_insert_with(|| Box::new((self.init)()));
        let val = entry.downcast_ref::<T>().expect("task-local type mismatch");
        f(val)
    }
}

/// Declare a task-local value, à la `async_std::task_local!`.
///
/// ```
/// use std::cell::Cell;
/// rust_async::task_local! {
///     static COUNTER: Cell<u32> = Cell::new(0);
/// }
/// ```
#[macro_export]
macro_rules! task_local {
    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty = $init:expr;) => {
        $(#[$attr])* $vis static $name: $crate::LocalKey<$t> =
            $crate::LocalKey { init: || $init };
    };
}
