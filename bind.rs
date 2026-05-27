use std::sync::{Arc, Weak};

/// Abstracts over `Arc<T>` and `Weak<T>` so `bind_once` can accept either.
///
/// - `Arc<T>`  → always resolves to `Some(arc)`
/// - `Weak<T>` → resolves to `Some(arc)` if the object is still alive, `None` otherwise
pub trait IntoArc<T>: Send + 'static {
    fn into_arc(self) -> Option<Arc<T>>;
}

impl<T: Send + Sync + 'static> IntoArc<T> for Arc<T> {
    fn into_arc(self) -> Option<Arc<T>> {
        Some(self)
    }
}

impl<T: Send + Sync + 'static> IntoArc<T> for Weak<T> {
    fn into_arc(self) -> Option<Arc<T>> {
        self.upgrade()
    }
}

/// Binds a pointer and a callback into a `Box<dyn FnOnce() + Send>` task.
///
/// Accepts both `Arc<T>` (always runs) and `Weak<T>` (skips if the object
/// has been dropped).  Using `Weak<T>` ensures the task runner does not
/// extend the object's lifetime: once all strong references are gone the
/// object is freed immediately, and the pending task becomes a no-op.
///
/// # Examples
///
/// ```ignore
/// // Arc: task always executes
/// pool.post_task(traits, bind_once(Arc::clone(&handler), |h| h.on_event()));
///
/// // Weak: task is skipped if handler is dropped before it runs
/// pool.post_task(traits, bind_once(Arc::downgrade(&handler), |h| h.on_event()));
///
/// // Extra arguments are captured in the closure as usual
/// let msg = "hello".to_string();
/// pool.post_task(traits, bind_once(Arc::downgrade(&handler), move |h| h.on_message(msg)));
/// ```
pub fn bind_once<P, T, F>(ptr: P, f: F) -> Box<dyn FnOnce() + Send + 'static>
where
    P: IntoArc<T>,
    T: Send + Sync + 'static,
    F: FnOnce(Arc<T>) + Send + 'static,
{
    Box::new(move || {
        if let Some(arc) = ptr.into_arc() {
            f(arc);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Counter {
        count: Mutex<usize>,
    }

    impl Counter {
        fn new() -> Arc<Self> {
            Arc::new(Self { count: Mutex::new(0) })
        }
        fn increment(&self) {
            *self.count.lock().unwrap() += 1;
        }
        fn get(&self) -> usize {
            *self.count.lock().unwrap()
        }
    }

    #[test]
    fn arc_always_runs() {
        let counter = Counter::new();
        let task = bind_once(Arc::clone(&counter), |c| c.increment());
        task();
        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn weak_runs_while_alive() {
        let counter = Counter::new();
        let task = bind_once(Arc::downgrade(&counter), |c| c.increment());
        task();
        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn weak_skips_after_drop() {
        let counter = Counter::new();
        let task = bind_once(Arc::downgrade(&counter), |c| c.increment());
        drop(counter); // object freed here
        task(); // should be a no-op
                // counter is gone; just verify task() didn't panic
    }

    #[test]
    fn weak_does_not_extend_lifetime() {
        let counter = Counter::new();
        // Weak does not increment the refcount.
        let _task = bind_once(Arc::downgrade(&counter), |c| c.increment());
        // Only one strong ref exists; drop should free the object immediately.
        let weak = Arc::downgrade(&counter);
        drop(counter);
        assert!(weak.upgrade().is_none(), "object should be freed");
    }
}
