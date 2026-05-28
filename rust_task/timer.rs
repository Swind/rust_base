use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::bind::bind_once;
use crate::sequenced_task_runner::SequencedTaskRunner;

// Private state shared between RepeatingTimer and each pending delayed task.
// Dropping the Arc (via stop() or RepeatingTimer drop) invalidates every
// Weak<TimerInner> held by pending tasks, making them no-ops.
struct TimerInner {
    interval: Duration,
    callback: Arc<dyn Fn() + Send + Sync + 'static>,
    runner: Arc<dyn SequencedTaskRunner>,
}

/// Repeatedly fires a callback at a fixed interval on a `SequencedTaskRunner`.
///
/// The cancellation mechanism reuses the `Weak<T>` pattern from `bind_once`:
/// every pending delayed task holds a `Weak<TimerInner>`.  Calling `stop()`
/// (or dropping the `RepeatingTimer`) sets `active` to `None`, which drops
/// the `Arc<TimerInner>` and immediately invalidates all outstanding weaks —
/// no generation counter needed.
///
/// Because the runner is sequenced, consecutive firings are never concurrent:
/// each task schedules the next firing *before* invoking the callback, matching
/// Chromium's `RepeatingTimer::RunUserTask()` pattern.
///
/// # Example
///
/// ```ignore
/// let runner = pool.create_sequenced_task_runner(TaskTraits::default());
/// let timer = RepeatingTimer::new(runner);
/// timer.start(Duration::from_secs(1), || println!("tick"));
/// // later…
/// timer.stop();
/// ```
pub struct RepeatingTimer {
    runner: Arc<dyn SequencedTaskRunner>,
    active: Mutex<Option<Arc<TimerInner>>>,
}

impl RepeatingTimer {
    pub fn new(runner: Arc<dyn SequencedTaskRunner>) -> Self {
        Self { runner, active: Mutex::new(None) }
    }

    /// Start (or restart) the timer. The first firing occurs after `interval`.
    /// Calling `start()` while already running replaces the previous timer.
    pub fn start(&self, interval: Duration, callback: impl Fn() + Send + Sync + 'static) {
        let inner = Arc::new(TimerInner {
            interval,
            callback: Arc::new(callback),
            runner: Arc::clone(&self.runner),
        });
        *self.active.lock().unwrap() = Some(Arc::clone(&inner));
        schedule_next(Arc::downgrade(&inner));
    }

    /// Stop the timer. Any pending delayed tasks become no-ops when they fire.
    pub fn stop(&self) {
        *self.active.lock().unwrap() = None;
    }

    pub fn is_running(&self) -> bool {
        self.active.lock().unwrap().is_some()
    }
}

// Post the next delayed tick.  Called once from start() and once from inside
// each firing task (before the callback runs, mirroring Chromium's pattern).
fn schedule_next(weak: Weak<TimerInner>) {
    // Upgrade only to read interval + runner; drop the Arc before posting so
    // the task's Weak is the only reference added.
    let Some(inner) = weak.upgrade() else { return };
    let interval = inner.interval;
    let runner = Arc::clone(&inner.runner);
    drop(inner);

    runner.post_delayed_task(
        bind_once(weak, |inner| {
            let cb = Arc::clone(&inner.callback);
            // Schedule next *before* calling the callback (same as Chromium's
            // RepeatingTimer::RunUserTask). This way, even if the callback calls
            // stop(), the next task is already queued with a Weak that will
            // fail to upgrade once stop() drops the Arc.
            schedule_next(Arc::downgrade(&inner));
            drop(inner); // release strong ref before invoking cb
            cb();
        }),
        interval,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bind::bind_repeating;
    use crate::task_traits::TaskTraits;
    use crate::thread_pool::thread_pool::ThreadPool;
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    fn make_timer() -> (Arc<ThreadPool>, RepeatingTimer) {
        let pool = ThreadPool::new(2);
        let runner = pool.create_sequenced_task_runner(TaskTraits::default());
        let timer = RepeatingTimer::new(runner);
        (pool, timer)
    }

    #[test]
    fn fires_multiple_times() {
        let (pool, timer) = make_timer();

        let count = Arc::new(Mutex::new(0usize));
        let barrier = Arc::new(Barrier::new(2));

        let c = Arc::clone(&count);
        let b = Arc::clone(&barrier);
        timer.start(Duration::from_millis(20), move || {
            let mut g = c.lock().unwrap();
            *g += 1;
            if *g == 3 {
                b.wait();
            }
        });

        barrier.wait();
        timer.stop();
        pool.shutdown();

        assert!(*count.lock().unwrap() >= 3);
    }

    #[test]
    fn stop_before_first_firing_is_noop() {
        let (pool, timer) = make_timer();

        let count = Arc::new(Mutex::new(0usize));
        let c = Arc::clone(&count);

        // Large interval so stop() definitely wins the race.
        timer.start(Duration::from_millis(500), move || {
            *c.lock().unwrap() += 1;
        });
        timer.stop();

        // Give the delayed task time to fire (it should be a no-op).
        std::thread::sleep(Duration::from_millis(600));
        pool.shutdown();

        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[test]
    fn restart_replaces_previous_timer() {
        let (pool, timer) = make_timer();

        // First run: wait for 2 firings.
        let count1 = Arc::new(Mutex::new(0usize));
        let b1 = Arc::new(Barrier::new(2));
        {
            let c = Arc::clone(&count1);
            let b = Arc::clone(&b1);
            timer.start(Duration::from_millis(20), move || {
                let mut g = c.lock().unwrap();
                *g += 1;
                if *g == 2 {
                    b.wait();
                }
            });
        }
        b1.wait();
        timer.stop();
        assert!(!timer.is_running());

        // Second run with a fresh callback.
        let count2 = Arc::new(Mutex::new(0usize));
        let b2 = Arc::new(Barrier::new(2));
        {
            let c = Arc::clone(&count2);
            let b = Arc::clone(&b2);
            timer.start(Duration::from_millis(20), move || {
                let mut g = c.lock().unwrap();
                *g += 1;
                if *g == 2 {
                    b.wait();
                }
            });
        }
        b2.wait();
        timer.stop();
        pool.shutdown();

        assert!(*count2.lock().unwrap() >= 2);
    }

    #[test]
    fn dropping_object_stops_timer_via_bind_repeating() {
        let (pool, timer) = make_timer();

        struct Handler {
            count: Mutex<usize>,
        }
        let handler = Arc::new(Handler { count: Mutex::new(0) });

        // Use bind_repeating so the closure holds Weak<Handler>, not Arc.
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let cb = bind_repeating(Arc::downgrade(&handler), move |h| {
            *h.count.lock().unwrap() += 1;
            b.wait(); // sync with test thread on first firing
        });

        // bind_repeating returns Arc<dyn Fn()>; wrap in a closure so it
        // satisfies impl Fn() (Arc<dyn Fn()> doesn't auto-impl Fn() in stable).
        timer.start(Duration::from_millis(20), move || cb());

        // Wait until the first firing has started.
        barrier.wait();

        // Drop handler — its strong count falls to 0 once the callback returns.
        let weak = Arc::downgrade(&handler);
        drop(handler);

        // After the callback releases its Arc, the object is freed.
        std::thread::sleep(Duration::from_millis(100));
        assert!(weak.upgrade().is_none(), "handler should be freed immediately");

        timer.stop();
        pool.shutdown();
    }
}
