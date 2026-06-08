use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::sequenced_task_runner::SequencedTaskRunner;
use crate::task::Task;
use crate::task_monitor::TaskMonitor;
use crate::task_runner::TaskRunner;
use crate::task_traits::TaskTraits;
use crate::thread_pool::delayed_task_manager::DelayedTaskManager;
use crate::thread_pool::pooled_parallel_task_runner::PooledParallelTaskRunner;
use crate::thread_pool::pooled_sequenced_task_runner::PooledSequencedTaskRunner;
use crate::thread_pool::sequence::Sequence;
use crate::thread_pool::task_tracker::TaskTracker;
use crate::thread_pool::thread_group::ThreadGroup;

pub struct ThreadPool {
    task_tracker: Arc<TaskTracker>,
    delayed_task_manager: Arc<DelayedTaskManager>,
    thread_group: Arc<ThreadGroup>,
    monitor: Option<Arc<TaskMonitor>>,
}

impl ThreadPool {
    pub fn new(num_threads: usize) -> Arc<Self> {
        Self::new_impl(num_threads, None)
    }

    /// Create a thread pool with task monitoring enabled.
    ///
    /// All tasks posted via `post_task`, `create_sequenced_task_runner`, and
    /// `create_task_runner` will have their queue time and execution time
    /// measured and reported to the monitor's `on_metrics` callback.  Workers
    /// are registered with the monitor for hang detection.
    pub fn new_with_monitor(num_threads: usize, monitor: Arc<TaskMonitor>) -> Arc<Self> {
        Self::new_impl(num_threads, Some(monitor))
    }

    fn new_impl(num_threads: usize, monitor: Option<Arc<TaskMonitor>>) -> Arc<Self> {
        let thread_group = ThreadGroup::new(num_threads, monitor.as_ref().map(Arc::clone));
        let delayed_task_manager = DelayedTaskManager::new(Arc::clone(&thread_group));
        Arc::new(Self {
            task_tracker: Arc::new(TaskTracker::new()),
            delayed_task_manager,
            thread_group,
            monitor,
        })
    }

    // Posts a one-shot parallel task (no ordering guarantees).
    pub fn post_task(
        &self,
        traits: TaskTraits,
        callback: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool {
        if !self.task_tracker.will_post_task(&traits) {
            return false;
        }
        let seq = Arc::new(Sequence::new(traits));
        seq.push_task(Task::new(self.wrap(traits, callback)));
        self.thread_group.push_task_source(seq);
        true
    }

    // Posts a one-shot parallel delayed task.
    pub fn post_delayed_task(
        &self,
        traits: TaskTraits,
        callback: Box<dyn FnOnce() + Send + 'static>,
        delay: Duration,
    ) -> bool {
        if !self.task_tracker.will_post_task(&traits) {
            return false;
        }
        let ready_time = Instant::now() + delay;
        let seq = Arc::new(Sequence::new(traits));
        seq.push_delayed_task(Task::new(self.wrap(traits, callback)), ready_time);
        self.delayed_task_manager.add_sequence(ready_time, seq);
        true
    }

    // Creates a sequenced task runner backed by this pool.
    // All tasks posted to the returned runner execute in FIFO order.
    pub fn create_sequenced_task_runner(&self, traits: TaskTraits) -> Arc<dyn SequencedTaskRunner> {
        PooledSequencedTaskRunner::new(
            traits,
            Arc::clone(&self.thread_group),
            Arc::clone(&self.delayed_task_manager),
            self.monitor.as_ref().map(Arc::clone),
        )
    }

    // Creates a parallel task runner backed by this pool.
    // Tasks posted to the returned runner may execute concurrently.
    pub fn create_task_runner(&self, traits: TaskTraits) -> Arc<dyn TaskRunner> {
        Arc::new(PooledParallelTaskRunner::new(
            traits,
            Arc::clone(&self.thread_group),
            Arc::clone(&self.delayed_task_manager),
            self.monitor.as_ref().map(Arc::clone),
        ))
    }

    /// Turn the **calling thread** into a worker of this pool, blocking until
    /// `shutdown()` is signalled. The thread competes for posted tasks exactly
    /// like a built-in worker, and is covered by the monitor if one is
    /// configured.
    ///
    /// Unlike the workers created by [`new`](Self::new), this thread is **not**
    /// tracked by the pool: `shutdown()` will not join it. The loop exits
    /// cleanly once shutdown is signalled (`get_work()` returns `None`); the
    /// caller owns the thread and is responsible for joining it.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::thread;
    /// use rust_task::ThreadPool;
    ///
    /// let pool = ThreadPool::new(2);
    /// let p = Arc::clone(&pool);
    /// let extra = thread::spawn(move || p.attach_current_thread());
    ///
    /// // ... post work to the pool ...
    ///
    /// pool.shutdown();        // signals the attached worker to exit its loop
    /// extra.join().unwrap();  // the caller joins its own thread
    /// ```
    pub fn attach_current_thread(&self) {
        self.thread_group.run_worker_on_current_thread(self.monitor.as_ref().map(Arc::clone));
    }

    // Signals shutdown and waits for BlockShutdown tasks to complete,
    // then stops all workers and the timer thread.
    pub fn shutdown(&self) {
        self.task_tracker.shutdown();
        self.thread_group.join_all();
        self.delayed_task_manager.shutdown();
    }

    // Wraps the callback to enforce shutdown behavior at execution time.
    // If a monitor is configured, also wraps with timing (queue + execution).
    fn wrap(
        &self,
        traits: TaskTraits,
        callback: Box<dyn FnOnce() + Send + 'static>,
    ) -> Box<dyn FnOnce() + Send + 'static> {
        // Timing wrapper is innermost so queue_time is post→execution-start,
        // and execution_time only covers the original callback (not shutdown
        // bookkeeping). SkipOnShutdown tasks that are dropped never call the
        // timing wrapper, so on_metrics is not invoked for skipped tasks.
        let callback = match self.monitor.as_ref() {
            Some(m) => m.wrap_task(callback),
            None => callback,
        };
        let tracker = Arc::clone(&self.task_tracker);
        Box::new(move || {
            if tracker.is_shutdown_started()
                && traits.shutdown_behavior
                    == crate::task_traits::TaskShutdownBehavior::SkipOnShutdown
            {
                return;
            }
            callback();
            tracker.after_run_task(&traits);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_traits::{TaskPriority, TaskShutdownBehavior, TaskTraits, ThreadPolicy};
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    fn default_traits() -> TaskTraits {
        TaskTraits::default()
    }

    fn traits_with(behavior: TaskShutdownBehavior) -> TaskTraits {
        TaskTraits {
            priority: TaskPriority::UserVisible,
            shutdown_behavior: behavior,
            thread_policy: ThreadPolicy::PreferBackground,
            may_block: false,
        }
    }

    #[test]
    fn post_task_executes() {
        let pool = ThreadPool::new(2);
        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        pool.post_task(
            default_traits(),
            Box::new(move || {
                *e.lock().unwrap() = true;
                b.wait();
            }),
        );

        barrier.wait();
        pool.shutdown();
        assert!(*executed.lock().unwrap());
    }

    #[test]
    fn create_sequenced_runner_executes_in_order() {
        let pool = ThreadPool::new(4);
        let runner = pool.create_sequenced_task_runner(default_traits());

        let results = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        for i in 0..5usize {
            let r = Arc::clone(&results);
            runner.post_task(Box::new(move || r.lock().unwrap().push(i)));
        }
        runner.post_task(Box::new(move || {
            b.wait();
        }));

        barrier.wait();
        pool.shutdown();
        assert_eq!(*results.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn create_task_runner_runs_in_parallel() {
        let pool = ThreadPool::new(4);
        let runner = pool.create_task_runner(default_traits());
        let barrier = Arc::new(Barrier::new(3)); // 2 tasks + test thread

        for _ in 0..2 {
            let b = Arc::clone(&barrier);
            runner.post_task(Box::new(move || {
                b.wait();
            }));
        }

        barrier.wait();
        pool.shutdown();
    }

    #[test]
    fn post_delayed_task_executes_after_deadline() {
        let pool = ThreadPool::new(2);
        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        pool.post_delayed_task(
            default_traits(),
            Box::new(move || {
                *e.lock().unwrap() = true;
                b.wait();
            }),
            Duration::from_millis(10),
        );

        barrier.wait();
        pool.shutdown();
        assert!(*executed.lock().unwrap());
    }

    #[test]
    fn skip_on_shutdown_task_is_rejected_after_shutdown() {
        // Verify that will_post_task() rejects SkipOnShutdown tasks once shutdown has
        // started. (Tasks already in the queue use a best-effort check inside
        // the wrapper closure.)
        let pool = ThreadPool::new(2);
        pool.task_tracker.shutdown(); // mark shutdown without waiting (no BlockShutdown tasks)

        let ran = Arc::new(Mutex::new(false));
        let ran_clone = Arc::clone(&ran);

        let posted = pool.post_task(
            traits_with(TaskShutdownBehavior::SkipOnShutdown),
            Box::new(move || {
                *ran_clone.lock().unwrap() = true;
            }),
        );

        assert!(!posted, "SkipOnShutdown task should be rejected after shutdown");
        assert!(!*ran.lock().unwrap(), "Task should not have run");

        pool.thread_group.join_all();
        pool.delayed_task_manager.shutdown();
    }

    #[test]
    fn attached_thread_runs_posted_tasks() {
        use std::thread;

        // A pool with zero built-in workers: the ONLY worker is the one we
        // attach from a thread we spawn ourselves. If the task runs, the
        // attached thread genuinely joined the pool.
        let pool = ThreadPool::new(0);
        let p = Arc::clone(&pool);
        let extra = thread::spawn(move || p.attach_current_thread());

        let ran = Arc::new(Mutex::new(false));
        let r = Arc::clone(&ran);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        pool.post_task(
            default_traits(),
            Box::new(move || {
                *r.lock().unwrap() = true;
                b.wait();
            }),
        );

        barrier.wait();
        pool.shutdown(); // signals the attached worker to exit its loop
        extra.join().unwrap();
        assert!(*ran.lock().unwrap());
    }

    #[test]
    fn post_task_after_shutdown_is_rejected() {
        let pool = ThreadPool::new(2);
        pool.task_tracker.shutdown();

        let result =
            pool.post_task(traits_with(TaskShutdownBehavior::SkipOnShutdown), Box::new(|| {}));
        assert!(!result);

        pool.thread_group.join_all();
        pool.delayed_task_manager.shutdown();
    }
}
