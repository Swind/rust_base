use std::sync::{Condvar, Mutex};

use crate::task_traits::{TaskShutdownBehavior, TaskTraits};

struct TaskTrackerInner {
    shutdown_started: bool,
    // Counts BlockShutdown tasks that have been posted but not yet finished.
    // Incremented at post time (will_post_task) so shutdown() waits for both
    // queued and currently-executing BlockShutdown tasks.
    num_tasks_blocking_shutdown: usize,
}

pub struct TaskTracker {
    inner: Mutex<TaskTrackerInner>,
    shutdown_done: Condvar,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TaskTrackerInner {
                shutdown_started: false,
                num_tasks_blocking_shutdown: 0,
            }),
            shutdown_done: Condvar::new(),
        }
    }

    // Returns true if the task may be posted.
    // For BlockShutdown tasks, increments the counter inside the same lock so
    // shutdown() cannot observe count==0 between the post and the execution.
    pub fn will_post_task(&self, traits: &TaskTraits) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutdown_started {
            return matches!(traits.shutdown_behavior, TaskShutdownBehavior::ContinueOnShutdown);
        }
        if traits.shutdown_behavior == TaskShutdownBehavior::BlockShutdown {
            inner.num_tasks_blocking_shutdown += 1;
        }
        true
    }

    // Called after a BlockShutdown task finishes executing.
    // Decrements the counter and notifies shutdown() if it reaches zero.
    pub fn after_run_task(&self, traits: &TaskTraits) {
        if traits.shutdown_behavior == TaskShutdownBehavior::BlockShutdown {
            let mut inner = self.inner.lock().unwrap();
            inner.num_tasks_blocking_shutdown -= 1;
            if inner.num_tasks_blocking_shutdown == 0 {
                self.shutdown_done.notify_all();
            }
        }
    }

    pub fn is_shutdown_started(&self) -> bool {
        self.inner.lock().unwrap().shutdown_started
    }

    // Marks shutdown as started and blocks until all BlockShutdown tasks finish.
    // Because will_post_task() increments the counter under the same lock,
    // there is no window where a queued BlockShutdown task goes unaccounted.
    pub fn shutdown(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.shutdown_started = true;
            if inner.num_tasks_blocking_shutdown == 0 {
                return;
            }
        }
        let mut inner = self.inner.lock().unwrap();
        while inner.num_tasks_blocking_shutdown > 0 {
            inner = self.shutdown_done.wait(inner).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_traits::{TaskPriority, TaskShutdownBehavior, TaskTraits, ThreadPolicy};
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn traits_with(behavior: TaskShutdownBehavior) -> TaskTraits {
        TaskTraits {
            priority: TaskPriority::UserVisible,
            shutdown_behavior: behavior,
            thread_policy: ThreadPolicy::PreferBackground,
            may_block: false,
        }
    }

    #[test]
    fn allows_all_tasks_before_shutdown() {
        let tracker = TaskTracker::new();
        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::ContinueOnShutdown)));
        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::SkipOnShutdown)));
        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::BlockShutdown)));
        // Clean up: the BlockShutdown post incremented the count
        tracker.after_run_task(&traits_with(TaskShutdownBehavior::BlockShutdown));
    }

    #[test]
    fn after_shutdown_only_continue_on_shutdown_is_allowed() {
        let tracker = TaskTracker::new();
        tracker.inner.lock().unwrap().shutdown_started = true;

        assert!(tracker.will_post_task(&traits_with(TaskShutdownBehavior::ContinueOnShutdown)));
        assert!(!tracker.will_post_task(&traits_with(TaskShutdownBehavior::SkipOnShutdown)));
        assert!(!tracker.will_post_task(&traits_with(TaskShutdownBehavior::BlockShutdown)));
    }

    #[test]
    fn shutdown_returns_immediately_when_no_block_shutdown_tasks() {
        let tracker = TaskTracker::new();
        tracker.shutdown();
    }

    #[test]
    fn will_post_task_increments_count_for_block_shutdown() {
        let tracker = TaskTracker::new();
        let traits = traits_with(TaskShutdownBehavior::BlockShutdown);

        tracker.will_post_task(&traits);
        tracker.will_post_task(&traits);
        assert_eq!(tracker.inner.lock().unwrap().num_tasks_blocking_shutdown, 2);

        tracker.after_run_task(&traits);
        assert_eq!(tracker.inner.lock().unwrap().num_tasks_blocking_shutdown, 1);

        tracker.after_run_task(&traits);
        assert_eq!(tracker.inner.lock().unwrap().num_tasks_blocking_shutdown, 0);
    }

    #[test]
    fn shutdown_waits_for_running_block_shutdown_task() {
        // Simulates a task that was already executing when shutdown() is called.
        let tracker = Arc::new(TaskTracker::new());
        let traits = traits_with(TaskShutdownBehavior::BlockShutdown);

        tracker.will_post_task(&traits); // simulate post
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let t = Arc::clone(&tracker);

        thread::spawn(move || {
            b.wait(); // signal that shutdown is now waiting
            t.after_run_task(&traits_with(TaskShutdownBehavior::BlockShutdown));
        });

        let t2 = Arc::clone(&tracker);
        let shutdown_handle = thread::spawn(move || t2.shutdown());

        barrier.wait();
        shutdown_handle.join().unwrap();
    }

    #[test]
    fn shutdown_waits_for_queued_block_shutdown_task() {
        // Core fix: will_post_task increments the count, so shutdown() must wait
        // even if the task has not yet started executing.
        let tracker = Arc::new(TaskTracker::new());
        let traits = traits_with(TaskShutdownBehavior::BlockShutdown);

        // Simulate: task posted (count = 1) but execution hasn't started yet.
        tracker.will_post_task(&traits);

        // Start shutdown in a background thread — it must block because count > 0.
        let t = Arc::clone(&tracker);
        let shutdown_handle = thread::spawn(move || t.shutdown());

        // Give shutdown() time to reach the wait.
        thread::sleep(std::time::Duration::from_millis(10));

        // Simulate task finishing — this should unblock shutdown().
        tracker.after_run_task(&traits);
        shutdown_handle.join().unwrap();
    }

    #[test]
    fn block_shutdown_rejected_after_shutdown_started() {
        // After shutdown starts, BlockShutdown posts must be rejected and must NOT
        // increment the counter (which would cause shutdown() to wait forever).
        let tracker = TaskTracker::new();
        tracker.inner.lock().unwrap().shutdown_started = true;

        let accepted = tracker.will_post_task(&traits_with(TaskShutdownBehavior::BlockShutdown));
        assert!(!accepted);
        assert_eq!(tracker.inner.lock().unwrap().num_tasks_blocking_shutdown, 0);
    }
}
