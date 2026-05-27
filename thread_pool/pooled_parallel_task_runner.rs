use std::sync::Arc;
use std::time::Duration;

use crate::task::Task;
use crate::task_runner::TaskRunner;
use crate::task_traits::TaskTraits;
use crate::thread_pool::delayed_task_manager::DelayedTaskManager;
use crate::thread_pool::sequence::Sequence;
use crate::thread_pool::thread_group::ThreadGroup;

// Each posted task gets its own Sequence, so all tasks can run in parallel.
// There is no ordering guarantee between tasks posted to the same runner.
pub struct PooledParallelTaskRunner {
    traits: TaskTraits,
    thread_group: Arc<ThreadGroup>,
    delayed_task_manager: Arc<DelayedTaskManager>,
}

impl PooledParallelTaskRunner {
    pub fn new(
        traits: TaskTraits,
        thread_group: Arc<ThreadGroup>,
        delayed_task_manager: Arc<DelayedTaskManager>,
    ) -> Self {
        Self { traits, thread_group, delayed_task_manager }
    }
}

impl TaskRunner for PooledParallelTaskRunner {
    fn post_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        let seq = Arc::new(Sequence::new(self.traits));
        seq.push_task(Task::new(callback));
        self.thread_group.push_task_source(seq);
        true
    }

    fn post_delayed_task(
        &self,
        callback: Box<dyn FnOnce() + Send + 'static>,
        delay: Duration,
    ) -> bool {
        let ready_time = std::time::Instant::now() + delay;
        let seq = Arc::new(Sequence::new(self.traits));
        seq.push_delayed_task(Task::new(callback), ready_time);
        self.delayed_task_manager.add_sequence(ready_time, seq);
        true
    }

    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool {
        let reply_runner = crate::sequenced_task_runner::current_default();
        let wrapped: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
            task();
            if let Some(runner) = reply_runner {
                runner.post_task(reply);
            }
        });
        self.post_task(wrapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_traits::TaskTraits;
    use crate::thread_pool::delayed_task_manager::DelayedTaskManager;
    use crate::thread_pool::thread_group::ThreadGroup;
    use std::sync::{Arc, Barrier, Mutex};

    fn make_runner(
        num_threads: usize,
    ) -> (Arc<ThreadGroup>, Arc<DelayedTaskManager>, PooledParallelTaskRunner) {
        let group = ThreadGroup::new(num_threads);
        let dtm = DelayedTaskManager::new(Arc::clone(&group));
        let runner = PooledParallelTaskRunner::new(
            TaskTraits::default(),
            Arc::clone(&group),
            Arc::clone(&dtm),
        );
        (group, dtm, runner)
    }

    #[test]
    fn tasks_run_in_parallel() {
        let (group, dtm, runner) = make_runner(4);
        let barrier = Arc::new(Barrier::new(3));

        for _ in 0..2 {
            let b = Arc::clone(&barrier);
            runner.post_task(Box::new(move || {
                b.wait();
            }));
        }

        barrier.wait();
        group.join_all();
        dtm.shutdown();
    }

    #[test]
    fn all_tasks_execute() {
        let (group, dtm, runner) = make_runner(5); // at least as many as the number of tasks

        let counter = Arc::new(Mutex::new(0usize));
        let barrier = Arc::new(Barrier::new(6)); // 5 tasks + test thread

        for _ in 0..5 {
            let c = Arc::clone(&counter);
            let b = Arc::clone(&barrier);
            runner.post_task(Box::new(move || {
                *c.lock().unwrap() += 1;
                b.wait();
            }));
        }

        barrier.wait();
        group.join_all();
        dtm.shutdown();

        assert_eq!(*counter.lock().unwrap(), 5);
    }
}
