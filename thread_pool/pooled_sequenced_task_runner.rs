use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::sequence_token::SequenceToken;
use crate::sequenced_task_runner::SequencedTaskRunner;
use crate::task::Task;
use crate::task_runner::TaskRunner;
use crate::task_traits::TaskTraits;
use crate::thread_pool::delayed_task_manager::DelayedTaskManager;
use crate::thread_pool::sequence::Sequence;
use crate::thread_pool::thread_group::ThreadGroup;

pub struct PooledSequencedTaskRunner {
    sequence: Arc<Sequence>,
    thread_group: Arc<ThreadGroup>,
    delayed_task_manager: Arc<DelayedTaskManager>,
}

impl PooledSequencedTaskRunner {
    pub fn new(
        traits: TaskTraits,
        thread_group: Arc<ThreadGroup>,
        delayed_task_manager: Arc<DelayedTaskManager>,
    ) -> Arc<Self> {
        let runner = Arc::new(Self {
            sequence: Arc::new(Sequence::new(traits)),
            thread_group,
            delayed_task_manager,
        });
        // Wire back-reference so tasks can re-post to the same sequence via
        // current_default().
        let weak: Weak<dyn SequencedTaskRunner> =
            Arc::downgrade(&runner) as Weak<dyn SequencedTaskRunner>;
        runner.sequence.set_task_runner(weak);
        runner
    }
}

impl TaskRunner for PooledSequencedTaskRunner {
    fn post_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        self.sequence.push_task(Task::new(callback));
        self.thread_group.push_task_source(self.sequence.clone());
        true
    }

    fn post_delayed_task(
        &self,
        callback: Box<dyn FnOnce() + Send + 'static>,
        delay: Duration,
    ) -> bool {
        let ready_time = std::time::Instant::now() + delay;
        self.sequence.push_delayed_task(Task::new(callback), ready_time);
        self.delayed_task_manager.add_sequence(ready_time, Arc::clone(&self.sequence));
        true
    }

    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool {
        // Capture the caller's current-default runner so reply executes on the caller's
        // sequence.
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

impl SequencedTaskRunner for PooledSequencedTaskRunner {
    fn post_non_nestable_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        // Non-nestable tasks are treated the same as regular tasks in a thread-pool
        // context because the thread pool never runs nested run loops.
        self.post_task(callback)
    }

    fn runs_tasks_in_current_sequence(&self) -> bool {
        SequenceToken::current() == Some(self.sequence.token())
    }

    fn sequence_token(&self) -> SequenceToken {
        self.sequence.token()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_traits::TaskTraits;
    use crate::thread_pool::delayed_task_manager::DelayedTaskManager;
    use crate::thread_pool::thread_group::ThreadGroup;
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    fn make_runner(
        num_threads: usize,
    ) -> (Arc<ThreadGroup>, Arc<DelayedTaskManager>, Arc<PooledSequencedTaskRunner>) {
        let group = ThreadGroup::new(num_threads);
        let dtm = DelayedTaskManager::new(Arc::clone(&group));
        let runner = PooledSequencedTaskRunner::new(
            TaskTraits::default(),
            Arc::clone(&group),
            Arc::clone(&dtm),
        );
        (group, dtm, runner)
    }

    #[test]
    fn tasks_execute_in_order() {
        let (group, dtm, runner) = make_runner(4);

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
        group.join_all();
        dtm.shutdown();

        assert_eq!(*results.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn runs_tasks_in_current_sequence_is_true_inside_task() {
        let (group, dtm, runner) = make_runner(2);

        let result = Arc::new(Mutex::new(false));
        let r = Arc::clone(&result);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        let runner_clone = Arc::clone(&runner);
        runner.post_task(Box::new(move || {
            *r.lock().unwrap() = runner_clone.runs_tasks_in_current_sequence();
            b.wait();
        }));

        barrier.wait();
        group.join_all();
        dtm.shutdown();

        assert!(*result.lock().unwrap());
    }

    #[test]
    fn post_task_and_reply_executes_reply_on_caller_sequence() {
        let group = ThreadGroup::new(4);
        let dtm = DelayedTaskManager::new(Arc::clone(&group));

        let runner_a = PooledSequencedTaskRunner::new(
            TaskTraits::default(),
            Arc::clone(&group),
            Arc::clone(&dtm),
        );
        let runner_b = PooledSequencedTaskRunner::new(
            TaskTraits::default(),
            Arc::clone(&group),
            Arc::clone(&dtm),
        );

        let reply_sequence = Arc::new(Mutex::new(None::<SequenceToken>));
        let rs = Arc::clone(&reply_sequence);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        let runner_a_clone = Arc::clone(&runner_a);
        let runner_b_clone = Arc::clone(&runner_b);
        runner_a.post_task(Box::new(move || {
            runner_b_clone.post_task_and_reply(
                Box::new(|| {}),
                Box::new(move || {
                    *rs.lock().unwrap() = SequenceToken::current();
                    b.wait();
                }),
            );
            drop(runner_a_clone);
        }));

        barrier.wait();
        group.join_all();
        dtm.shutdown();

        assert_eq!(*reply_sequence.lock().unwrap(), Some(runner_a.sequence_token()));
    }

    #[test]
    fn delayed_task_executes_after_deadline() {
        let (group, dtm, runner) = make_runner(2);

        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        runner.post_delayed_task(
            Box::new(move || {
                *e.lock().unwrap() = true;
                b.wait();
            }),
            Duration::from_millis(10),
        );

        barrier.wait();
        group.join_all();
        dtm.shutdown();

        assert!(*executed.lock().unwrap());
    }
}
