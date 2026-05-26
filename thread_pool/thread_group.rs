use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::sequenced_task_runner::CurrentDefaultHandle;
use crate::thread_pool::priority_queue::PriorityQueue;
use crate::thread_pool::task_source::{RegisteredTaskSource, RunStatus, TaskSource};
use crate::thread_pool::worker_thread::ScopedSequenceToken;

struct ThreadGroupInner {
    priority_queue: PriorityQueue,
    handles: Vec<JoinHandle<()>>,
}

pub struct ThreadGroup {
    inner: Mutex<ThreadGroupInner>,
    condvar: Condvar,
    shutdown: AtomicBool,
}

impl ThreadGroup {
    pub fn new(num_threads: usize) -> Arc<Self> {
        let group = Arc::new(Self {
            inner: Mutex::new(ThreadGroupInner {
                priority_queue: PriorityQueue::new(),
                handles: Vec::new(),
            }),
            condvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
        });

        {
            let mut inner = group.inner.lock().unwrap();
            for _ in 0..num_threads {
                let group_clone = Arc::clone(&group);
                let handle = thread::spawn(move || worker_loop(group_clone));
                inner.handles.push(handle);
            }
        }

        group
    }

    pub fn push_task_source(&self, source: Arc<dyn TaskSource>) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.priority_queue.push(source);
        }
        self.condvar.notify_one();
    }

    pub fn get_work(&self) -> Option<RegisteredTaskSource> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            if let Some(source) = inner.priority_queue.pop() {
                return Some(RegisteredTaskSource::new(source));
            }
            // wait atomically releases the lock and blocks; re-acquires on wake-up.
            inner = self.condvar.wait(inner).unwrap();
        }
    }

    pub fn join_all(&self) {
        // Set the shutdown flag while holding the lock so no worker can slip between
        // checking the flag and calling wait(), which would cause a lost wake-up.
        {
            let _guard = self.inner.lock().unwrap();
            self.shutdown.store(true, Ordering::Release);
        }
        self.condvar.notify_all();

        // Take handles out before joining to avoid holding the lock while blocked.
        let handles = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.handles)
        };
        for handle in handles {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use crate::task_traits::TaskTraits;
    use crate::thread_pool::sequence::Sequence;
    use std::sync::{Arc, Barrier, Mutex};

    #[test]
    fn single_task_executes() {
        // Basic sanity check: push one task and confirm a worker executes it.
        //
        // Barrier::new(2) makes the test thread wait until the worker has finished
        // the task before calling join_all(), preventing a premature shutdown.
        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        let group = ThreadGroup::new(2);
        let seq = Arc::new(Sequence::new(TaskTraits::default()));

        seq.push_task(Task::new(Box::new(move || {
            *e.lock().unwrap() = true;
            b.wait(); // signal the test thread that the task is done
        })));

        group.push_task_source(seq);
        barrier.wait(); // wait for the worker to finish
        group.join_all();

        assert!(*executed.lock().unwrap());
    }

    #[test]
    fn tasks_in_same_sequence_execute_in_order() {
        // Tasks posted to the same Sequence must remain ordered even with 4 workers competing.
        //
        // The has_worker flag ensures only one worker holds the Sequence at a time,
        // so the FIFO order of the immediate_queue is always respected.
        let results = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        let group = ThreadGroup::new(4);
        let seq = Arc::new(Sequence::new(TaskTraits::default()));

        for i in 0..5usize {
            let r = Arc::clone(&results);
            seq.push_task(Task::new(Box::new(move || r.lock().unwrap().push(i))));
        }

        // Final task signals the test thread that all preceding tasks are done.
        seq.push_task(Task::new(Box::new(move || {
            b.wait();
        })));

        group.push_task_source(seq);
        barrier.wait();
        group.join_all();

        assert_eq!(*results.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn tasks_in_different_sequences_run_independently() {
        // Tasks in distinct Sequences must be able to run concurrently.
        //
        // Barrier::new(3) requires both worker tasks and the test thread to arrive together.
        // If the two tasks were forced to run sequentially, the first task's barrier.wait()
        // would block forever (deadlock), causing the test to fail.
        let barrier = Arc::new(Barrier::new(3));

        let group = ThreadGroup::new(4); // needs at least 2 workers to run in parallel

        for _ in 0..2 {
            let b = Arc::clone(&barrier);
            let seq = Arc::new(Sequence::new(TaskTraits::default()));
            seq.push_task(Task::new(Box::new(move || {
                b.wait();
            })));
            group.push_task_source(seq);
        }

        barrier.wait(); // all three parties must arrive simultaneously
        group.join_all();
    }
}

fn worker_loop(group: Arc<ThreadGroup>) {
    while let Some(registered) = group.get_work() {
        let source = registered.into_source();
        let env = source.get_execution_environment();

        let _token_guard = ScopedSequenceToken::new(env.token);
        let _default_handle = env.task_runner.map(CurrentDefaultHandle::new);

        match source.will_run_task() {
            RunStatus::Disallowed => {}
            _ => {
                if let Some(task) = source.take_task() {
                    (task.callback)();
                }
            }
        }

        // did_process_task clears has_worker and returns true if more ready tasks remain.
        if source.did_process_task() {
            group.push_task_source(source);
        }
    }
}
