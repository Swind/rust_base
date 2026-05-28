use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Mutex, Weak};
use std::time::Instant;

use crate::sequence_token::SequenceToken;
use crate::sequenced_task_runner::SequencedTaskRunner;
use crate::task::Task;
use crate::task_traits::TaskTraits;
use crate::thread_pool::task_source::{
    ExecutionEnvironment, RunStatus, TaskSource, TaskSourceSortKey,
};

// BinaryHeap requires Ord; only ready_time and sequence_num are compared, the
// callback is ignored.
struct DelayedTask {
    ready_time: Instant,
    sequence_num: u64,
    task: Task,
}

impl PartialEq for DelayedTask {
    fn eq(&self, other: &Self) -> bool {
        self.ready_time == other.ready_time && self.sequence_num == other.sequence_num
    }
}

impl Eq for DelayedTask {}

impl PartialOrd for DelayedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Natural order: earlier deadline = smaller value.
        // Combined with BinaryHeap<Reverse<>> this gives a min-heap by ready_time.
        self.ready_time.cmp(&other.ready_time).then(self.sequence_num.cmp(&other.sequence_num))
    }
}

struct SequenceInner {
    immediate_queue: VecDeque<Task>,
    // Reverse turns BinaryHeap into a min-heap so peek() yields the earliest deadline.
    delayed_queue: BinaryHeap<Reverse<DelayedTask>>,
    next_sequence_num: u64,
}

pub struct Sequence {
    token: SequenceToken,
    inner: Mutex<SequenceInner>,
    has_worker: AtomicBool,
    traits: TaskTraits,
    // Weak avoids a reference cycle with PooledSequencedTaskRunner.
    // Option is needed because Weak::<dyn T>::new() requires T: Sized.
    task_runner: Mutex<Option<Weak<dyn SequencedTaskRunner>>>,
}

impl Sequence {
    pub fn new(traits: TaskTraits) -> Self {
        Self {
            token: SequenceToken::create(),
            inner: Mutex::new(SequenceInner {
                immediate_queue: VecDeque::new(),
                delayed_queue: BinaryHeap::new(),
                next_sequence_num: 0,
            }),
            has_worker: AtomicBool::new(false),
            traits,
            task_runner: Mutex::new(None),
        }
    }

    pub fn token(&self) -> SequenceToken {
        self.token
    }

    pub fn set_task_runner(&self, runner: Weak<dyn SequencedTaskRunner>) {
        *self.task_runner.lock().unwrap() = Some(runner);
    }

    pub fn push_task(&self, mut task: Task) {
        let mut inner = self.inner.lock().unwrap();
        task.sequence_num = inner.next_sequence_num;
        inner.next_sequence_num += 1;
        inner.immediate_queue.push_back(task);
    }

    pub fn push_delayed_task(&self, mut task: Task, ready_time: Instant) {
        let mut inner = self.inner.lock().unwrap();
        task.sequence_num = inner.next_sequence_num;
        inner.next_sequence_num += 1;
        inner.delayed_queue.push(Reverse(DelayedTask {
            ready_time,
            sequence_num: task.sequence_num,
            task,
        }));
    }

    // Must be called while holding the lock: moves all expired delayed tasks into
    // immediate_queue.
    fn flush_ready_delayed_tasks(inner: &mut SequenceInner, now: Instant) {
        while let Some(Reverse(delayed)) = inner.delayed_queue.peek() {
            if delayed.ready_time <= now {
                let Reverse(delayed) = inner.delayed_queue.pop().unwrap();
                inner.immediate_queue.push_back(delayed.task);
            } else {
                break;
            }
        }
    }
}

impl TaskSource for Sequence {
    fn get_execution_environment(&self) -> ExecutionEnvironment {
        ExecutionEnvironment {
            token: self.token,
            task_runner: self.task_runner.lock().unwrap().as_ref().and_then(|w| w.upgrade()),
        }
    }

    fn get_sort_key(&self) -> TaskSourceSortKey {
        TaskSourceSortKey { priority: self.traits.priority, ready_time: Instant::now() }
    }

    fn has_ready_tasks(&self, now: Instant) -> bool {
        let inner = self.inner.lock().unwrap();
        !inner.immediate_queue.is_empty()
            || inner.delayed_queue.peek().is_some_and(|Reverse(d)| d.ready_time <= now)
    }

    fn will_run_task(&self) -> RunStatus {
        // swap returns the old value: true means a worker already owns this sequence
        // (reject); false means we successfully claimed it.
        if self.has_worker.swap(true, AtomicOrdering::AcqRel) {
            RunStatus::Disallowed
        } else {
            RunStatus::AllowedNotSaturated
        }
    }

    fn take_task(&self) -> Option<Task> {
        let mut inner = self.inner.lock().unwrap();
        Self::flush_ready_delayed_tasks(&mut inner, Instant::now());
        inner.immediate_queue.pop_front()
    }

    fn did_process_task(&self) -> bool {
        self.has_worker.store(false, AtomicOrdering::Release);
        let inner = self.inner.lock().unwrap();
        let now = Instant::now();
        !inner.immediate_queue.is_empty()
            || inner.delayed_queue.peek().is_some_and(|Reverse(d)| d.ready_time <= now)
    }

    fn will_re_enqueue(&self, now: Instant) -> bool {
        self.has_ready_tasks(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use crate::task_traits::TaskTraits;
    use crate::thread_pool::task_source::{RunStatus, TaskSource};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    // Simulates the full worker execution cycle:
    // will_run_task -> take_task -> callback -> did_process_task.
    // Returns the result of did_process_task (true = more tasks remain).
    fn run_one_task(seq: &Sequence) -> bool {
        match seq.will_run_task() {
            RunStatus::Disallowed => false,
            _ => {
                if let Some(task) = seq.take_task() {
                    (task.callback)();
                }
                seq.did_process_task()
            }
        }
    }

    #[test]
    fn push_and_take_task() {
        // Verify the basic push -> take -> execute round trip.
        let seq = Arc::new(Sequence::new(TaskTraits::default()));
        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);

        seq.push_task(Task::new(Box::new(move || *e.lock().unwrap() = true)));

        assert!(seq.has_ready_tasks(Instant::now()));
        run_one_task(&seq);
        assert!(*executed.lock().unwrap());
    }

    #[test]
    fn tasks_execute_in_fifo_order() {
        // Tasks posted to the same Sequence must execute in the order they were pushed
        // (FIFO).
        let seq = Arc::new(Sequence::new(TaskTraits::default()));
        let results = Arc::new(Mutex::new(Vec::new()));

        for i in 0..5usize {
            let r = Arc::clone(&results);
            seq.push_task(Task::new(Box::new(move || r.lock().unwrap().push(i))));
        }

        while seq.has_ready_tasks(Instant::now()) {
            run_one_task(&seq);
        }

        assert_eq!(*results.lock().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn has_worker_prevents_second_worker() {
        // will_run_task must return Disallowed while has_worker is true.
        // This is the core mechanism that prevents a Sequence from running
        // concurrently.
        let seq = Arc::new(Sequence::new(TaskTraits::default()));
        seq.push_task(Task::new(Box::new(|| {})));

        // First call succeeds and sets has_worker = true.
        match seq.will_run_task() {
            RunStatus::AllowedNotSaturated => {}
            _ => panic!("expected AllowedNotSaturated"),
        }

        // Second call is rejected because a worker already owns the sequence.
        match seq.will_run_task() {
            RunStatus::Disallowed => {}
            _ => panic!("expected Disallowed"),
        }

        seq.take_task();
        seq.did_process_task(); // clears has_worker
    }

    #[test]
    fn delayed_task_not_ready_before_time() {
        // A delayed task whose deadline has not yet passed must not appear in
        // has_ready_tasks.
        let seq = Arc::new(Sequence::new(TaskTraits::default()));
        let future = Instant::now() + Duration::from_secs(60);

        seq.push_delayed_task(Task::new(Box::new(|| {})), future);

        assert!(!seq.has_ready_tasks(Instant::now()));
    }

    #[test]
    fn delayed_task_ready_after_deadline() {
        // A delayed task whose deadline has already passed must be taken and executed.
        let seq = Arc::new(Sequence::new(TaskTraits::default()));
        let past = Instant::now() - Duration::from_millis(1);
        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);

        seq.push_delayed_task(Task::new(Box::new(move || *e.lock().unwrap() = true)), past);

        assert!(seq.has_ready_tasks(Instant::now()));
        run_one_task(&seq);
        assert!(*executed.lock().unwrap());
    }
}
