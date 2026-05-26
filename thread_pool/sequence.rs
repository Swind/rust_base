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

// BinaryHeap 需要 Ord，只用 ready_time 和 sequence_num 比較，忽略 callback
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
        // 自然序：越早到期越小，配合 BinaryHeap<Reverse<>> 成為 min-heap
        self.ready_time
            .cmp(&other.ready_time)
            .then(self.sequence_num.cmp(&other.sequence_num))
    }
}

struct SequenceInner {
    immediate_queue: VecDeque<Task>,
    // Reverse 讓 BinaryHeap 變成 min-heap，peek() 取出最早到期的 task
    delayed_queue: BinaryHeap<Reverse<DelayedTask>>,
    next_sequence_num: u64,
}

pub struct Sequence {
    token: SequenceToken,
    inner: Mutex<SequenceInner>,
    has_worker: AtomicBool,
    traits: TaskTraits,
    // Weak 避免與 PooledSequencedTaskRunner 形成循環引用；Option 因為 Weak::<dyn T>::new() 不支援 unsized type
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

    // 在持有 lock 的情況下，把到期的 delayed task 移進 immediate_queue
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
        TaskSourceSortKey {
            priority: self.traits.priority,
            ready_time: Instant::now(),
        }
    }

    fn has_ready_tasks(&self, now: Instant) -> bool {
        let inner = self.inner.lock().unwrap();
        !inner.immediate_queue.is_empty()
            || inner
                .delayed_queue
                .peek()
                .map_or(false, |Reverse(d)| d.ready_time <= now)
    }

    fn will_run_task(&self) -> RunStatus {
        // swap 回傳舊值：舊值為 true 代表已有 worker，拒絕；舊值為 false 代表成功取得
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
            || inner
                .delayed_queue
                .peek()
                .map_or(false, |Reverse(d)| d.ready_time <= now)
    }

    fn will_re_enqueue(&self, now: Instant) -> bool {
        self.has_ready_tasks(now)
    }
}
