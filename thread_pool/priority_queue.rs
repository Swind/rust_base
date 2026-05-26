use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::thread_pool::task_source::{TaskSource, TaskSourceSortKey};

// Arc<dyn TaskSource> 不實作 Ord，用 newtype 只比較 sort_key
struct QueueEntry {
    sort_key: TaskSourceSortKey,
    task_source: Arc<dyn TaskSource>,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap 是 max-heap：「最大」的先出來
        // priority 高的優先，同 priority 時 ready_time 早的優先（反轉比較）
        self.sort_key
            .priority
            .cmp(&other.sort_key.priority)
            .then(other.sort_key.ready_time.cmp(&self.sort_key.ready_time))
    }
}

pub struct PriorityQueue {
    heap: BinaryHeap<QueueEntry>,
}

impl PriorityQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub fn push(&mut self, task_source: Arc<dyn TaskSource>) {
        let sort_key = task_source.get_sort_key();
        self.heap.push(QueueEntry { sort_key, task_source });
    }

    pub fn pop(&mut self) -> Option<Arc<dyn TaskSource>> {
        self.heap.pop().map(|e| e.task_source)
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}
