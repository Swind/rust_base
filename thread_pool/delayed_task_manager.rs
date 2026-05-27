use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::Instant;

use crate::thread_pool::sequence::Sequence;
use crate::thread_pool::thread_group::ThreadGroup;

static NEXT_ENTRY_ID: AtomicU64 = AtomicU64::new(0);

// Pairs a ready_time with a Sequence that has a delayed task waiting in it.
// The timer thread pushes the Sequence to ThreadGroup when ready_time arrives;
// the worker then calls take_task() which flushes expired delayed tasks.
struct DelayedEntry {
    ready_time: Instant,
    id: u64,
    sequence: Arc<Sequence>,
}

impl PartialEq for DelayedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for DelayedEntry {}

impl PartialOrd for DelayedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Natural order: earlier deadline = smaller.  Combined with BinaryHeap<Reverse<>>
        // this gives a min-heap sorted by ready_time.
        self.ready_time.cmp(&other.ready_time).then(self.id.cmp(&other.id))
    }
}

struct DelayedManagerInner {
    heap: BinaryHeap<Reverse<DelayedEntry>>,
    handle: Option<thread::JoinHandle<()>>,
    shutdown: bool,
}

pub struct DelayedTaskManager {
    inner: Mutex<DelayedManagerInner>,
    condvar: Condvar,
    thread_group: Weak<ThreadGroup>,
}

impl DelayedTaskManager {
    pub fn new(thread_group: Arc<ThreadGroup>) -> Arc<Self> {
        let manager = Arc::new(Self {
            inner: Mutex::new(DelayedManagerInner {
                heap: BinaryHeap::new(),
                handle: None,
                shutdown: false,
            }),
            condvar: Condvar::new(),
            thread_group: Arc::downgrade(&thread_group),
        });

        let m = Arc::clone(&manager);
        let handle = thread::spawn(move || timer_loop(m));
        manager.inner.lock().unwrap().handle = Some(handle);

        manager
    }

    // Register a Sequence that has a delayed task ready at ready_time.
    // The timer thread will push the Sequence to ThreadGroup when the time comes.
    pub fn add_sequence(&self, ready_time: Instant, sequence: Arc<Sequence>) {
        {
            let mut inner = self.inner.lock().unwrap();
            let id = NEXT_ENTRY_ID.fetch_add(1, AtomicOrdering::Relaxed);
            inner.heap.push(Reverse(DelayedEntry { ready_time, id, sequence }));
        }
        // Wake the timer thread in case the new entry has an earlier deadline.
        self.condvar.notify_one();
    }

    pub fn shutdown(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.shutdown = true;
        }
        self.condvar.notify_one();

        let handle = self.inner.lock().unwrap().handle.take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

fn timer_loop(manager: Arc<DelayedTaskManager>) {
    loop {
        let mut inner = manager.inner.lock().unwrap();

        if inner.shutdown {
            break;
        }

        if inner.heap.is_empty() {
            // Nothing to wait for; block until a new entry is added or shutdown.
            drop(manager.condvar.wait(inner).unwrap());
            continue;
        }

        let next_ready = inner.heap.peek().map(|Reverse(e)| e.ready_time).unwrap();
        let now = Instant::now();

        if next_ready <= now {
            let Reverse(entry) = inner.heap.pop().unwrap();
            // Release the lock before notifying ThreadGroup to avoid holding two locks.
            drop(inner);
            if let Some(tg) = manager.thread_group.upgrade() {
                tg.push_task_source(entry.sequence);
            }
        } else {
            let timeout = next_ready - now;
            // wait_timeout releases the lock and re-acquires on wake-up.
            // We drop the guard so the loop re-acquires cleanly at the top.
            let _ = manager.condvar.wait_timeout(inner, timeout).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use crate::task_traits::TaskTraits;
    use crate::thread_pool::sequence::Sequence;
    use crate::thread_pool::thread_group::ThreadGroup;
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    #[test]
    fn delayed_task_fires_after_deadline() {
        let group = ThreadGroup::new(2);
        let dtm = DelayedTaskManager::new(Arc::clone(&group));

        let executed = Arc::new(Mutex::new(false));
        let e = Arc::clone(&executed);
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);

        let seq = Arc::new(Sequence::new(TaskTraits::default()));
        let ready_time = Instant::now() + Duration::from_millis(10);
        seq.push_delayed_task(
            Task::new(Box::new(move || {
                *e.lock().unwrap() = true;
                b.wait();
            })),
            ready_time,
        );
        dtm.add_sequence(ready_time, Arc::clone(&seq));

        barrier.wait();
        group.join_all();
        dtm.shutdown();

        assert!(*executed.lock().unwrap());
    }

    #[test]
    fn earlier_deadline_fires_first() {
        let group = ThreadGroup::new(2);
        let dtm = DelayedTaskManager::new(Arc::clone(&group));

        let order = Arc::new(Mutex::new(Vec::new()));
        let barrier = Arc::new(Barrier::new(3)); // 2 tasks + test thread

        let now = Instant::now();

        // Post the later task first to confirm ordering is by deadline, not insertion order.
        for (label, delay_ms) in [("late", 40u64), ("early", 10u64)] {
            let o = Arc::clone(&order);
            let b = Arc::clone(&barrier);
            let seq = Arc::new(Sequence::new(TaskTraits::default()));
            let ready_time = now + Duration::from_millis(delay_ms);
            seq.push_delayed_task(
                Task::new(Box::new(move || {
                    o.lock().unwrap().push(label);
                    b.wait();
                })),
                ready_time,
            );
            dtm.add_sequence(ready_time, Arc::clone(&seq));
        }

        barrier.wait();
        group.join_all();
        dtm.shutdown();

        assert_eq!(*order.lock().unwrap(), vec!["early", "late"]);
    }
}
