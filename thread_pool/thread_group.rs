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
        // shutdown 標記必須在持有 lock 時設定，避免 worker 在 check 和 wait 之間錯過通知
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
            // wait 會原子性地釋放 lock 並阻塞，被喚醒後重新取得 lock
            inner = self.condvar.wait(inner).unwrap();
        }
    }

    pub fn join_all(&self) {
        // shutdown 標記在持有 lock 時設定，確保不會有 worker 在 check 後、wait 前錯過通知
        {
            let _guard = self.inner.lock().unwrap();
            self.shutdown.store(true, Ordering::Release);
        }
        self.condvar.notify_all();

        // 取出所有 handle 後再 join，避免持有 lock 時阻塞
        let handles = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.handles)
        };
        for handle in handles {
            let _ = handle.join();
        }
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

        // did_process_task 設定 has_worker = false，回傳是否還有 ready task
        if source.did_process_task() {
            group.push_task_source(source);
        }
    }
}
