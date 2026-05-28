use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ── Public types ──────────────────────────────────────────────────────────────

/// Timing data delivered after each task completes.
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    /// Time the task spent waiting in the queue before a worker picked it up.
    pub queue_time: Duration,
    /// Time the task's callback took to execute.
    pub execution_time: Duration,
}

/// Reported when a worker has been running the same task longer than the
/// configured hang threshold.  May fire repeatedly (every `watchdog_interval`)
/// while the hang persists.
#[derive(Debug, Clone)]
pub struct HangInfo {
    /// Internal worker index, stable for the lifetime of the monitor.
    pub worker_id: usize,
    /// How long the worker has been stuck on the current task.
    pub stuck_duration: Duration,
}

// ── TaskMonitor ───────────────────────────────────────────────────────────────

struct Inner {
    reference: Instant,
    slots: Mutex<Vec<Arc<AtomicU64>>>, // one per registered worker; 0 = idle
    hang_threshold: Duration,
    on_metrics: Option<Arc<dyn Fn(&TaskMetrics) + Send + Sync>>,
    on_hang: Option<Arc<dyn Fn(&HangInfo) + Send + Sync>>,
    shutdown: AtomicBool,
    shutdown_notify: (Mutex<bool>, Condvar),
}

/// Monitors queue time, execution time, and long-running tasks across a
/// `ThreadPool` and/or `IoTaskRunner`.
///
/// # Usage
///
/// ```ignore
/// let monitor = TaskMonitor::builder()
///     .hang_threshold(Duration::from_secs(5))
///     .on_metrics(|m| println!("queued {:?}  ran {:?}", m.queue_time, m.execution_time))
///     .on_hang(|h| eprintln!("worker {} stuck for {:?}", h.worker_id, h.stuck_duration))
///     .build();
///
/// let pool = ThreadPool::new_with_monitor(4, Arc::clone(&monitor));
/// let io   = IoTaskRunner::new_with_monitor(Arc::clone(&monitor));
/// ```
pub struct TaskMonitor {
    inner: Arc<Inner>,
    watchdog: Mutex<Option<thread::JoinHandle<()>>>,
}

impl TaskMonitor {
    pub fn builder() -> TaskMonitorBuilder {
        TaskMonitorBuilder::new()
    }

    /// Register a worker thread with this monitor.
    ///
    /// Returns a [`WorkerSlot`] that the worker uses to bracket each task
    /// execution with [`WorkerSlot::task_started`] / [`WorkerSlot::task_finished`].
    /// The slot is automatically zeroed (idle) when dropped.
    pub fn register_worker(&self) -> WorkerSlot {
        let slot = Arc::new(AtomicU64::new(0));
        let mut slots = self.inner.slots.lock().unwrap();
        let idx = slots.len();
        slots.push(Arc::clone(&slot));
        WorkerSlot { inner: Arc::clone(&self.inner), idx, slot }
    }

    /// Wrap a task callback to measure queue time and execution time.
    ///
    /// Call this at **post time** (not execution time) so that `queue_time` is
    /// measured from posting to the moment the worker starts running the task.
    /// The `on_metrics` callback is invoked immediately after the task finishes.
    pub fn wrap_task(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Box<dyn FnOnce() + Send + 'static> {
        let posted_at = Instant::now();
        let on_metrics = self.inner.on_metrics.clone();
        Box::new(move || {
            let queue_time = posted_at.elapsed();
            let exec_start = Instant::now();
            task();
            if let Some(ref f) = on_metrics {
                f(&TaskMetrics { queue_time, execution_time: exec_start.elapsed() });
            }
        })
    }
}

impl Drop for TaskMonitor {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        let (lock, cvar) = &self.inner.shutdown_notify;
        *lock.lock().unwrap() = true;
        cvar.notify_one();
        if let Some(handle) = self.watchdog.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

// ── WorkerSlot ────────────────────────────────────────────────────────────────

/// RAII handle representing one worker thread's monitoring slot.
///
/// Obtained via [`TaskMonitor::register_worker`].  Call [`task_started`] before
/// invoking a task callback and [`task_finished`] after — the watchdog uses
/// these timestamps to detect hangs.
///
/// [`task_started`]: WorkerSlot::task_started
/// [`task_finished`]: WorkerSlot::task_finished
pub struct WorkerSlot {
    inner: Arc<Inner>,
    /// Index of this slot within the monitor; matches `HangInfo::worker_id`.
    pub idx: usize,
    slot: Arc<AtomicU64>,
}

impl WorkerSlot {
    /// Mark the start of a task execution on this worker.
    pub fn task_started(&self) {
        // Store nanos since reference; max(1) ensures 0 always means idle.
        let nanos = (self.inner.reference.elapsed().as_nanos() as u64).max(1);
        self.slot.store(nanos, Ordering::Release);
    }

    /// Mark the end of a task execution on this worker.
    pub fn task_finished(&self) {
        self.slot.store(0, Ordering::Release);
    }
}

impl Drop for WorkerSlot {
    fn drop(&mut self) {
        self.slot.store(0, Ordering::Release);
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`TaskMonitor`].
pub struct TaskMonitorBuilder {
    hang_threshold: Option<Duration>,
    watchdog_interval: Duration,
    on_metrics: Option<Arc<dyn Fn(&TaskMetrics) + Send + Sync>>,
    on_hang: Option<Arc<dyn Fn(&HangInfo) + Send + Sync>>,
}

impl TaskMonitorBuilder {
    fn new() -> Self {
        Self {
            hang_threshold: None,
            watchdog_interval: Duration::from_secs(1),
            on_metrics: None,
            on_hang: None,
        }
    }

    /// Tasks running longer than `d` will trigger `on_hang`.
    /// Must be set to enable hang detection and start the watchdog thread.
    pub fn hang_threshold(mut self, d: Duration) -> Self {
        self.hang_threshold = Some(d);
        self
    }

    /// How often the watchdog scans for hung workers (default: 1 s).
    pub fn watchdog_interval(mut self, d: Duration) -> Self {
        self.watchdog_interval = d;
        self
    }

    /// Called after each task completes with queue + execution timing.
    pub fn on_metrics(mut self, f: impl Fn(&TaskMetrics) + Send + Sync + 'static) -> Self {
        self.on_metrics = Some(Arc::new(f));
        self
    }

    /// Called when a worker has been stuck longer than `hang_threshold`.
    pub fn on_hang(mut self, f: impl Fn(&HangInfo) + Send + Sync + 'static) -> Self {
        self.on_hang = Some(Arc::new(f));
        self
    }

    /// Construct the monitor, starting the watchdog thread if `hang_threshold`
    /// was configured.
    pub fn build(self) -> Arc<TaskMonitor> {
        let inner = Arc::new(Inner {
            reference: Instant::now(),
            slots: Mutex::new(Vec::new()),
            hang_threshold: self.hang_threshold.unwrap_or(Duration::MAX),
            on_metrics: self.on_metrics,
            on_hang: self.on_hang,
            shutdown: AtomicBool::new(false),
            shutdown_notify: (Mutex::new(false), Condvar::new()),
        });

        let watchdog = if self.hang_threshold.is_some() {
            let inner_clone = Arc::clone(&inner);
            let interval = self.watchdog_interval;
            Some(thread::spawn(move || watchdog_loop(inner_clone, interval)))
        } else {
            None
        };

        Arc::new(TaskMonitor { inner, watchdog: Mutex::new(watchdog) })
    }
}

// ── Watchdog loop ─────────────────────────────────────────────────────────────

fn watchdog_loop(inner: Arc<Inner>, interval: Duration) {
    let (lock, cvar) = &inner.shutdown_notify;
    loop {
        let (guard, result) = cvar.wait_timeout(lock.lock().unwrap(), interval).unwrap();
        drop(guard);

        if inner.shutdown.load(Ordering::Acquire) || !result.timed_out() {
            break;
        }

        let now_nanos = inner.reference.elapsed().as_nanos() as u64;
        let threshold_nanos =
            inner.hang_threshold.as_nanos().min(u64::MAX as u128) as u64;

        // Collect hung workers while holding the lock, then release before
        // calling user callbacks to avoid potential lock-order issues.
        let hung: Vec<HangInfo> = {
            let slots = inner.slots.lock().unwrap();
            slots
                .iter()
                .enumerate()
                .filter_map(|(idx, slot)| {
                    let started = slot.load(Ordering::Acquire);
                    if started == 0 {
                        return None;
                    }
                    let elapsed = now_nanos.saturating_sub(started);
                    if elapsed >= threshold_nanos {
                        Some(HangInfo {
                            worker_id: idx,
                            stuck_duration: Duration::from_nanos(elapsed),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        };

        if let Some(ref f) = inner.on_hang {
            for info in hung {
                f(&info);
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};

    #[test]
    fn metrics_reported_after_task() {
        let metrics: Arc<Mutex<Vec<TaskMetrics>>> = Arc::new(Mutex::new(Vec::new()));
        let m = Arc::clone(&metrics);
        let monitor = TaskMonitor::builder()
            .on_metrics(move |met| m.lock().unwrap().push(met.clone()))
            .build();

        let wrapped = monitor.wrap_task(Box::new(|| {
            thread::sleep(Duration::from_millis(5));
        }));
        wrapped();

        let got = metrics.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert!(got[0].execution_time >= Duration::from_millis(5));
    }

    #[test]
    fn queue_time_measured_from_post() {
        let metrics: Arc<Mutex<Vec<TaskMetrics>>> = Arc::new(Mutex::new(Vec::new()));
        let m = Arc::clone(&metrics);
        let monitor = TaskMonitor::builder()
            .on_metrics(move |met| m.lock().unwrap().push(met.clone()))
            .build();

        let wrapped = monitor.wrap_task(Box::new(|| {}));
        thread::sleep(Duration::from_millis(10)); // simulate queueing delay
        wrapped();

        let got = metrics.lock().unwrap();
        assert!(got[0].queue_time >= Duration::from_millis(10));
    }

    #[test]
    fn hang_detected_when_task_runs_too_long() {
        let hangs: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let h = Arc::clone(&hangs);
        let monitor = TaskMonitor::builder()
            .hang_threshold(Duration::from_millis(30))
            .watchdog_interval(Duration::from_millis(10))
            .on_hang(move |info| h.lock().unwrap().push(info.worker_id))
            .build();

        let slot = monitor.register_worker();
        slot.task_started();
        thread::sleep(Duration::from_millis(80)); // well over threshold
        slot.task_finished();

        thread::sleep(Duration::from_millis(30)); // let watchdog run one more time
        assert!(!hangs.lock().unwrap().is_empty(), "hang should have been detected");
        assert_eq!(hangs.lock().unwrap()[0], 0);
    }

    #[test]
    fn no_hang_when_task_finishes_quickly() {
        let hangs: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let h = Arc::clone(&hangs);
        let monitor = TaskMonitor::builder()
            .hang_threshold(Duration::from_millis(200))
            .watchdog_interval(Duration::from_millis(30))
            .on_hang(move |info| h.lock().unwrap().push(info.worker_id))
            .build();

        let slot = monitor.register_worker();
        slot.task_started();
        thread::sleep(Duration::from_millis(5)); // well within threshold
        slot.task_finished();

        thread::sleep(Duration::from_millis(100)); // wait for potential false positive
        assert!(hangs.lock().unwrap().is_empty(), "no hang should be reported");
    }

    #[test]
    fn worker_slot_idles_on_drop() {
        let monitor = TaskMonitor::builder().build();
        let slot = monitor.register_worker();
        slot.task_started();

        let raw = {
            let slots = monitor.inner.slots.lock().unwrap();
            Arc::clone(&slots[0])
        };
        assert_ne!(raw.load(Ordering::Acquire), 0, "slot should be non-zero while running");

        drop(slot);
        assert_eq!(raw.load(Ordering::Acquire), 0, "slot should be zero after drop");
    }

    #[test]
    fn multiple_workers_tracked_independently() {
        let hangs: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let h = Arc::clone(&hangs);
        let monitor = TaskMonitor::builder()
            .hang_threshold(Duration::from_millis(30))
            .watchdog_interval(Duration::from_millis(10))
            .on_hang(move |info| h.lock().unwrap().push(info.worker_id))
            .build();

        let slot0 = monitor.register_worker(); // worker 0: will hang
        let slot1 = monitor.register_worker(); // worker 1: finishes quickly

        slot0.task_started();
        slot1.task_started();
        thread::sleep(Duration::from_millis(5));
        slot1.task_finished(); // worker 1 done quickly

        thread::sleep(Duration::from_millis(80)); // worker 0 still running
        slot0.task_finished();

        thread::sleep(Duration::from_millis(30));
        let got = hangs.lock().unwrap();
        assert!(got.contains(&0), "worker 0 should be detected as hung");
        assert!(!got.contains(&1), "worker 1 should not be hung");
    }

    #[test]
    fn no_watchdog_without_hang_threshold() {
        // If hang_threshold is not set, the watchdog thread should NOT be started.
        let monitor = TaskMonitor::builder()
            .on_metrics(|_| {})
            .build();
        assert!(monitor.watchdog.lock().unwrap().is_none());
    }

    #[test]
    fn wrap_task_does_not_report_when_no_on_metrics() {
        // wrap_task should work even without an on_metrics callback.
        let monitor = TaskMonitor::builder().build();
        let wrapped = monitor.wrap_task(Box::new(|| {}));
        wrapped(); // must not panic
    }

    #[test]
    fn metrics_barrier_integration() {
        // Verify wrap_task works when the task itself synchronises threads.
        let metrics: Arc<Mutex<Vec<TaskMetrics>>> = Arc::new(Mutex::new(Vec::new()));
        let m = Arc::clone(&metrics);
        let monitor = TaskMonitor::builder()
            .on_metrics(move |met| m.lock().unwrap().push(met.clone()))
            .build();

        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let wrapped = monitor.wrap_task(Box::new(move || { b.wait(); }));

        let done = Arc::new(Barrier::new(2));
        let d = Arc::clone(&done);
        thread::spawn(move || {
            wrapped();
            d.wait();
        });

        barrier.wait();
        done.wait();

        assert_eq!(metrics.lock().unwrap().len(), 1);
    }
}
