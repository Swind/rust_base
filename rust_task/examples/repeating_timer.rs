//! Repeating-timer demo.
//!
//! Shows three usage patterns:
//!
//!  1. **Basic cronjob** — tick every 100 ms, stop after 5 ticks.
//!  2. **Self-stopping timer** — the callback itself calls `stop()` after 3
//!     ticks without any external coordination.
//!  3. **Lifetime-safe timer via `bind_repeating`** — the callback is bound to
//!     a `Weak<Handler>`, so dropping the handler immediately cancels the timer
//!     without an explicit `stop()` call.
//!
//! Run with:
//!   cargo run --example repeating_timer

use rust_task::{RepeatingTimer, TaskTraits, ThreadPool, bind_repeating};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

// ── 1. Basic cronjob
// ──────────────────────────────────────────────────────────

fn demo_basic_cronjob(pool: &Arc<ThreadPool>) {
    println!("=== 1. Basic cronjob (5 ticks @ 100 ms) ===");

    let runner = pool.create_sequenced_task_runner(TaskTraits::default());
    let timer = Arc::new(RepeatingTimer::new(runner));

    let count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let c = Arc::clone(&count);
    let b = Arc::clone(&barrier);
    timer.start(Duration::from_millis(100), move || {
        let n = c.fetch_add(1, Ordering::Relaxed) + 1;
        println!("  tick {n}");
        if n == 5 {
            b.wait(); // signal the main thread
        }
    });

    barrier.wait();
    timer.stop();

    println!("  stopped after {} ticks\n", count.load(Ordering::Relaxed));
}

// ── 2. Self-stopping timer
// ────────────────────────────────────────────────────

fn demo_self_stopping(pool: &Arc<ThreadPool>) {
    println!("=== 2. Self-stopping timer (stops itself after 3 ticks) ===");

    let runner = pool.create_sequenced_task_runner(TaskTraits::default());
    let timer = Arc::new(RepeatingTimer::new(runner));

    let count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let c = Arc::clone(&count);
    let b = Arc::clone(&barrier);
    let t = Arc::clone(&timer);
    timer.start(Duration::from_millis(80), move || {
        let n = c.fetch_add(1, Ordering::Relaxed) + 1;
        println!("  tick {n}");
        if n == 3 {
            t.stop(); // cancel from inside the callback
            b.wait();
        }
    });

    barrier.wait();
    println!("  timer is_running = {}\n", timer.is_running());
}

// ── 3. Lifetime-safe via bind_repeating ──────────────────────────────────────

struct MetricCollector {
    samples: Mutex<Vec<u64>>,
    next_value: AtomicUsize,
}

impl MetricCollector {
    fn new() -> Arc<Self> {
        Arc::new(Self { samples: Mutex::new(Vec::new()), next_value: AtomicUsize::new(1) })
    }

    fn collect(self: Arc<Self>) {
        let v = self.next_value.fetch_add(1, Ordering::Relaxed) as u64;
        self.samples.lock().unwrap().push(v);
        println!("  collected sample {v}");
    }
}

fn demo_bind_repeating_lifetime(pool: &Arc<ThreadPool>) {
    println!("=== 3. Lifetime-safe timer (drop collector → timer stops) ===");

    let runner = pool.create_sequenced_task_runner(TaskTraits::default());
    let timer = RepeatingTimer::new(runner);

    let collector = MetricCollector::new();

    // bind_repeating holds Weak<MetricCollector> — the timer does not extend
    // the collector's lifetime.
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);
    let sample_count = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&sample_count);
    let cb = bind_repeating(Arc::downgrade(&collector), move |c| {
        c.collect();
        let n = sc.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 3 {
            b.wait(); // let test thread know 3 samples are in
        }
    });

    timer.start(Duration::from_millis(80), move || cb());

    // Wait until 3 samples are collected.
    barrier.wait();

    // Drop the collector — no more samples should be collected.
    let weak = Arc::downgrade(&collector);
    drop(collector);

    // Arc freed immediately (timer holds only a Weak).
    assert!(weak.upgrade().is_none(), "collector should be freed immediately");
    println!("  collector freed immediately after drop ✓");

    // Give the timer a couple of intervals to confirm it's no longer firing.
    std::thread::sleep(Duration::from_millis(200));
    timer.stop();

    println!("  collected {} samples total\n", sample_count.load(Ordering::Relaxed));
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    let pool = ThreadPool::new(4);

    demo_basic_cronjob(&pool);
    demo_self_stopping(&pool);
    demo_bind_repeating_lifetime(&pool);

    pool.shutdown();
}
