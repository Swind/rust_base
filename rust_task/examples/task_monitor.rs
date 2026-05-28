//! TaskMonitor demo — queue/execution timing and hang detection.
//!
//! Two demos:
//!
//!  1. **Metrics** — posts tasks to a ThreadPool and prints queue time and
//!     execution time for each one via `on_metrics`.
//!  2. **Hang detection** — registers a worker slot manually and simulates a
//!     long-running task so the watchdog fires `on_hang`.
//!
//! Run with:
//!   cargo run --example task_monitor

use rust_task::{HangInfo, TaskMetrics, TaskMonitor, TaskTraits, ThreadPool};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

fn main() {
    demo_metrics();
    demo_hang_detection();
}

// ── Demo 1: queue time + execution time ──────────────────────────────────────

fn demo_metrics() {
    println!("=== Demo 1: Metrics ===");

    let log: Arc<Mutex<Vec<TaskMetrics>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = Arc::clone(&log);

    let monitor = TaskMonitor::builder()
        .on_metrics(move |m| {
            println!(
                "  task done — queue={:?}  exec={:?}",
                m.queue_time, m.execution_time
            );
            log_clone.lock().unwrap().push(m.clone());
        })
        .build();

    // 4 workers so all 3 tasks can run in parallel and the Barrier releases cleanly.
    let pool = ThreadPool::new_with_monitor(4, Arc::clone(&monitor));

    let barrier = Arc::new(Barrier::new(4)); // 3 tasks + main thread

    for ms in [5u64, 10, 15] {
        let b = Arc::clone(&barrier);
        pool.post_task(
            TaskTraits::default(),
            Box::new(move || {
                std::thread::sleep(Duration::from_millis(ms));
                b.wait();
            }),
        );
    }

    barrier.wait();
    pool.shutdown();
    drop(monitor);

    let captured = log.lock().unwrap();
    assert_eq!(captured.len(), 3, "expected 3 metric reports");
    println!("  all {} tasks reported metrics\n", captured.len());
}

// ── Demo 2: hang detection ────────────────────────────────────────────────────

fn demo_hang_detection() {
    println!("=== Demo 2: Hang detection ===");

    let hangs: Arc<Mutex<Vec<HangInfo>>> = Arc::new(Mutex::new(Vec::new()));
    let hangs_clone = Arc::clone(&hangs);

    let monitor = TaskMonitor::builder()
        .hang_threshold(Duration::from_millis(50))
        .watchdog_interval(Duration::from_millis(20))
        .on_hang(move |h| {
            println!(
                "  HANG: worker {} stuck for {:?}",
                h.worker_id, h.stuck_duration
            );
            hangs_clone.lock().unwrap().push(h.clone());
        })
        .build();

    let slot = monitor.register_worker();
    slot.task_started();

    std::thread::sleep(Duration::from_millis(150)); // well over 50 ms threshold

    slot.task_finished();

    std::thread::sleep(Duration::from_millis(30));
    drop(monitor);

    let got = hangs.lock().unwrap();
    assert!(!got.is_empty(), "hang should have been detected");
    println!("  watchdog fired {} time(s) for worker {}\n", got.len(), got[0].worker_id);
}
