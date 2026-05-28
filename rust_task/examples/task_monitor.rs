//! TaskMonitor demo — queue/execution timing and hang detection.
//!
//! Three demos:
//!
//!  1. **Metrics** — posts tasks to a ThreadPool and prints queue time and
//!     execution time for each one via `on_metrics`.
//!  2. **Hang detection** — registers a worker slot manually and simulates a
//!     long-running task so the watchdog fires `on_hang`.
//!  3. **IoTaskRunner integration** — wraps IO callbacks with the same monitor
//!     so IO wait time and callback duration are tracked alongside pool tasks.
//!
//! Run with:
//!   cargo run --example task_monitor

use rust_task::{HangInfo, TaskMetrics, TaskMonitor, TaskRunner, TaskTraits, ThreadPool};
#[cfg(target_os = "linux")]
use rust_io::IoTaskRunner;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

fn main() {
    demo_metrics();
    demo_hang_detection();
    demo_io_integration();
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

    // Register a slot manually to simulate a single long-running task.
    let slot = monitor.register_worker();
    slot.task_started();

    std::thread::sleep(Duration::from_millis(150)); // well over 50 ms threshold

    slot.task_finished();

    // Give the watchdog one more interval to settle before dropping the monitor.
    std::thread::sleep(Duration::from_millis(30));
    drop(monitor);

    let got = hangs.lock().unwrap();
    assert!(!got.is_empty(), "hang should have been detected");
    println!("  watchdog fired {} time(s) for worker {}\n", got.len(), got[0].worker_id);
}

// ── Demo 3: IoTaskRunner integration ─────────────────────────────────────────

#[cfg(target_os = "linux")]
fn demo_io_integration() {
    println!("=== Demo 3: IoTaskRunner + monitor ===");

    let log: Arc<Mutex<Vec<TaskMetrics>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = Arc::clone(&log);

    let monitor = TaskMonitor::builder()
        .on_metrics(move |m| {
            println!(
                "  IO task — queue={:?}  exec={:?}",
                m.queue_time, m.execution_time
            );
            log_clone.lock().unwrap().push(m.clone());
        })
        .build();

    let io = IoTaskRunner::new_with_monitor(Arc::clone(&monitor));
    let barrier = Arc::new(Barrier::new(2));

    let b = Arc::clone(&barrier);
    io.post_task(Box::new(move || {
        std::thread::sleep(Duration::from_millis(5));
        b.wait();
    }));

    barrier.wait();
    drop(monitor);
    drop(io);

    let captured = log.lock().unwrap();
    println!("  {} IO-runner task(s) reported metrics\n", captured.len());
}

#[cfg(not(target_os = "linux"))]
fn demo_io_integration() {
    println!("=== Demo 3: IoTaskRunner + monitor ===");
    println!("  (skipped — requires Linux)\n");
}
