//! The single-threaded (reactor-lane) executor, and `offload` heavy work.

use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use rust_async::{current_thread, offload, sleep};

#[test]
fn run_returns_root_output() {
    let out = current_thread::run(async { 21 * 2 });
    assert_eq!(out, 42);
}

#[test]
fn run_drives_spawned_tasks_on_the_lane() {
    let total = current_thread::run(async {
        let a = current_thread::spawn(async { 1 + 2 });
        let b = current_thread::spawn(async { 3 + 4 });
        a.await + b.await
    });
    assert_eq!(total, 10);
}

#[test]
fn everything_runs_on_a_single_thread() {
    // The root, both spawned tasks, all observe the same thread id — proof that
    // the executor and reactor share one lane.
    let ids: Vec<ThreadId> = current_thread::run(async {
        let root = thread::current().id();
        let a = current_thread::spawn(async { thread::current().id() }).await;
        let b = current_thread::spawn(async { thread::current().id() }).await;
        vec![root, a, b]
    });
    assert!(ids.iter().all(|id| *id == ids[0]), "expected one lane, got {ids:?}");
}

#[test]
fn reactor_timer_works_on_the_lane() {
    // sleep() is driven by the same IO thread that runs the executor.
    let start = Instant::now();
    current_thread::run(async {
        sleep(Duration::from_millis(30)).await;
    });
    assert!(start.elapsed() >= Duration::from_millis(30));
}

#[test]
fn offload_runs_off_lane_and_resumes_on_lane() {
    let (lane_before, work_id, lane_after): (ThreadId, ThreadId, ThreadId) =
        current_thread::run(async {
            let lane_before = thread::current().id();
            let work_id = offload(|| thread::current().id()).await;
            let lane_after = thread::current().id();
            (lane_before, work_id, lane_after)
        });

    // Heavy work ran on a different thread (the parallel pool)...
    assert_ne!(lane_before, work_id, "offload should run off the reactor lane");
    // ...but execution resumed back on the original reactor lane.
    assert_eq!(lane_before, lane_after, "should resume on the same lane");
}

#[test]
fn offload_computes_correctly() {
    let sum = current_thread::run(async { offload(|| (0u64..1_000_000).sum::<u64>()).await });
    assert_eq!(sum, 499_999_500_000);
}
