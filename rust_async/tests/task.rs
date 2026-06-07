//! Stage 3: `sleep`, `timeout`, `yield_now`, `spawn_blocking`.

use std::time::{Duration, Instant};

use rust_async::{block_on, sleep, spawn, spawn_blocking, timeout, yield_now};

#[test]
fn sleep_elapses() {
    let start = Instant::now();
    block_on(sleep(Duration::from_millis(80)));
    assert!(start.elapsed() >= Duration::from_millis(70));
}

#[test]
fn timeout_returns_ok_when_future_finishes_first() {
    let v = block_on(timeout(Duration::from_secs(5), async { 7 }));
    assert_eq!(v.unwrap(), 7);
}

#[test]
fn timeout_errors_when_deadline_passes() {
    let r = block_on(timeout(Duration::from_millis(30), sleep(Duration::from_secs(5))));
    assert!(r.is_err());
}

#[test]
fn spawn_blocking_runs_off_executor_and_returns_value() {
    let sum = block_on(async { spawn_blocking(|| (1..=10).sum::<i32>()).await });
    assert_eq!(sum, 55);
}

#[test]
fn yield_now_resumes() {
    let v = block_on(async {
        yield_now().await;
        let h = spawn(async {
            yield_now().await;
            41
        });
        h.await + 1
    });
    assert_eq!(v, 42);
}
