//! Sequenced executor: one sequence is ordered & non-concurrent; distinct
//! sequences run in parallel on the shared pool.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rust_async::block_on;
use rust_async::sequenced::Sequence;

#[test]
fn one_sequence_never_runs_tasks_concurrently() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let seq = Sequence::new();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let active = Arc::clone(&active);
            let max_seen = Arc::clone(&max_seen);
            handles.push(seq.spawn(async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(5));
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await;
        }
    });

    assert_eq!(
        max_seen.load(Ordering::SeqCst),
        1,
        "tasks on a single sequence overlapped — the sequence is not serializing"
    );
}

#[test]
fn distinct_sequences_run_in_parallel() {
    let start = Instant::now();
    block_on(async {
        let mut handles = Vec::new();
        for _ in 0..4 {
            // A separate sequence each: free to run on different pool workers.
            let seq = Sequence::new();
            handles.push(seq.spawn(async { thread::sleep(Duration::from_millis(50)) }));
        }
        for h in handles {
            h.await;
        }
    });

    assert!(
        start.elapsed() < Duration::from_millis(150),
        "4 sequences x 50ms took {:?}; distinct sequences should overlap on the pool",
        start.elapsed()
    );
}

#[test]
fn sequence_preserves_fifo_order() {
    let order = block_on(async {
        let seq = Sequence::new();
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for i in 0..6 {
            let log = Arc::clone(&log);
            handles.push(seq.spawn(async move { log.lock().unwrap().push(i) }));
        }
        for h in handles {
            h.await;
        }
        Arc::try_unwrap(log).unwrap().into_inner().unwrap()
    });
    assert_eq!(order, vec![0, 1, 2, 3, 4, 5], "sequence did not run tasks FIFO");
}
