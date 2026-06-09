//! The unified [`Runtime`]: one `(task runner, io task runner)` pair covers
//! every topology. Each helper below builds a different pairing; the tests show
//! that ordering, parallelism, reactor-arming, and runtime inheritance all
//! follow from the two arguments alone.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use rust_async::{Runtime, block_on, offload, sleep, spawn};
use rust_io::IoTaskRunner;
use rust_task::{TaskTraits, ThreadPool};

/// One fused lane: a single `IoTaskRunner` is *both* the executor (where tasks
/// are polled) and the reactor (where their I/O is armed).
fn fused() -> Runtime {
    let io = IoTaskRunner::new();
    Runtime::new(io.clone(), io)
}

/// A parallel `ThreadPool` executor paired with its own dedicated reactor.
fn parallel(threads: usize) -> Runtime {
    let pool = ThreadPool::new(threads);
    Runtime::new(pool.create_task_runner(TaskTraits::default()), IoTaskRunner::new())
}

/// One ordered `SequencedTaskRunner` on `pool`, paired with its own reactor.
fn sequenced(pool: &Arc<ThreadPool>) -> Runtime {
    Runtime::new(pool.create_sequenced_task_runner(TaskTraits::default()), IoTaskRunner::new())
}

#[test]
fn fused_runs_on_one_thread_and_inherits() {
    // Root + two nested spawns all observe the same thread id: the executor and
    // reactor share one lane, and the nested `spawn`s inherited the runtime
    // (otherwise they'd land on the global pool, a different thread).
    let ids: Vec<ThreadId> = block_on(fused().spawn(async {
        let root = thread::current().id();
        let a = spawn(async { thread::current().id() }).await;
        let b = spawn(async { thread::current().id() }).await;
        vec![root, a, b]
    }));
    assert!(ids.iter().all(|id| *id == ids[0]), "expected one lane, got {ids:?}");
}

#[test]
fn fused_reactor_timer_fires() {
    let start = Instant::now();
    block_on(fused().spawn(async {
        sleep(Duration::from_millis(30)).await;
    }));
    assert!(start.elapsed() >= Duration::from_millis(30));
}

#[test]
fn parallel_runtime_inherits_and_overlaps() {
    // Nested spawns inherit the 4-thread parallel runtime, so four 50ms blocking
    // sleeps overlap instead of serializing to ~200ms.
    let start = Instant::now();
    block_on(parallel(4).spawn(async {
        let handles: Vec<_> =
            (0..4).map(|_| spawn(async { thread::sleep(Duration::from_millis(50)) })).collect();
        for h in handles {
            h.await;
        }
    }));
    assert!(
        start.elapsed() < Duration::from_millis(150),
        "4 x 50ms took {:?}; parallel should overlap",
        start.elapsed()
    );
}

#[test]
fn sequenced_runtime_is_serial_and_ordered() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let a2 = Arc::clone(&active);
    let m2 = Arc::clone(&max_seen);

    let pool = ThreadPool::new(4);
    let rt = sequenced(&pool);

    let order: Vec<usize> = block_on(rt.spawn(async move {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for i in 0..8 {
            let active = Arc::clone(&a2);
            let max_seen = Arc::clone(&m2);
            let log = Arc::clone(&log);
            // Inherits the sequence: all eight run on the same ordered lane.
            handles.push(spawn(async move {
                let now = active.fetch_add(1, SeqCst) + 1;
                max_seen.fetch_max(now, SeqCst);
                thread::sleep(Duration::from_millis(3));
                active.fetch_sub(1, SeqCst);
                log.lock().unwrap().push(i);
            }));
        }
        for h in handles {
            h.await;
        }
        Arc::try_unwrap(log).unwrap().into_inner().unwrap()
    }));

    assert_eq!(max_seen.load(SeqCst), 1, "tasks on one sequence overlapped");
    assert_eq!(order, (0..8).collect::<Vec<_>>(), "sequence did not run FIFO");
}

#[test]
fn distinct_sequences_run_in_parallel() {
    let pool = ThreadPool::new(4);
    let a = sequenced(&pool);
    let b = sequenced(&pool);

    let start = Instant::now();
    let ha = a.spawn(async { thread::sleep(Duration::from_millis(50)) });
    let hb = b.spawn(async { thread::sleep(Duration::from_millis(50)) });
    block_on(async {
        ha.await;
        hb.await;
    });

    assert!(
        start.elapsed() < Duration::from_millis(150),
        "two sequences x 50ms took {:?}; distinct sequences should overlap",
        start.elapsed()
    );
}

#[test]
fn offload_resumes_on_the_runtime_lane() {
    let (before, work, after): (ThreadId, ThreadId, ThreadId) = block_on(fused().spawn(async {
        let before = thread::current().id();
        let work = offload(|| thread::current().id()).await;
        let after = thread::current().id();
        (before, work, after)
    }));

    assert_ne!(before, work, "offload should run off the runtime lane");
    assert_eq!(before, after, "execution should resume on the runtime lane");
}

#[test]
fn parallel_runtime_pairs_with_its_dedicated_reactor() {
    // The task runs on a pool worker (not an IO thread); its `sleep` is armed on
    // the runtime's dedicated reactor and still fires — the forward half of the
    // pairing working across a thread boundary.
    let start = Instant::now();
    block_on(parallel(2).spawn(async {
        sleep(Duration::from_millis(30)).await;
    }));
    assert!(start.elapsed() >= Duration::from_millis(30));
}
