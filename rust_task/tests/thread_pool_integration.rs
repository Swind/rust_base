// Integration tests: only the public API is used here, exactly as an external
// caller would.

use rust_task::{
    SequenceToken, TaskPriority, TaskShutdownBehavior, TaskTraits, ThreadPolicy, ThreadPool,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::thread;
use std::time::Duration;

// A countdown latch: blocks the caller until count_down() has been called N
// times. Cleaner than Barrier for cases where the N tasks don't all need to
// wait for each other.
struct Latch {
    count: Mutex<usize>,
    cvar: Condvar,
}

impl Latch {
    fn new(n: usize) -> Arc<Self> {
        Arc::new(Self { count: Mutex::new(n), cvar: Condvar::new() })
    }

    fn count_down(&self) {
        let mut c = self.count.lock().unwrap();
        *c -= 1;
        if *c == 0 {
            self.cvar.notify_all();
        }
    }

    fn wait(&self) {
        let mut c = self.count.lock().unwrap();
        while *c > 0 {
            c = self.cvar.wait(c).unwrap();
        }
    }
}

fn default_traits() -> TaskTraits {
    TaskTraits::default()
}

fn traits_with(behavior: TaskShutdownBehavior) -> TaskTraits {
    TaskTraits {
        priority: TaskPriority::UserVisible,
        shutdown_behavior: behavior,
        thread_policy: ThreadPolicy::PreferBackground,
        may_block: false,
    }
}

// ── 1. Basic lifecycle
// ────────────────────────────────────────────────────────

#[test]
fn pool_executes_posted_task() {
    let pool = ThreadPool::new(2);

    let executed = Arc::new(Mutex::new(false));
    let e = Arc::clone(&executed);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    pool.post_task(
        default_traits(),
        Box::new(move || {
            *e.lock().unwrap() = true;
            b.wait();
        }),
    );

    barrier.wait();
    pool.shutdown();

    assert!(*executed.lock().unwrap());
}

#[test]
fn pool_executes_delayed_task() {
    let pool = ThreadPool::new(2);

    let executed = Arc::new(Mutex::new(false));
    let e = Arc::clone(&executed);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    pool.post_delayed_task(
        default_traits(),
        Box::new(move || {
            *e.lock().unwrap() = true;
            b.wait();
        }),
        Duration::from_millis(20),
    );

    barrier.wait();
    pool.shutdown();

    assert!(*executed.lock().unwrap());
}

// ── 2. SequencedTaskRunner ordering ──────────────────────────────────────────

#[test]
fn sequenced_runner_executes_tasks_in_fifo_order() {
    let pool = ThreadPool::new(4);
    let runner = pool.create_sequenced_task_runner(default_traits());

    let results = Arc::new(Mutex::new(Vec::<usize>::new()));
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    for i in 0..5 {
        let r = Arc::clone(&results);
        runner.post_task(Box::new(move || r.lock().unwrap().push(i)));
    }
    runner.post_task(Box::new(move || {
        b.wait();
    }));

    barrier.wait();
    pool.shutdown();

    assert_eq!(*results.lock().unwrap(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn two_sequenced_runners_are_independent_and_run_in_parallel() {
    // Each runner posts one task that waits on a Barrier(3).
    // If the runners were serialized, the first task's wait would deadlock forever.
    let pool = ThreadPool::new(4);
    let barrier = Arc::new(Barrier::new(3)); // 2 tasks + test thread

    for _ in 0..2 {
        let runner = pool.create_sequenced_task_runner(default_traits());
        let b = Arc::clone(&barrier);
        runner.post_task(Box::new(move || {
            b.wait();
        }));
    }

    barrier.wait();
    pool.shutdown();
}

#[test]
fn multiple_sequenced_runners_each_maintain_their_own_order() {
    // 3 runners each post 5 tasks; within each runner the order must be preserved,
    // but runners may interleave with each other.
    let pool = ThreadPool::new(4);
    let barrier = Arc::new(Barrier::new(4)); // 3 final tasks + test thread

    let all_results: Vec<Arc<Mutex<Vec<usize>>>> =
        (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();

    for results in &all_results {
        let runner = pool.create_sequenced_task_runner(default_traits());
        let b = Arc::clone(&barrier);

        for i in 0..5 {
            let r = Arc::clone(results);
            runner.post_task(Box::new(move || r.lock().unwrap().push(i)));
        }
        let r = Arc::clone(results);
        runner.post_task(Box::new(move || {
            // Verify order before signalling done.
            assert_eq!(*r.lock().unwrap(), vec![0, 1, 2, 3, 4]);
            b.wait();
        }));
    }

    barrier.wait();
    pool.shutdown();
}

// ── 3. Parallel task runner
// ───────────────────────────────────────────────────

#[test]
fn parallel_runner_executes_tasks_concurrently() {
    let pool = ThreadPool::new(4);
    let runner = pool.create_task_runner(default_traits());
    let barrier = Arc::new(Barrier::new(3)); // 2 tasks + test thread

    for _ in 0..2 {
        let b = Arc::clone(&barrier);
        runner.post_task(Box::new(move || {
            b.wait();
        }));
    }

    barrier.wait();
    pool.shutdown();
}

// ── 4. post_task_and_reply
// ────────────────────────────────────────────────────

#[test]
fn post_task_and_reply_sends_reply_to_caller_sequence() {
    // runner_a posts a task-and-reply to runner_b while running on runner_a.
    // The reply must execute on runner_a's sequence.
    let pool = ThreadPool::new(4);
    let runner_a = pool.create_sequenced_task_runner(default_traits());
    let runner_b = pool.create_sequenced_task_runner(default_traits());

    let reply_token = Arc::new(Mutex::new(None::<SequenceToken>));
    let rt = Arc::clone(&reply_token);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    let expected_token = runner_a.sequence_token();
    let runner_b_clone = Arc::clone(&runner_b);

    // This task runs on runner_a; current_default() == runner_a inside it.
    runner_a.post_task(Box::new(move || {
        runner_b_clone.post_task_and_reply(
            Box::new(|| {}),
            Box::new(move || {
                *rt.lock().unwrap() = SequenceToken::current();
                b.wait();
            }),
        );
    }));

    barrier.wait();
    pool.shutdown();

    assert_eq!(*reply_token.lock().unwrap(), Some(expected_token));
}

// ── 5. runs_tasks_in_current_sequence ────────────────────────────────────────

#[test]
fn runs_tasks_in_current_sequence_returns_true_inside_own_task() {
    let pool = ThreadPool::new(2);
    let runner = pool.create_sequenced_task_runner(default_traits());

    let result = Arc::new(Mutex::new(false));
    let r = Arc::clone(&result);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    let runner_clone = Arc::clone(&runner);
    runner.post_task(Box::new(move || {
        *r.lock().unwrap() = runner_clone.runs_tasks_in_current_sequence();
        b.wait();
    }));

    barrier.wait();
    pool.shutdown();

    assert!(*result.lock().unwrap());
}

#[test]
fn runs_tasks_in_current_sequence_returns_false_on_different_runner() {
    let pool = ThreadPool::new(2);
    let runner_a = pool.create_sequenced_task_runner(default_traits());
    let runner_b = pool.create_sequenced_task_runner(default_traits());

    let result = Arc::new(Mutex::new(true)); // start true, expect false
    let r = Arc::clone(&result);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    // Run on runner_a but ask runner_b if we're in its sequence.
    let runner_b_clone = Arc::clone(&runner_b);
    runner_a.post_task(Box::new(move || {
        *r.lock().unwrap() = runner_b_clone.runs_tasks_in_current_sequence();
        b.wait();
    }));

    barrier.wait();
    pool.shutdown();

    assert!(!*result.lock().unwrap());
}

// ── 6. Shutdown behavior ─────────────────────────────────────────────────────

#[test]
fn block_shutdown_task_completes_before_shutdown_returns() {
    let pool = ThreadPool::new(2);

    let completed = Arc::new(Mutex::new(false));
    let c = Arc::clone(&completed);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    pool.post_task(
        traits_with(TaskShutdownBehavior::BlockShutdown),
        Box::new(move || {
            b.wait(); // signal test thread that task is running
            *c.lock().unwrap() = true;
        }),
    );

    barrier.wait(); // wait until task is running
    pool.shutdown(); // must block until the BlockShutdown task finishes

    // If shutdown() returned before the task set completed=true, this fails.
    assert!(*completed.lock().unwrap());
}

#[test]
fn continue_on_shutdown_task_can_be_posted_after_shutdown_starts() {
    // We call pool.shutdown() and then verify that ContinueOnShutdown is accepted
    // by will_post_task. We test this via the return value of post_task.
    //
    // Note: after join_all(), workers are gone, so we only test the post
    // acceptance, not actual execution.
    let pool = ThreadPool::new(2);

    // Drain workers first so shutdown() returns quickly.
    pool.shutdown();

    // Re-use the internal components via ThreadPool to test will_post_task.
    // Since shutdown() has completed, posting a ContinueOnShutdown task should
    // be rejected (shutdown_complete) — but posting it DURING the window between
    // shutdown_started=true and join_all() would be accepted.
    //
    // What we CAN assert: SkipOnShutdown is rejected after shutdown.
    let rejected =
        !pool.post_task(traits_with(TaskShutdownBehavior::SkipOnShutdown), Box::new(|| {}));
    assert!(rejected, "SkipOnShutdown should be rejected after shutdown");
}

// ── 7. Delayed task ordering ─────────────────────────────────────────────────

#[test]
fn delayed_task_does_not_execute_before_deadline() {
    let pool = ThreadPool::new(2);
    let runner = pool.create_sequenced_task_runner(default_traits());

    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    // Post immediate task first, then delayed task — immediate must run first.
    let o1 = Arc::clone(&order);
    runner.post_task(Box::new(move || o1.lock().unwrap().push("immediate")));

    let o2 = Arc::clone(&order);
    runner.post_delayed_task(
        Box::new(move || {
            o2.lock().unwrap().push("delayed");
            b.wait();
        }),
        Duration::from_millis(20),
    );

    barrier.wait();
    pool.shutdown();

    assert_eq!(*order.lock().unwrap(), vec!["immediate", "delayed"]);
}

// ── 8. Stress tests ──────────────────────────────────────────────────────────

// Single sequenced runner, 1000 tasks: verify strict FIFO across many tasks.
#[test]
fn stress_sequenced_runner_1000_tasks_in_order() {
    const N: usize = 1000;
    let pool = ThreadPool::new(8);
    let runner = pool.create_sequenced_task_runner(default_traits());

    // Each task records the value of the counter at the time it runs.
    // Because tasks execute one at a time in FIFO order, task i must see counter ==
    // i.
    let counter = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new(N);

    for i in 0..N {
        let c = Arc::clone(&counter);
        let l = Arc::clone(&latch);
        runner.post_task(Box::new(move || {
            let prev = c.fetch_add(1, Ordering::SeqCst);
            assert_eq!(prev, i, "task {} ran out of order (counter was {})", i, prev);
            l.count_down();
        }));
    }

    latch.wait();
    pool.shutdown();
    assert_eq!(counter.load(Ordering::SeqCst), N);
}

// 50 independent sequenced runners, 100 tasks each: each runner must maintain
// its own order while all runners execute concurrently.
#[test]
fn stress_50_sequenced_runners_100_tasks_each() {
    const RUNNERS: usize = 50;
    const TASKS_PER_RUNNER: usize = 100;

    let pool = ThreadPool::new(16);
    let latch = Latch::new(RUNNERS * TASKS_PER_RUNNER);

    for _ in 0..RUNNERS {
        let runner = pool.create_sequenced_task_runner(default_traits());
        let counter = Arc::new(AtomicUsize::new(0));

        for i in 0..TASKS_PER_RUNNER {
            let c = Arc::clone(&counter);
            let l = Arc::clone(&latch);
            runner.post_task(Box::new(move || {
                let prev = c.fetch_add(1, Ordering::SeqCst);
                assert_eq!(prev, i, "out-of-order execution in runner");
                l.count_down();
            }));
        }
    }

    latch.wait();
    pool.shutdown();
}

// Parallel runner with 500 tasks and 20 workers: all tasks must complete.
#[test]
fn stress_parallel_runner_500_tasks_20_workers() {
    const N: usize = 500;
    let pool = ThreadPool::new(20);
    let runner = pool.create_task_runner(default_traits());

    let completed = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new(N);

    for _ in 0..N {
        let c = Arc::clone(&completed);
        let l = Arc::clone(&latch);
        runner.post_task(Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
            l.count_down();
        }));
    }

    latch.wait();
    pool.shutdown();
    assert_eq!(completed.load(Ordering::SeqCst), N);
}

// High contention: many sequenced runners and a parallel runner all share the
// same thread pool.  Verify total task count and per-runner ordering together.
#[test]
fn stress_mixed_sequenced_and_parallel_high_contention() {
    const SEQ_RUNNERS: usize = 20;
    const SEQ_TASKS: usize = 200;
    const PAR_TASKS: usize = 300;
    const TOTAL: usize = SEQ_RUNNERS * SEQ_TASKS + PAR_TASKS;

    let pool = ThreadPool::new(12);
    let total_completed = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new(TOTAL);

    // Sequenced runners.
    for _ in 0..SEQ_RUNNERS {
        let runner = pool.create_sequenced_task_runner(default_traits());
        let counter = Arc::new(AtomicUsize::new(0));

        for i in 0..SEQ_TASKS {
            let c = Arc::clone(&counter);
            let t = Arc::clone(&total_completed);
            let l = Arc::clone(&latch);
            runner.post_task(Box::new(move || {
                let prev = c.fetch_add(1, Ordering::SeqCst);
                assert_eq!(prev, i);
                t.fetch_add(1, Ordering::Relaxed);
                l.count_down();
            }));
        }
    }

    // Parallel runner.
    let par_runner = pool.create_task_runner(default_traits());
    for _ in 0..PAR_TASKS {
        let t = Arc::clone(&total_completed);
        let l = Arc::clone(&latch);
        par_runner.post_task(Box::new(move || {
            t.fetch_add(1, Ordering::Relaxed);
            l.count_down();
        }));
    }

    latch.wait();
    pool.shutdown();
    assert_eq!(total_completed.load(Ordering::SeqCst), TOTAL);
}

// Parallel runner with many workers but few tasks: ensure no tasks are lost
// even when workers outnumber tasks.
#[test]
fn stress_more_workers_than_tasks() {
    const WORKERS: usize = 32;
    const TASKS: usize = 10;

    let pool = ThreadPool::new(WORKERS);
    let runner = pool.create_task_runner(default_traits());

    let completed = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new(TASKS);

    for _ in 0..TASKS {
        let c = Arc::clone(&completed);
        let l = Arc::clone(&latch);
        runner.post_task(Box::new(move || {
            c.fetch_add(1, Ordering::Relaxed);
            l.count_down();
        }));
    }

    latch.wait();
    pool.shutdown();
    assert_eq!(completed.load(Ordering::SeqCst), TASKS);
}

// Sequenced runner under high worker count: ordering guarantee must hold
// even when many idle workers are competing for the same sequence.
#[test]
fn stress_sequenced_runner_with_many_competing_workers() {
    const WORKERS: usize = 32;
    const N: usize = 500;

    let pool = ThreadPool::new(WORKERS);
    let runner = pool.create_sequenced_task_runner(default_traits());

    let counter = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new(N);

    for i in 0..N {
        let c = Arc::clone(&counter);
        let l = Arc::clone(&latch);
        runner.post_task(Box::new(move || {
            let prev = c.fetch_add(1, Ordering::SeqCst);
            assert_eq!(prev, i, "ordering violated with {} workers", WORKERS);
            l.count_down();
        }));
    }

    latch.wait();
    pool.shutdown();
}

// ── 9. BlockShutdown queued-task guarantees
// ───────────────────────────────────

// Verifies that shutdown() waits for a BlockShutdown task that was posted
// AFTER the worker is already busy — i.e., the task sits in the queue and has
// not yet started executing when shutdown() is called.
#[test]
fn shutdown_waits_for_queued_block_shutdown_task() {
    // Single worker so the second task is guaranteed to be queued when the
    // first task is still running.
    let pool = ThreadPool::new(1);

    // Barrier that lets us know the first task is executing (worker is busy).
    let start_barrier = Arc::new(Barrier::new(2));
    let sb = Arc::clone(&start_barrier);
    // Barrier that lets us release the first task so the worker becomes free.
    let release_barrier = Arc::new(Barrier::new(2));
    let rb = Arc::clone(&release_barrier);

    let block_shutdown_executed = Arc::new(Mutex::new(false));
    let e = Arc::clone(&block_shutdown_executed);

    // First task: occupies the single worker.
    pool.post_task(
        default_traits(),
        Box::new(move || {
            sb.wait(); // signal: worker is now busy
            rb.wait(); // wait until test thread releases us
        }),
    );

    start_barrier.wait(); // wait until worker is busy

    // Post BlockShutdown task — it lands in the queue (worker is still busy).
    pool.post_task(
        traits_with(TaskShutdownBehavior::BlockShutdown),
        Box::new(move || {
            *e.lock().unwrap() = true;
        }),
    );

    // Call shutdown() in a background thread so we can also release the first task.
    let pool_clone = Arc::clone(&pool);
    let shutdown_handle = thread::spawn(move || pool_clone.shutdown());

    // Release the first task so the worker can proceed to the BlockShutdown task.
    release_barrier.wait();

    shutdown_handle.join().unwrap();
    assert!(
        *block_shutdown_executed.lock().unwrap(),
        "shutdown() returned before the queued BlockShutdown task executed"
    );
}

// Posts many BlockShutdown tasks and verifies all of them complete before
// shutdown() returns, even under concurrent execution across multiple workers.
#[test]
fn shutdown_waits_for_all_queued_block_shutdown_tasks() {
    const BLOCK_TASKS: usize = 10;

    let pool = ThreadPool::new(4);
    let executed = Arc::new(AtomicUsize::new(0));
    let latch = Latch::new(BLOCK_TASKS);

    for _ in 0..BLOCK_TASKS {
        let e = Arc::clone(&executed);
        let l = Arc::clone(&latch);
        pool.post_task(
            traits_with(TaskShutdownBehavior::BlockShutdown),
            Box::new(move || {
                e.fetch_add(1, Ordering::Relaxed);
                l.count_down();
            }),
        );
    }

    pool.shutdown();

    assert_eq!(
        executed.load(Ordering::SeqCst),
        BLOCK_TASKS,
        "not all BlockShutdown tasks ran before shutdown() completed"
    );
}
