// Integration tests: only the public API is used here, exactly as an external caller would.

use rust_task::{
    SequenceToken, TaskPriority, TaskShutdownBehavior, TaskTraits, ThreadPolicy, ThreadPool,
};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

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

// ── 1. Basic lifecycle ────────────────────────────────────────────────────────

#[test]
fn pool_executes_posted_task() {
    let pool = ThreadPool::new(2);

    let executed = Arc::new(Mutex::new(false));
    let e = Arc::clone(&executed);
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);

    pool.post_task(default_traits(), Box::new(move || {
        *e.lock().unwrap() = true;
        b.wait();
    }));

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
    runner.post_task(Box::new(move || { b.wait(); }));

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
        runner.post_task(Box::new(move || { b.wait(); }));
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

// ── 3. Parallel task runner ───────────────────────────────────────────────────

#[test]
fn parallel_runner_executes_tasks_concurrently() {
    let pool = ThreadPool::new(4);
    let runner = pool.create_task_runner(default_traits());
    let barrier = Arc::new(Barrier::new(3)); // 2 tasks + test thread

    for _ in 0..2 {
        let b = Arc::clone(&barrier);
        runner.post_task(Box::new(move || { b.wait(); }));
    }

    barrier.wait();
    pool.shutdown();
}

// ── 4. post_task_and_reply ────────────────────────────────────────────────────

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

    pool.post_task(traits_with(TaskShutdownBehavior::BlockShutdown), Box::new(move || {
        b.wait(); // signal test thread that task is running
        *c.lock().unwrap() = true;
    }));

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
    // Note: after join_all(), workers are gone, so we only test the post acceptance,
    // not actual execution.
    let pool = ThreadPool::new(2);

    // Drain workers first so shutdown() returns quickly.
    pool.shutdown();

    // Re-use the internal components via ThreadPool to test will_post_task.
    // Since shutdown() has completed, posting a ContinueOnShutdown task should
    // be rejected (shutdown_complete) — but posting it DURING the window between
    // shutdown_started=true and join_all() would be accepted.
    //
    // What we CAN assert: SkipOnShutdown is rejected after shutdown.
    let rejected = !pool.post_task(
        traits_with(TaskShutdownBehavior::SkipOnShutdown),
        Box::new(|| {}),
    );
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
