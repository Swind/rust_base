# rust_task

A Rust port of Chromium's `base::task` threading system. Cross-platform, no unsafe dependencies beyond `std`.

## Architecture

```
ThreadPool  (public entry point)
  ├── TaskTracker          shutdown lifecycle; BlockShutdown counting
  ├── DelayedTaskManager   timer thread; min-heap of (deadline, Sequence)
  └── ThreadGroup          OS worker threads + priority queue of TaskSources
        └── Sequence       per-runner task queue (immediate + delayed, one lock)
```

`ThreadPool` owns the worker threads and exposes factory methods for task runners. All internals are hidden behind the public API.

## Features

- **`ThreadPool`** — fixed-size worker thread pool
- **`SequencedTaskRunner`** — FIFO-ordered execution; tasks on the same runner never run concurrently
- **`TaskRunner`** (parallel) — tasks may execute concurrently
- **Delayed tasks** — `post_delayed_task` via a dedicated timer thread
- **`post_task_and_reply`** — posts a task then replies back to the caller's sequence
- **`TaskTraits`** — priority, shutdown behavior, thread policy per task
- **`TaskMonitor`** — queue-time + execution-time metrics and hang detection
- **`bind_once`** — binds `Arc<T>` or `Weak<T>` to a callback; `Weak` silently no-ops if dropped

### Shutdown behaviors

| `TaskShutdownBehavior` | After `shutdown()` is called |
|------------------------|------------------------------|
| `SkipOnShutdown`       | Pending tasks are dropped; new posts rejected |
| `ContinueOnShutdown`   | New posts accepted; tasks may still run |
| `BlockShutdown`        | `shutdown()` blocks until all such tasks finish |

## Usage

```rust
use rust_task::{ThreadPool, TaskTraits};

let pool = ThreadPool::new(4);

// Fire-and-forget
pool.post_task(TaskTraits::default(), Box::new(|| println!("hello")));

// Sequenced runner — guaranteed FIFO
let runner = pool.create_sequenced_task_runner(TaskTraits::default());
for i in 0..5 {
    runner.post_task(Box::new(move || println!("{i}")));
}

pool.shutdown();
```

```rust
// TaskMonitor — queue/execution timing + hang detection
use rust_task::{TaskMonitor, ThreadPool};
use std::time::Duration;

let monitor = TaskMonitor::builder()
    .on_metrics(|m| println!("queue={:?}  exec={:?}", m.queue_time, m.execution_time))
    .hang_threshold(Duration::from_secs(5))
    .on_hang(|h| eprintln!("worker {} stuck for {:?}", h.worker_id, h.stuck_duration))
    .build();

let pool = ThreadPool::new_with_monitor(4, Arc::clone(&monitor));
```

```rust
// bind_once — safe weak binding
use rust_task::{bind_once, ThreadPool, TaskTraits};

pool.post_task(
    TaskTraits::default(),
    bind_once(Arc::downgrade(&handler), |h| h.on_event()),
    // no-op if handler has been dropped
);
```

## Examples

```bash
cargo run --example event_bus       # event bus on SequencedTaskRunner
cargo run --example repeating_timer # repeating timer via post_delayed_task
cargo run --example task_monitor    # queue/execution metrics and hang detection
```
