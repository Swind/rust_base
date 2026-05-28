# rust_task

[![CI](https://github.com/Swind/rust-task-runner/actions/workflows/ci.yml/badge.svg)](https://github.com/Swind/rust-task-runner/actions/workflows/ci.yml)

An experimental Rust implementation of Chromium's threading and task system, based on the design described in [Threading and Tasks in Chrome](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md).

This is not a replacement for Chromium's `base::task` — it is a standalone Rust library that ports the core concepts for use in new Rust components.

---

## Architecture

```
ThreadPool  (public API)
  ├── TaskTracker          shutdown lifecycle; BlockShutdown counting
  ├── DelayedTaskManager   timer thread; fires delayed tasks at their deadline
  └── ThreadGroup          OS worker threads + priority queue of TaskSources
        └── Sequence       per-runner task queue (immediate + delayed, one lock)
```

**`ThreadPool`** is the single entry point. It owns the worker threads and exposes factory methods for creating task runners. All internal components (`ThreadGroup`, `Sequence`, etc.) are hidden behind the public API.

**`Sequence`** is the unit of ordering. A `SequencedTaskRunner` wraps a persistent `Sequence`; tasks posted to the same runner share one sequence and execute strictly in FIFO order. A one-shot parallel task gets its own temporary sequence.

**`TaskTracker`** enforces shutdown semantics. It rejects new tasks once shutdown has started (except `ContinueOnShutdown`) and blocks `shutdown()` until all `BlockShutdown` tasks complete.

---

## Features

- **`ThreadPool`** — fixed-size worker thread pool
- **`SequencedTaskRunner`** — FIFO-ordered task execution; tasks on the same runner never run concurrently
- **`TaskRunner`** (parallel) — tasks may execute concurrently on different workers
- **Delayed tasks** — `post_delayed_task` via a dedicated timer thread
- **`post_task_and_reply`** — posts a task, then automatically posts the reply back to the caller's sequence
- **`runs_tasks_in_current_sequence`** — lets a runner verify it is running on its own sequence
- **`TaskTraits`** — priority, shutdown behavior, thread policy per task
- **`bind_once`** — binds `Arc<T>` or `Weak<T>` to a callback; `Weak` variant silently skips if the object has been dropped
- **`IoTaskRunner`** — single-threaded event loop for async I/O via epoll (Linux); supports `FdWatcher` with one-shot and persistent watch modes
- **`SocketPosix`** — async TCP client with Chromium-style `Read` / `ReadIfReady` / `Write` / `Connect` callbacks
- **`TaskMonitor`** — queue-time + execution-time metrics and hang detection across `ThreadPool` and `IoTaskRunner`

### Shutdown behaviors

| `TaskShutdownBehavior` | Behavior after `shutdown()` is called |
|------------------------|--------------------------------------|
| `SkipOnShutdown`       | Pending tasks are dropped; new posts rejected |
| `ContinueOnShutdown`   | New posts accepted; tasks may still run |
| `BlockShutdown`        | `shutdown()` blocks until all such tasks finish |

### Not implemented

`SingleThreadTaskRunner`, `BrowserThread`, `MessagePump` / `RunLoop`, `PostJob`, `CancelableTaskTracker`, `UpdateableSequencedTaskRunner`, `SequenceLocalStorageSlot`.

---

## TaskMonitor

`TaskMonitor` measures queue time, execution time, and long-running tasks across a `ThreadPool` and/or `IoTaskRunner`.

```rust
use rust_task::{TaskMonitor, ThreadPool, IoTaskRunner};
use std::time::Duration;

let monitor = TaskMonitor::builder()
    // called after every task completes
    .on_metrics(|m| println!("queue={:?}  exec={:?}", m.queue_time, m.execution_time))
    // called (repeatedly) when a worker is stuck longer than the threshold
    .hang_threshold(Duration::from_secs(5))
    .watchdog_interval(Duration::from_secs(1))
    .on_hang(|h| eprintln!("worker {} stuck for {:?}", h.worker_id, h.stuck_duration))
    .build();

let pool = ThreadPool::new_with_monitor(4, Arc::clone(&monitor));
let io   = IoTaskRunner::new_with_monitor(Arc::clone(&monitor));
```

- **Queue time** is measured from the `post_task` call to the moment a worker picks it up.
- **Execution time** covers only the user callback itself.
- Tasks that are skipped at shutdown (i.e. `SkipOnShutdown` tasks dropped after `shutdown()`) never trigger `on_metrics`.
- Hang detection is opt-in: the watchdog thread is only started when `hang_threshold` is set.
- `ThreadPool::new` / `IoTaskRunner::new` take no monitor and have zero overhead.

---

## Usage

### Post a fire-and-forget task

```rust
use rust_task::{ThreadPool, TaskTraits};

let pool = ThreadPool::new(4);

pool.post_task(
    TaskTraits::default(),
    Box::new(|| println!("hello from a worker")),
);

pool.shutdown();
```

### SequencedTaskRunner — guaranteed FIFO order

```rust
use rust_task::{ThreadPool, TaskTraits};
use std::sync::{Arc, Mutex};

let pool = ThreadPool::new(4);
let runner = pool.create_sequenced_task_runner(TaskTraits::default());

let log = Arc::new(Mutex::new(Vec::new()));

for i in 0..5 {
    let log = Arc::clone(&log);
    runner.post_task(Box::new(move || log.lock().unwrap().push(i)));
}

pool.shutdown();
assert_eq!(*log.lock().unwrap(), vec![0, 1, 2, 3, 4]);
```

### Parallel TaskRunner — concurrent execution

```rust
use rust_task::{ThreadPool, TaskTraits};
use std::sync::{Arc, Barrier};

let pool = ThreadPool::new(4);
let runner = pool.create_task_runner(TaskTraits::default());
let barrier = Arc::new(Barrier::new(3)); // 2 tasks + test thread

for _ in 0..2 {
    let b = Arc::clone(&barrier);
    runner.post_task(Box::new(move || { b.wait(); }));
}

barrier.wait(); // both tasks are running concurrently
pool.shutdown();
```

### Delayed task

```rust
use rust_task::{ThreadPool, TaskTraits};
use std::time::Duration;

let pool = ThreadPool::new(2);

pool.post_delayed_task(
    TaskTraits::default(),
    Box::new(|| println!("runs after 100ms")),
    Duration::from_millis(100),
);

pool.shutdown();
```

### post_task_and_reply

```rust
use rust_task::{ThreadPool, TaskTraits};
use std::sync::{Arc, Mutex};

let pool = ThreadPool::new(4);
let caller = pool.create_sequenced_task_runner(TaskTraits::default());
let worker = pool.create_sequenced_task_runner(TaskTraits::default());

let result = Arc::new(Mutex::new(0u32));
let r = Arc::clone(&result);

// Must be called from within a sequence so the reply has somewhere to go.
let worker_clone = Arc::clone(&worker);
caller.post_task(Box::new(move || {
    worker_clone.post_task_and_reply(
        Box::new(|| { /* expensive work */ }),
        Box::new(move || { *r.lock().unwrap() = 42; }),
        // reply runs back on `caller`'s sequence
    );
}));

pool.shutdown();
```

### bind_once — safe weak binding

```rust
use rust_task::{ThreadPool, TaskTraits, bind_once};
use std::sync::Arc;

struct Handler;
impl Handler {
    fn on_event(&self) { println!("handled"); }
}

let pool = ThreadPool::new(2);
let handler = Arc::new(Handler);

// Weak: if handler is dropped before the task runs, the task is a no-op.
// The task queue does not extend handler's lifetime.
pool.post_task(
    TaskTraits::default(),
    bind_once(Arc::downgrade(&handler), |h| h.on_event()),
);

// Arc: always executes.
pool.post_task(
    TaskTraits::default(),
    bind_once(Arc::clone(&handler), |h| h.on_event()),
);

pool.shutdown();
```

### TaskTraits

```rust
use rust_task::{TaskTraits, TaskPriority, TaskShutdownBehavior, ThreadPolicy};

let traits = TaskTraits {
    priority: TaskPriority::UserBlocking,
    shutdown_behavior: TaskShutdownBehavior::BlockShutdown,
    thread_policy: ThreadPolicy::MustUseForeground,
    may_block: true,
};
```

---

## Examples

| Example | What it shows |
|---------|---------------|
| `event_bus` | Event bus on `SequencedTaskRunner`: ordered dispatch, safe re-entrant publish, auto-cancel via `Weak` |
| `io_task_runner` | epoll-backed I/O: one-shot watch, persistent watch, lifetime-safe watch via `Weak` |
| `socket_posix` | Async TCP client: connect + read/write, `ReadIfReady`, streaming with callback chaining |
| `repeating_timer` | Repeating timer built on `post_delayed_task` |
| `task_monitor` | Queue/execution metrics via `on_metrics`; hang detection via `on_hang` |
| `file_proxy` | Async file I/O via `FileProxy`: write→read chaining, sequential appends, concurrent reads |

```bash
cargo run --example event_bus
cargo run --example io_task_runner
cargo run --example socket_posix
cargo run --example repeating_timer
cargo run --example task_monitor
cargo run --example file_proxy
```
