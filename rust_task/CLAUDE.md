# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands must be run from the `rust_task/` directory (where `Cargo.toml` lives).

```bash
# Run all tests (unit + integration + doctests)
cargo test

# Run a single test by name
cargo test <test_name>

# Run only unit tests inside a specific module
cargo test thread_pool::task_tracker

# Run only integration tests
cargo test --test thread_pool_integration

# Run an example
cargo run --example event_bus

# Check compilation without building
cargo check
```

## Architecture

This crate is a Rust port of Chromium's `base::task` thread pool. See `architecture.md` for the full original design doc. What follows is the as-built summary.

### Public API (lib.rs re-exports)

| Symbol | Purpose |
|--------|---------|
| `ThreadPool` | Entry point — creates runners, posts tasks, shuts down |
| `TaskRunner` | Trait for parallel task posting |
| `SequencedTaskRunner` | Trait for FIFO-ordered task posting |
| `TaskTraits` | Priority + shutdown behavior + thread policy |
| `bind_once` | Helper to bind `Arc<T>` or `Weak<T>` to a task closure |

### Ownership and data flow

```
ThreadPool
  ├── TaskTracker          — shutdown lifecycle, BlockShutdown counting
  ├── DelayedTaskManager   — timer thread, min-heap of (deadline, Sequence)
  └── ThreadGroup          — worker threads + PriorityQueue<TaskSource>
        └── workers run Sequence tasks
              └── Sequence — immediate VecDeque + delayed BinaryHeap (one Mutex)
```

`ThreadPool` is the only public struct; `ThreadGroup`, `Sequence`, etc. are internal.

### Task lifecycle

1. `post_task(traits, callback)` → `TaskTracker::will_post_task` (rejects if shutdown)
2. Callback is wrapped in a closure that enforces `SkipOnShutdown`/`BlockShutdown` at run time
3. A one-task `Sequence` is created and pushed to `ThreadGroup::push_task_source`
4. A worker wakes, calls `Sequence::will_run_task` (sets `has_worker`), runs the task, calls `did_process_task`
5. For delayed tasks, `DelayedTaskManager` holds the `Sequence` until deadline, then pushes it to `ThreadGroup`

### SequencedTaskRunner

`PooledSequencedTaskRunner` holds a persistent `Arc<Sequence>`. All tasks posted to the same runner share one sequence:
- `has_worker: AtomicBool` on the sequence prevents two workers from running it concurrently
- `did_process_task()` returns `true` if the sequence has more tasks → worker re-enqueues it

### Key invariants

- **`did_process_task()` must only be called when `will_run_task()` did NOT return `Disallowed`** — calling it after `Disallowed` clears `has_worker` while another worker may still hold it (see `thread_group.rs` worker loop)
- **`TaskTracker` uses `Mutex<Inner>`** (not two separate atomics) so `will_post_task` increments the `BlockShutdown` counter atomically with the shutdown check — this prevents `shutdown()` from returning before a queued-but-not-yet-started `BlockShutdown` task completes
- **`flush()` in `EventBus` must NOT use `bind_once(Weak<Self>)`** — the done callback must always fire or the caller's `Barrier` will hang forever

### `bind_once` and `IntoArc`

`bind_once(ptr, f)` wraps a pointer + callback into a `Box<dyn FnOnce() + Send>`:
- `Arc<T>` → always calls `f`
- `Weak<T>` → calls `f` only if the object is still alive

`IntoArc<T>` is the unifying trait. Using `Weak<T>` prevents the task queue from extending an object's lifetime past its last strong owner.

Internal methods that need `Arc::downgrade(self)` use `self: &Arc<Self>` as the receiver; callers write `obj.method()` as usual.

### `current_default` and sequence identity

`SequenceToken::current()` and `sequenced_task_runner::current_default()` are thread-locals set by the worker before running each task and cleared after. They are the mechanism behind `runs_tasks_in_current_sequence()` and `post_task_and_reply` (reply is posted back to the runner captured at call time).
