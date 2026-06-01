# rust_io — epoll event loop + async file I/O

Linux-only. Ports Chromium's `base::MessagePumpForIO` (epoll loop) and
`base::FileProxy` (async file I/O). Read the `rust_task` page in `SKILL.md` first
— this layer builds directly on its task-runner and `bind_once` concepts.

Public exports:

```rust
pub use rust_io::{IoTaskRunner, FdWatcher, FdWatchController, WatchMode, FileProxy};
// platform event-loop abstraction (see "MessagePump layering" below):
pub use rust_io::{MessagePumpForIo, MessagePumpDelegate, EpollMessagePump};
```

## The one rule that governs everything here

`IoTaskRunner` is a **single thread** that owns an epoll fd and a task queue.
Tasks posted with `post_task`, and the callbacks fired by `FdWatcher`, all run on
that same IO thread — so they never race each other and need no locking between
themselves.

The flip side: **every method that touches epoll must be called *from* the IO
thread.** That means `watch_file_descriptor` and all `SocketPosix` operations.
The way you "get onto" the IO thread is to `post_task` and do your work inside
that closure. Calling these from another thread is a bug.

## IoTaskRunner

`IoTaskRunner` implements the `SequencedTaskRunner` trait, so everything you know
from `rust_task` (`post_task`, `post_delayed_task`, `post_task_and_reply`,
`runs_tasks_in_current_sequence`) works identically — the IO thread *is* its
sequence.

```rust
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;

let io = IoTaskRunner::new();                 // spawns the IO thread, returns Arc<IoTaskRunner>

io.post_task(Box::new(|| println!("runs on the IO thread")));

io.shutdown();                                // stops the loop, joins the thread
```

| Method | Notes |
|--------|-------|
| `IoTaskRunner::new() -> Arc<Self>` | Starts the epoll loop on a new thread |
| `IoTaskRunner::new_with_monitor(monitor) -> Arc<Self>` | Same, wired to a `rust_task::TaskMonitor` |
| `IoTaskRunner::current() -> Option<Arc<Self>>` | The runner for the *current* IO thread, à la Chromium's `CurrentIOThread::Get()` |
| `watch_file_descriptor(fd, persistent, mode, &mut controller, watcher)` | **IO thread only.** Register `fd` with epoll |
| `shutdown()` | Stop the loop and join |

The runner is woken from a blocked `epoll_wait` by an internal `eventfd`, so
`post_task` from another thread takes effect promptly.

## Watching a file descriptor

Implement `FdWatcher` on the object that should be notified, hold an
`FdWatchController` to keep (and later cancel) the registration, and pass a
`WatchMode`:

```rust
use rust_io::{IoTaskRunner, FdWatcher, FdWatchController, WatchMode};
use rust_task::TaskRunner;
use std::os::unix::io::RawFd;
use std::sync::Arc;

struct Reader;
impl FdWatcher for Reader {
    fn on_file_can_read_without_blocking(&self, fd: RawFd) {
        // drain the fd here; runs on the IO thread
    }
    fn on_file_can_write_without_blocking(&self, _fd: RawFd) {}
}

let io = IoTaskRunner::new();
let watcher = Arc::new(Reader);
let w = Arc::clone(&watcher);
let io2 = Arc::clone(&io);

io.post_task(Box::new(move || {            // get onto the IO thread first
    let mut controller = FdWatchController::new();
    io2.watch_file_descriptor(fd, /*persistent=*/true, WatchMode::Read, &mut controller, w);
    // KEEP `controller` and `watcher` alive — store them in a struct that
    // outlives the watch. Dropping the controller cancels the watch; the loop
    // only holds a Weak ref to the watcher, so dropping it silences callbacks.
}));
```

### persistent vs. one-shot

- `persistent = true` — watch stays armed until you call
  `controller.stop_watching_file_descriptor()` (or drop the controller). Good for
  an accept loop or a long-lived connection.
- `persistent = false` — fires **once**, then auto-removes from epoll. To listen
  again you re-arm by calling `watch_file_descriptor` again inside the callback.
  This is the "per-operation" pattern behind `SocketPosix::read_if_ready`.

`WatchMode` is `Read`, `Write`, or `ReadWrite`.

### Why Weak watchers / generation counters matter

- The loop stores `Weak<dyn FdWatcher>`. If your watcher's last strong ref drops,
  `upgrade()` fails and the callback is simply skipped — no dangling. **So you
  must keep an `Arc` to the watcher alive yourself.**
- `FdWatchController` carries a generation counter, so an old controller dropped
  *after* the same fd was re-registered won't accidentally cancel the new watch.
- Callbacks are invoked only *after* the internal `watches` lock is released, so
  it's safe to re-arm (`watch_file_descriptor`) from inside a callback without
  deadlocking.

## FileProxy — async file I/O

Regular files aren't epoll-compatible, so `FileProxy` offloads the blocking
`pread`/`pwrite` to a `rust_task::ThreadPool` and posts the **result callback back
to the IO thread**. Construct it with the pool that should do the blocking work:

```rust
use rust_io::{FileProxy, IoTaskRunner};
use rust_task::{ThreadPool, TaskRunner};
use std::sync::Arc;

let pool = ThreadPool::new(2);
let io   = IoTaskRunner::new();

io.post_task(Box::new(move || {
    let file = FileProxy::new("/tmp/data.bin", Arc::clone(&pool));

    file.write_all(b"hello".to_vec(), move |result| {
        result.unwrap();
        // This callback runs on the IO thread — safe to chain the next op here.
        file.read_all(move |r| println!("read {} bytes", r.unwrap().len()));
    });
    // Keep `file` alive until its callbacks have fired (here it's moved into
    // the chain; otherwise store it somewhere that outlives the I/O).
}));
```

`FileProxy::with_runner(path, runner)` lets you supply any `Arc<dyn TaskRunner>`
instead of a `ThreadPool` directly.

| Method | Description |
|--------|-------------|
| `read(offset, len, cb)` | Positional `pread` — no seek |
| `read_all(cb)` | Read the whole file → `io::Result<Vec<u8>>` |
| `write(offset, data, cb)` | Positional `pwrite` — no truncation |
| `write_all(data, cb)` | Create or truncate, write from byte 0 → `io::Result<()>` |
| `append(data, cb)` | `O_APPEND` atomic write → `io::Result<usize>` |

## Try it

```bash
cd rust_io
cargo run --example io_task_runner   # one-shot watch, persistent watch, Weak lifetime
cargo run --example file_proxy       # write→read chaining, appends, concurrent reads
```

## MessagePump layering

`IoTaskRunner` is split into two layers, mirroring Chromium's
`SingleThreadTaskRunner` + `MessagePumpForIO`:

- **`IoTaskRunner`** (task layer, platform-agnostic): the task/delayed-task
  queues, `SequenceToken`, `TaskMonitor` wiring, and the `TaskRunner` /
  `SequencedTaskRunner` trait impls. It implements `MessagePumpDelegate` and owns
  an `Arc<dyn MessagePumpForIo>`.
- **`MessagePumpForIo`** (platform event loop): blocks on fd readiness,
  dispatches to `FdWatcher`s, and calls back into the delegate's `do_work()` to
  run ready tasks. The Linux backend is **`EpollMessagePump`** (owns the
  `epoll`/`eventfd` fds). `IoTaskRunner::new()` wires one up automatically.

You almost never touch these directly — `IoTaskRunner`'s public API is
unchanged. The seam exists so a non-Linux backend (kqueue, IOCP) could be added
without changing the task layer. To inject a custom backend, use
`IoTaskRunner::with_pump(pump)`.

## Chromium correspondence

| rust_io | Chromium |
|---------|----------|
| `IoTaskRunner` | `SingleThreadTaskRunner` / `CurrentIOThread` |
| `MessagePumpForIo` (trait) | `MessagePumpForIO` |
| `MessagePumpDelegate` (trait) | `MessagePump::Delegate` |
| `EpollMessagePump` | `MessagePumpEpoll` |
| `FdWatcher` / `FdWatchController` / `WatchMode` | `MessagePumpForIO::FdWatcher` / `FdWatchController` / `Mode` |
| internal `eventfd` wake | `MessagePumpEpoll::wake_event_` |
| `FileProxy` | `base::FileProxy` |
