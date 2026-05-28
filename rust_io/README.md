# rust_io

Linux async I/O primitives built on top of `rust_task`. Requires Linux (epoll).

## Crates

| Type | Description |
|------|-------------|
| `IoTaskRunner` | Single-threaded epoll event loop; implements `TaskRunner` |
| `FileProxy` | Async file I/O — offloads blocking `pread`/`pwrite` to a thread pool, fires callback on the IO thread |

## IoTaskRunner

Wraps an epoll fd and a task queue in one thread. Callbacks from `FdWatcher` and tasks posted via `post_task` all run on the same IO thread, so no synchronization is needed between them.

```rust
use rust_io::{IoTaskRunner, FdWatcher, WatchMode};
use rust_task::TaskRunner;

let io = IoTaskRunner::new();

io.post_task(Box::new(|| println!("runs on IO thread")));

// Watch a file descriptor
io.watch_file_descriptor(
    fd, /*persistent=*/true, WatchMode::Read,
    &mut controller, watcher,
);

io.shutdown();
```

All methods that touch epoll (`watch_file_descriptor`, socket operations) **must be called from the IO thread**.

## FileProxy

Regular files are not epoll-compatible. `FileProxy` offloads blocking I/O to a `ThreadPool` and posts each result callback back to the IO thread.

```rust
use rust_io::{FileProxy, IoTaskRunner};
use rust_task::{ThreadPool, TaskTraits};

let pool = ThreadPool::new(2);
let io   = IoTaskRunner::new();

// From a task on the IO thread:
io.post_task(Box::new(move || {
    let file = FileProxy::new("/tmp/data.bin", Arc::clone(&pool));

    file.read_all(move |result| {
        println!("read {} bytes", result.unwrap().len());
        // callback always runs on the IO thread — safe to chain more ops
    });
}));
```

### Operations

| Method | Description |
|--------|-------------|
| `read(offset, len, cb)` | `pread` — positional, no seek needed |
| `read_all(cb)` | Read entire file |
| `write(offset, data, cb)` | `pwrite` — no truncation |
| `write_all(data, cb)` | Create or truncate, write from byte 0 |
| `append(data, cb)` | `O_APPEND` atomic write |

## Examples

```bash
cargo run --example io_task_runner  # one-shot watch, persistent watch, Weak lifetime
cargo run --example file_proxy      # write→read chaining, appends, concurrent reads
```
