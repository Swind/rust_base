# rust_base

[![CI](https://github.com/Swind/rust_base/actions/workflows/ci.yml/badge.svg)](https://github.com/Swind/rust_base/actions/workflows/ci.yml)

A personal base library for Rust projects, porting core concepts from Chromium's [`base/`](https://source.chromium.org/chromium/chromium/src/+/main:base/) and [`net/`](https://source.chromium.org/chromium/chromium/src/+/main:net/) layers into idiomatic Rust.

> **Experimental.** This is a personal learning and utility project, not a production-grade library. APIs may change without notice.

---

## Background

Chromium has a well-designed threading and I/O model built around a few core abstractions:

- **`base::TaskRunner`** / **`base::SequencedTaskRunner`** — post callbacks to thread pools with ordering and shutdown guarantees
- **`base::MessagePumpForIO`** / **`base::FileDescriptorWatcher`** — epoll-backed event loop for async I/O
- **`base::FileProxy`** — async file I/O via a blocking thread pool
- **`net::SocketPosix`** — non-blocking TCP socket with callback-based read/write/connect/accept

This workspace ports those ideas to Rust while keeping the same separation of concerns:
task scheduling stays cross-platform, I/O primitives are Linux-specific.

---

## Crates

| Crate | Description | Platform |
|-------|-------------|----------|
| [`rust_task`](rust_task/) | Thread pool, task runners, sequencing, shutdown lifecycle, task monitoring | cross-platform |
| [`rust_io`](rust_io/) | epoll event loop, async file I/O | Linux |
| [`rust_net`](rust_net/) | Async TCP socket (client + server) | Linux |

### Dependency graph

```
rust_task  ←── rust_io  ←── rust_net
```

`rust_task` has no platform-specific dependencies — only `std`. `rust_io` and `rust_net` require Linux (epoll / `accept4`).

---

## rust_task

Thread pool and task scheduling. Port of Chromium's `base::TaskRunner` / `base::ThreadPool`.

```rust
use rust_task::{ThreadPool, TaskTraits};

let pool = ThreadPool::new(4);

// One-shot parallel task
pool.post_task(TaskTraits::default(), Box::new(|| println!("hello")));

// Sequenced runner — tasks execute strictly in FIFO order
let runner = pool.create_sequenced_task_runner(TaskTraits::default());
runner.post_task(Box::new(|| println!("first")));
runner.post_task(Box::new(|| println!("second")));

pool.shutdown(); // waits for BlockShutdown tasks to complete
```

→ See [`rust_task/README.md`](rust_task/README.md) for the full API.

---

## rust_io

epoll-backed event loop and async file I/O. Port of Chromium's `base::MessagePumpForIO` and `base::FileProxy`.

```rust
use rust_io::{IoTaskRunner, FileProxy};
use rust_task::{ThreadPool, TaskTraits, TaskRunner};

let io   = IoTaskRunner::new();
let pool = ThreadPool::new(2);

io.post_task(Box::new(move || {
    // All file ops run on the thread pool; callback fires back on the IO thread.
    let file = FileProxy::new("/tmp/hello.txt", Arc::clone(&pool));
    file.write_all(b"hello".to_vec(), move |result| {
        result.unwrap();
        println!("written");
    });
}));

io.shutdown();
```

→ See [`rust_io/README.md`](rust_io/README.md) for the full API.

---

## rust_net

Async TCP socket. Port of Chromium's `net::SocketPosix`.

```rust
use rust_net::SocketPosix;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;

let io     = IoTaskRunner::new();
let socket = SocketPosix::new();   // keep alive outside the closure
let s      = Arc::clone(&socket);

io.post_task(Box::new(move || {
    s.open(&addr).unwrap();
    let s2 = Arc::clone(&s);
    s.connect(addr, move |result| {
        result.unwrap();
        s2.write(b"hello".to_vec(), |_| {});
    });
}));

io.shutdown();
```

→ See [`rust_net/README.md`](rust_net/README.md) for the full API including server-side `bind` / `listen` / `accept`.

---

## Installation

Add crates as git dependencies:

```toml
[dependencies]
rust_task = { git = "https://github.com/Swind/rust_base" }

# Linux only
rust_io  = { git = "https://github.com/Swind/rust_base" }
rust_net = { git = "https://github.com/Swind/rust_base" }
```

Pin to a specific commit for reproducible builds:

```toml
rust_task = { git = "https://github.com/Swind/rust_base", rev = "abc1234" }
```

For local development, use `[patch]` in `.cargo/config.toml`:

```toml
[patch."https://github.com/Swind/rust_base"]
rust_task = { path = "/path/to/rust_base/rust_task" }
rust_io   = { path = "/path/to/rust_base/rust_io" }
rust_net  = { path = "/path/to/rust_base/rust_net" }
```
