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
task scheduling stays cross-platform, I/O primitives are Linux-specific. On top of
the callback-based core, `rust_async` adds a conventional `async`/`await` runtime
(Future / poll / Waker) without modifying the lower crates — the epoll-readiness
callback becomes a `Waker` wake-up.

---

## Crates

| Crate | Description | Platform |
|-------|-------------|----------|
| [`rust_task`](rust_task/) | Thread pool, task runners, sequencing, shutdown lifecycle, task monitoring | cross-platform |
| [`rust_io`](rust_io/) | epoll event loop, async file I/O | Linux |
| [`rust_net`](rust_net/) | Async TCP socket (client + server), `StreamSocket` abstraction, async TLS (`tls` feature) | Linux |
| [`rust_async`](rust_async/) | `async`/`await` runtime (Future / poll / Waker) on rust_task + rust_io: async TCP/UDP/Unix sockets, files, buffering, stdio, `Mutex`/`RwLock`/`Condvar`/`channel`, streams, timers | Linux |

### Dependency graph

```
rust_task  ←── rust_io  ←┬── rust_net
                         └── rust_async
```

`rust_task` has no platform-specific dependencies — only `std`. `rust_io`, `rust_net`, and `rust_async` require Linux (epoll / `accept4`). Async TLS lives in `rust_net` behind the optional `tls` feature, which adds a `rustls` dependency; without it `rust_net` stays dependency-light. `rust_async` is an alternative front end to the same core — pick the callback API (`rust_io`/`rust_net`) or the `async`/`await` one (`rust_async`).

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

## rust_net TLS (`tls` feature)

Enabling `rust_net`'s `tls` feature adds `TlsClientSocket`: async TLS over any `StreamSocket`, using `rustls`'s sans-IO core. Port of Chromium's `net::SSLClientSocket`.

```rust
use rust_net::{StreamSocket, TcpClientSocket, TlsClientSocket};
use rustls::pki_types::ServerName;

// On the IO thread, after the TCP transport has connected:
let name = ServerName::try_from("example.com").unwrap().to_owned();
let tls  = TlsClientSocket::new(transport, config, name).unwrap();

tls.handshake(Box::new(move |r| {
    r.unwrap();
    // TlsClientSocket is itself a StreamSocket: read/write plaintext from here.
    tls.write(b"GET / HTTP/1.0\r\n\r\n".to_vec(), Box::new(|_| {}));
}));
```

→ See [`rust_net/README.md`](rust_net/README.md) for config setup and the `https_get` example.

---

## rust_async

A conventional `async`/`await` runtime layered on `rust_task` + `rust_io`, organised
like [`async-std`](https://docs.rs/async-std). It consumes only the public APIs of
the lower crates: the pivotal idea is turning an epoll-readiness callback into a
`Waker` wake-up. Files, DNS, and stdio (which have no epoll readiness) run as
blocking work on a thread pool instead.

```rust
use rust_async::block_on;
use rust_async::net::TcpListener;
use rust_async::io::{AsyncBufReadExt, BufReader};
use rust_async::stream::StreamExt;

block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let (stream, _) = listener.accept().await?;

    // `stream` implements futures_io's AsyncRead/AsyncWrite, so the whole
    // `futures` ecosystem (BufReader, lines, io::copy, …) composes on top.
    let mut lines = BufReader::new(stream).lines();
    while let Some(line) = lines.next().await {
        println!("{}", line?);
    }
    Ok::<_, std::io::Error>(())
});
```

What it covers:

- **`net`** — `TcpListener`/`TcpStream` (`Async`), `UdpSocket`, `ToSocketAddrs` (host
  names resolved off the executor), `listener.incoming()` stream.
- **`os::unix::net`** — `UnixStream`/`UnixListener`/`UnixDatagram` over the same reactor.
- **`fs`** — cursor-based `File` (AsyncRead/AsyncWrite/AsyncSeek) + `OpenOptions`,
  directory/metadata ops, `read_dir` stream; blocking-pool size configurable via
  `fs::init_pool` / `RUST_ASYNC_FS_THREADS`.
- **`io`** — `futures_io` traits, buffering (`BufReader`/`BufWriter`/`Cursor`/`copy`),
  and async `stdin`/`stdout`/`stderr`.
- **`sync`** — async `Mutex`, `RwLock`, `Condvar`, `Barrier`, MPMC `channel`.
- **`task`/`stream`** — `spawn`, `block_on`, `sleep`, `timeout`, `spawn_blocking`,
  `stream::interval`, and a small `StreamExt`.

The runtime topology (a single fused lane, a parallel pool, or thread-per-core) is
a choice of two arguments to `Runtime`, not a separate module.

→ See the [`rust_async/examples/`](rust_async/examples/) directory (`file_pipeline`,
`unix_echo`, `http_get`, `pipeline_sync`, `tick_and_timeout`, …) and the crate-level
docs (`cargo doc -p rust_async --open`) for the full surface.

---

## Installation

Add crates as git dependencies:

```toml
[dependencies]
rust_task = { git = "https://github.com/Swind/rust_base" }

# Linux only
rust_io    = { git = "https://github.com/Swind/rust_base" }
rust_net   = { git = "https://github.com/Swind/rust_base" }
rust_async = { git = "https://github.com/Swind/rust_base" }

# Enable async TLS / HTTPS support
rust_net = { git = "https://github.com/Swind/rust_base", features = ["tls"] }
```

Pin to a specific commit or tag for reproducible builds:

```toml
rust_task = { git = "https://github.com/Swind/rust_base", rev = "abc1234" }
rust_task = { git = "https://github.com/Swind/rust_base", tag = "v0.1.0" }
```

> The crates are not yet published to [crates.io](https://crates.io); use the
> git dependencies above. The manifests already carry the `version`, `license`,
> and `repository` metadata needed for a future `cargo publish` (publish order:
> `rust_task` → `rust_io` → `rust_net` / `rust_async`).

For local development, use `[patch]` in `.cargo/config.toml`:

```toml
[patch."https://github.com/Swind/rust_base"]
rust_task  = { path = "/path/to/rust_base/rust_task" }
rust_io    = { path = "/path/to/rust_base/rust_io" }
rust_net   = { path = "/path/to/rust_base/rust_net" }
rust_async = { path = "/path/to/rust_base/rust_async" }
```
