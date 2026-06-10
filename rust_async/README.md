# rust_async

A conventional `async` / `.await` runtime layered on `rust_task` + `rust_io`.
Requires Linux (epoll).

It consumes only the **public** APIs of the lower crates — nothing in
`rust_task` or `rust_io` was changed to support it. The pivotal idea is small:
an epoll-readiness callback becomes a `Waker` wake-up. From there the crate grows
to roughly the surface area of [`async-std`](https://docs.rs/async-std),
organised the same way (`task`, `net`, `io`, `fs`, `sync`, `stream`, `os::unix`).

| Concept | Provided by |
|---------|-------------|
| Runnable queue / re-schedule | [`async-task`](https://docs.rs/async-task) + a `Runtime`'s task runner |
| Reactor (epoll → `Waker`) | `rust_io::IoTaskRunner` + an `FdWatcher` that wakes the task |
| `block_on` | a thread-parking waker on the calling thread |
| `spawn` | `async_task::spawn` scheduled onto a `Runtime` |

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

## Two I/O paths

Not every fd has a meaningful epoll readiness. The crate uses the reactor where
it works and offloads to a blocking thread pool where it doesn't:

- **epoll readiness** — sockets, pipes, TTYs. A non-blocking syscall returns
  `WouldBlock`, the task registers its `Waker` with the reactor, and the
  epoll callback wakes it. This is `net::Async` and everything in
  `os::unix::net`.
- **blocking offload** — regular files, DNS resolution, directory/metadata
  syscalls, and stdio, none of which report readiness. These run as blocking
  work on a thread pool (`task::spawn_blocking` / `fs`'s own pool) and complete
  through a oneshot the future polls.

So `fs::File` is cursor-based over a blocking pool, while `net::TcpStream` is
reactor-backed — the same `AsyncRead`/`AsyncWrite` traits, different machinery
underneath.

## The runtime is one configurable thing

A `Runtime` pairs a **task runner** (where woken futures are polled) with an **io
task runner** (where `await`ed I/O is armed and woken):

```rust
use rust_async::Runtime;

let rt = Runtime::new(task_runner, io_task_runner);
let handle = rt.spawn(async { /* … */ });
```

Every topology — one fused lane, a parallel pool, an ordered sequence, or
thread-per-core — is a choice of those two arguments, not a separate module. A
task carries its runtime, so nested `spawn`s inherit it and `await`ed I/O arms
its reactor no matter which thread polls the task. The global `block_on` /
`spawn` use a parallel runner over a shared pool, so spawned tasks get real
cross-thread concurrency.

## Modules

- **`task`** — `block_on`, `spawn`, `sleep`, `timeout`, `yield_now`,
  `spawn_blocking`, `offload`, task-locals.
- **`net`** — `TcpListener` / `TcpStream` (`Async`), `UdpSocket`, a
  `ToSocketAddrs` that resolves host names off the executor, and
  `listener.incoming()` as a `Stream`.
- **`os::unix::net`** — `UnixStream` / `UnixListener` / `UnixDatagram` over the
  same reactor readiness path.
- **`fs`** — cursor-based `File` (AsyncRead/AsyncWrite/AsyncSeek) + `OpenOptions`
  / `DirBuilder`, directory and metadata ops (`create_dir_all`, `rename`,
  `copy`, `metadata`, `set_permissions`, …), and `read_dir` as a `Stream`. The
  blocking-pool size is configurable via `fs::init_pool(n)` or the
  `RUST_ASYNC_FS_THREADS` env var (default 4).
- **`io`** — the `futures_io` traits, buffering (`BufReader`, `BufWriter`,
  `Cursor`, `io::copy`) re-exported from `futures-util`, and async `stdin` /
  `stdout` / `stderr`.
- **`sync`** — async `Mutex`, `RwLock`, `Condvar`, `Barrier`, and an MPMC
  `channel`.
- **`stream`** — the `futures_core` `Stream` trait, a small `StreamExt`, and
  `stream::interval`.
- **`prelude`** — `Future`, `AsyncRead`/`AsyncWrite`, `Stream`/`StreamExt`.

## Keeping objects alive

The reactor-backed types follow the same rule as `rust_io`/`rust_net`: an
`await`ed I/O object must stay alive until its callback fires (the event loop
holds only a `Weak` watcher). With `async`/`await` this is usually automatic —
the future owns the socket across the `.await`, so dropping the future is the
only way to cancel, and that is exactly the cancellation semantics you want.

## Known limitations

- **Linux only** (inherits `rust_io`).
- **Combinators come from the `futures` ecosystem.** `net::Async` implements
  `futures_io`'s traits rather than re-implementing `read`/`write` adapters.
- **Timers are not cancellable.** Dropping a `sleep` / `timeout` /
  `stream::interval` future does not unschedule the delayed task already queued
  on the reactor — it still fires once, into a now-empty waker slot. Harmless
  per-timer, but many long-deadline timeouts can accumulate pending reactor work
  until their deadlines elapse. A production runtime would use a cancellable
  timer (e.g. a timer wheel).

## Examples

```bash
cargo run -p rust_async --example async_tcp_echo        # reactor-backed TCP echo server + client
cargo run -p rust_async --example http_server           # minimal HTTP server, single lane
cargo run -p rust_async --example http_server_multicore # same server, thread-per-core Runtime
cargo run -p rust_async --example http_get              # client GET over net::TcpStream + BufReader
cargo run -p rust_async --example file_pipeline         # fs::File read → transform → write
cargo run -p rust_async --example async_cat             # stream stdin/files to stdout via io::copy
cargo run -p rust_async --example unix_echo             # UnixListener / UnixStream echo
cargo run -p rust_async --example udp_chat              # UdpSocket send_to/recv_from + connected mode
cargo run -p rust_async --example pipeline_sync         # spawn + channel + Mutex coordination
cargo run -p rust_async --example tick_and_timeout      # stream::interval, sleep, timeout
```

```bash
cargo test -p rust_async       # unit + integration (fs, io, sync, tcp_echo, unix, stdio) + doctests
cargo doc  -p rust_async --open
```
