# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Cargo workspace porting Chromium's `base/` threading model and `net/` I/O model into idiomatic Rust. Three crates with a strict dependency chain:

```
rust_task  ←── rust_io  ←── rust_net
```

- **`rust_task`** — thread pool, task runners, sequencing, delayed tasks, shutdown lifecycle, monitoring. Cross-platform (`std` only, no external deps).
- **`rust_io`** — epoll event loop + async file I/O. Linux only (`libc`).
- **`rust_net`** — async TCP client/server, `StreamSocket` trait, and async TLS behind the optional `tls` feature (adds `rustls`). Linux only.

Each crate sets `path = "lib.rs"` in `[lib]`, so crate roots are `<crate>/lib.rs`, not `src/`. Workspace uses Rust **edition 2024**.

**There is a `rust-base` skill** (see SKILL list) with the full conceptual model and per-crate references. Invoke it before writing or reviewing any code that uses these crates — it covers the load-bearing details (`bind_once` weak/strong semantics, shutdown behaviors, the "IO-thread-only" rule) that aren't repeated here.

## Commands

CI gates on all four of these (`.github/workflows/ci.yml`); run them before considering work done:

```bash
cargo +nightly fmt --all --check        # rustfmt is nightly (see rustfmt.toml)
cargo +stable clippy --workspace -- -D warnings   # warnings are errors
cargo +stable test --workspace          # unit + integration + doctests
cargo +stable build --workspace --examples
```

The `tls` feature is **not** covered by the default workspace test run — exercise it explicitly:

```bash
cargo test -p rust_net --features tls
cargo run -p rust_net --example https_get --features tls
```

Run a single test: `cargo test -p rust_task <test_name>`. Examples live in each crate's `examples/` (e.g. `cargo run -p rust_task --example event_bus`).

## Architecture essentials

**Everything is "post a callback, get it run elsewhere."** You never spawn threads directly. `ThreadPool::new(n)` returns an `Arc<ThreadPool>`; from it you create parallel `TaskRunner`s (tasks may run concurrently) or `SequencedTaskRunner`s (strict FIFO, never concurrent — the way to protect shared state without a mutex). Callbacks are `Box<dyn FnOnce() + Send + 'static>`.

**`rust_io`/`rust_net` add one hard rule:** every operation touching epoll must be called *from the IO thread* (post onto `IoTaskRunner` to get there), and you must keep the I/O object (`FileProxy`, `SocketPosix`, `TcpClientSocket`, `TlsClientSocket`) alive until its callbacks fire — the event loop holds only `Weak` references to watchers. Dropping the object early silently cancels its callbacks.

**`bind_once`** binds a callback to an `Arc` (always runs, extends lifetime) or a `Weak` (no-ops if the object is gone). Choosing wrong is the most common bug: a `Weak` binding on a "done" signal that another thread blocks on will hang forever.

**Shutdown is not "drop everything."** `pool.shutdown()` behavior depends on each task's `TaskShutdownBehavior`: `SkipOnShutdown` (default, dropped), `ContinueOnShutdown`, or `BlockShutdown` (shutdown blocks until complete). Use `BlockShutdown` for work that must finish.

## Chromium parallel

This intentionally mirrors Chromium's design. When unsure how an abstraction should behave, the corresponding `base::`/`net::` type (TaskRunner, SequencedTaskRunner, MessagePumpForIO, FileDescriptorWatcher, FileProxy, SocketPosix, SSLClientSocket) is the reference. `BUILD.gn` files exist because this is structured to drop into a Chromium-style GN build; the Cargo workspace is the primary build path.
