//! # rust_async — a Future/poll/Waker runtime on top of `rust_task` + `rust_io`
//!
//! This crate layers the conventional Rust async model (`async` / `.await` /
//! `Future` / `Waker`) on the existing callback-based core **without modifying
//! `rust_task` or `rust_io` at all** — it consumes only their public APIs. It
//! began as a spike to validate the pivotal idea (turn an epoll-readiness
//! callback into a `Waker` wake-up) and has since grown to roughly the surface
//! area of [`async-std`](https://docs.rs/async-std), organised the same way.
//!
//! The mapping it demonstrates:
//!
//! | Runtime concept            | Provided by                                   |
//! |----------------------------|-----------------------------------------------|
//! | Runnable queue / re-schedule | [`async_task`] + a [`Runtime`]'s task runner |
//! | Reactor (epoll → `Waker`)  | `rust_io::IoTaskRunner` + an `FdWatcher` that wakes |
//! | `block_on`                 | thread-parking waker on the calling thread    |
//! | `spawn`                    | `async_task::spawn` scheduled onto a [`Runtime`] |
//!
//! See [`net::Async`] for the load-bearing piece: turning epoll readiness
//! callbacks into `Waker` wake-ups.
//!
//! ## The runtime is one configurable thing
//!
//! A [`Runtime`] pairs a **task runner** (where woken futures are polled) with
//! an **io task runner** (where `await`ed I/O is armed and woken). Every
//! topology — one fused lane, a parallel pool, an ordered sequence, or
//! thread-per-core — is a choice of those two arguments, not a separate module.
//! A task carries its runtime, so nested [`spawn`]s inherit it and `await`ed
//! I/O arms its reactor no matter which thread polls the task. See [`Runtime`].
//!
//! ## Module map (mirrors `async_std`)
//!
//! - [`task`] — [`block_on`], [`spawn`], [`task::sleep`], [`task::timeout`],
//!   [`task::yield_now`], [`task::spawn_blocking`], task-locals.
//! - [`net`] — [`net::Async`] (a reactor-backed `TcpStream`),
//!   [`net::TcpListener`], [`net::UdpSocket`].
//! - [`io`] — the ecosystem-standard [`futures_io`] `AsyncRead`/`AsyncWrite`
//!   traits that [`net::Async`] implements.
//! - [`fs`] — async files over `rust_io::FileProxy`; the blocking pool size is
//!   configurable via [`fs::init_pool`] or `RUST_ASYNC_FS_THREADS`.
//! - [`sync`] — async `Mutex`/`RwLock`/`Barrier`/`channel`.
//! - [`stream`] — the [`futures_core`] `Stream` trait plus combinators.
//! - [`prelude`] — the traits you usually want in scope.
//!
//! ## Known limitations
//!
//! - Linux only (inherits `rust_io`).
//! - Combinators on [`net::Async`] come from the `futures` ecosystem (it
//!   implements `futures_io`'s traits); we do not re-implement them.
//! - **Timers are not cancellable.** Dropping a [`task::sleep`] /
//!   [`task::timeout`] / [`stream::interval`] future does not unschedule the
//!   delayed task already queued on the reactor — it still fires once, into a
//!   now-empty waker slot. Harmless per-timer, but many long-deadline timeouts
//!   can accumulate pending reactor work until their deadlines elapse. A
//!   production runtime would use a cancellable timer (e.g. a timer wheel).
//!
//! See `docs/runtime-feasibility.md` for the full roadmap and how each piece
//! maps onto its `async-std` counterpart.

mod block_on;
mod executor;
mod local;
mod reactor;

pub mod fs;
pub mod net;
pub mod runtime;
pub mod stream;
pub mod sync;

#[path = "task.rs"]
mod task_impl;

/// Spawning and timing, mirroring `async_std::task`.
pub mod task {
    pub use crate::block_on::block_on;
    pub use crate::executor::{JoinHandle, spawn};
    pub use crate::local::LocalKey;
    pub use crate::task_impl::{
        Offload, Timeout, TimeoutError, Timer, YieldNow, offload, sleep, spawn_blocking, timeout,
        yield_now,
    };
}

/// Async I/O traits, mirroring `async_std::io`.
///
/// These are the de-facto-standard [`futures_io`] traits (the same ones
/// `async-std` re-exports), so types here interoperate with the wider
/// `futures` ecosystem.
pub mod io {
    pub use futures_io::{AsyncRead, AsyncWrite};
}

/// The traits you usually want in scope, mirroring `async_std::prelude`.
pub mod prelude {
    pub use std::future::Future;

    pub use crate::io::{AsyncRead, AsyncWrite};
    pub use crate::stream::{Stream, StreamExt};
}

// Convenience re-exports at the crate root (the most-reached-for items).
pub use block_on::block_on;
pub use executor::{JoinHandle, spawn};
pub use local::LocalKey;
pub use net::{Async, Incoming, TcpListener, TcpStream, UdpSocket};
pub use runtime::Runtime;
pub use task_impl::{
    Offload, Timeout, TimeoutError, Timer, YieldNow, offload, sleep, spawn_blocking, timeout,
    yield_now,
};
