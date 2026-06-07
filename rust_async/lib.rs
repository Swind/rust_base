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
//! | Runnable queue / re-schedule | [`async_task`] + `rust_task::TaskRunner::post_task` |
//! | Reactor (epoll → `Waker`)  | `rust_io::IoTaskRunner` + an `FdWatcher` that wakes |
//! | `block_on`                 | thread-parking waker on the calling thread    |
//! | `spawn`                    | `async_task::spawn` scheduled onto a `ThreadPool` |
//!
//! See [`net::Async`] for the load-bearing piece: turning epoll readiness
//! callbacks into `Waker` wake-ups.
//!
//! ## Module map (mirrors `async_std`)
//!
//! - [`task`] — [`block_on`], [`spawn`], [`task::sleep`], [`task::timeout`],
//!   [`task::yield_now`], [`task::spawn_blocking`], task-locals.
//! - [`net`] — [`net::Async`] (a reactor-backed `TcpStream`),
//!   [`net::TcpListener`], [`net::UdpSocket`].
//! - [`io`] — the ecosystem-standard [`futures_io`] `AsyncRead`/`AsyncWrite`
//!   traits that [`net::Async`] implements.
//! - [`fs`] — async files over `rust_io::FileProxy`.
//! - [`sync`] — async `Mutex`/`RwLock`/`Barrier`/`channel`.
//! - [`stream`] — the [`futures_core`] `Stream` trait plus combinators.
//! - [`prelude`] — the traits you usually want in scope.
//!
//! ## Known limitations
//!
//! - Linux only (inherits `rust_io`).
//! - A single reactor thread (the `rust_io` epoll loop). Fine for a faithful
//!   Chromium-style model; a general-purpose runtime might want several.
//! - Combinators on [`net::Async`] come from the `futures` ecosystem (it
//!   implements `futures_io`'s traits); we do not re-implement them.
//!
//! See `docs/runtime-feasibility.md` for the full roadmap and how each piece
//! maps onto its `async-std` counterpart.

mod block_on;
mod executor;
mod local;
mod reactor;

pub mod current_thread;
pub mod fs;
pub mod net;
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
pub use net::{Async, TcpListener, UdpSocket};
pub use task_impl::{
    Offload, Timeout, TimeoutError, Timer, YieldNow, offload, sleep, spawn_blocking, timeout,
    yield_now,
};
