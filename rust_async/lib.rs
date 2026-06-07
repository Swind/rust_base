//! # rust_async — a Future/poll/Waker runtime spike on top of `rust_task` + `rust_io`
//!
//! This crate is a **proof-of-concept**, not a finished runtime. Its only job
//! is to validate that the conventional Rust async model (`async` / `.await` /
//! `Future` / `Waker`) can be layered on the existing callback-based core
//! **without modifying `rust_task` or `rust_io` at all** — it consumes only
//! their public APIs.
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
//! See [`Async`] for the pivotal piece: turning epoll readiness callbacks into
//! `Waker` wake-ups.
//!
//! ## Known limitations (it's a spike)
//!
//! - Linux only (inherits `rust_io`).
//! - No combinators, no graceful shutdown wiring, and no `fs`/`sync`/`Stream`
//!   yet. Those are "surface area" left for later — see
//!   `docs/runtime-feasibility.md` for the roadmap to async-std parity.
//!
//! [`Async`] implements `futures_io::AsyncRead`/`AsyncWrite` (the same traits
//! `async-std` re-exports), so it already works with the `futures` ecosystem's
//! combinators.

mod block_on;
mod executor;
mod local;
mod net;
mod reactor;
mod task;

pub use block_on::block_on;
pub use executor::{JoinHandle, spawn};
pub use local::LocalKey;
pub use net::{Async, TcpListener, UdpSocket};
pub use task::{Timeout, TimeoutError, Timer, YieldNow, sleep, spawn_blocking, timeout, yield_now};
