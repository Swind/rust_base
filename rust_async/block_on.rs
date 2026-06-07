//! `block_on`: drive a future to completion on the calling thread.
//!
//! The waker simply unparks the calling thread. The reactor runs on its own
//! thread (`rust_io::IoTaskRunner`); when an fd becomes ready it wakes this
//! waker, which unparks us so we re-poll. No executor is involved for the root
//! future — this is deliberately the simplest thing that proves the wiring.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use crate::local::tag;

struct ThreadWaker {
    thread: Thread,
    awoken: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.awoken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

/// Run `future` to completion, blocking the current thread until it resolves.
pub fn block_on<F: Future>(future: F) -> F::Output {
    // Wrap so the root future has its own task-local storage too.
    let mut future = pin!(tag(future));
    let tw = Arc::new(ThreadWaker { thread: thread::current(), awoken: AtomicBool::new(false) });
    let waker = Waker::from(tw.clone());
    let mut cx = Context::from_waker(&waker);

    loop {
        if let Poll::Ready(v) = future.as_mut().poll(&mut cx) {
            return v;
        }
        // Set-before-unpark in the waker plus this swap-before-park means a wake
        // that races the poll is never lost.
        while !tw.awoken.swap(false, Ordering::Acquire) {
            thread::park();
        }
    }
}
