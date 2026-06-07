//! Stage 3 finish: task-local storage and `JoinHandle` detach-on-drop.

use std::cell::Cell;
use std::sync::{Arc, Condvar, Mutex};

use rust_async::{block_on, spawn};

rust_async::task_local! {
    static N: Cell<u32> = Cell::new(0);
}

#[test]
fn task_local_is_isolated_per_task() {
    block_on(async {
        let a = spawn(async {
            N.with(|n| n.set(n.get() + 1));
            N.with(|n| n.set(n.get() + 1));
            N.with(|n| n.get())
        });
        let b = spawn(async {
            N.with(|n| n.set(n.get() + 10));
            N.with(|n| n.get())
        });
        assert_eq!(a.await, 2);
        assert_eq!(b.await, 10);

        // The root (block_on) task has its own independent slot.
        N.with(|n| n.set(n.get() + 5));
        assert_eq!(N.with(|n| n.get()), 5);
    });
}

#[test]
fn dropped_handle_detaches_and_keeps_running() {
    let done = Arc::new((Mutex::new(false), Condvar::new()));
    let signal = done.clone();

    block_on(async move {
        let handle = spawn(async move {
            let (lock, cv) = &*signal;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        });
        drop(handle); // detach: the task must still run to completion
    });

    let (lock, cv) = &*done;
    let mut ran = lock.lock().unwrap();
    while !*ran {
        ran = cv.wait(ran).unwrap();
    }
    assert!(*ran);
}
