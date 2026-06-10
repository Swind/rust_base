//! Stage 5: async synchronization primitives.

use std::sync::Arc;

use rust_async::sync::{Barrier, Mutex, RwLock, channel};
use rust_async::{block_on, spawn};

#[test]
fn mutex_serializes_increments() {
    block_on(async {
        let m = Arc::new(Mutex::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let m = m.clone();
            handles.push(spawn(async move {
                for _ in 0..1000 {
                    let mut g = m.lock().await;
                    *g += 1;
                }
            }));
        }
        for h in handles {
            h.await;
        }
        assert_eq!(*m.lock().await, 8 * 1000);
    });
}

#[test]
fn mutex_try_lock() {
    block_on(async {
        let m = Mutex::new(5);
        let g = m.lock().await;
        assert!(m.try_lock().is_none());
        drop(g);
        assert_eq!(*m.try_lock().unwrap(), 5);
    });
}

#[test]
fn rwlock_allows_concurrent_readers() {
    block_on(async {
        let lock = Arc::new(RwLock::new(0i64));
        {
            let r1 = lock.read().await;
            let r2 = lock.read().await;
            assert_eq!(*r1 + *r2, 0);
        }
        {
            let mut w = lock.write().await;
            *w = 42;
        }
        assert_eq!(*lock.read().await, 42);
    });
}

#[test]
fn rwlock_writer_is_exclusive() {
    block_on(async {
        let lock = Arc::new(RwLock::new(Vec::<u32>::new()));
        let mut handles = Vec::new();
        for i in 0..16 {
            let lock = lock.clone();
            handles.push(spawn(async move {
                let mut w = lock.write().await;
                w.push(i);
            }));
        }
        for h in handles {
            h.await;
        }
        assert_eq!(lock.read().await.len(), 16);
    });
}

#[test]
fn barrier_releases_group_with_one_leader() {
    block_on(async {
        let n = 5;
        let barrier = Arc::new(Barrier::new(n));
        let mut handles = Vec::new();
        for _ in 0..n {
            let barrier = barrier.clone();
            handles.push(spawn(async move { barrier.wait().await.is_leader() }));
        }
        let mut leaders = 0;
        for h in handles {
            if h.await {
                leaders += 1;
            }
        }
        assert_eq!(leaders, 1);
    });
}

#[test]
fn channel_round_trip() {
    block_on(async {
        let (tx, rx) = channel::<u32>();
        let producer = spawn(async move {
            for i in 0..100 {
                tx.send(i).unwrap();
            }
            // tx dropped here → channel closes once drained.
        });
        let mut sum = 0u32;
        while let Ok(v) = rx.recv().await {
            sum += v;
        }
        producer.await;
        assert_eq!(sum, (0..100).sum());
    });
}

#[test]
fn channel_closed_sender_yields_error() {
    block_on(async {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(rx.recv().await, Ok(1));
        assert!(rx.recv().await.is_err());
    });
}

#[test]
fn channel_send_after_receivers_gone_fails() {
    block_on(async {
        let (tx, rx) = channel::<i32>();
        drop(rx);
        assert!(tx.send(1).is_err());
    });
}

#[test]
fn condvar_notifies_waiter() {
    use rust_async::sync::Condvar;

    block_on(async {
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let pair2 = Arc::clone(&pair);

        let waiter = spawn(async move {
            let (lock, cvar) = &*pair2;
            let mut ready = lock.lock().await;
            while !*ready {
                ready = cvar.wait(ready).await;
            }
            true
        });

        // Set the flag and notify.
        let (lock, cvar) = &*pair;
        {
            let mut ready = lock.lock().await;
            *ready = true;
        }
        cvar.notify_one();

        assert!(waiter.await);
    });
}

#[test]
fn condvar_wait_until_helper() {
    use rust_async::sync::Condvar;

    block_on(async {
        let pair = Arc::new((Mutex::new(0u32), Condvar::new()));
        let pair2 = Arc::clone(&pair);

        let waiter = spawn(async move {
            let (lock, cvar) = &*pair2;
            let guard = lock.lock().await;
            let guard = cvar.wait_until(guard, |n| *n >= 3).await;
            *guard
        });

        let (lock, cvar) = &*pair;
        for _ in 0..3 {
            *lock.lock().await += 1;
            cvar.notify_all();
        }

        assert_eq!(waiter.await, 3);
    });
}
