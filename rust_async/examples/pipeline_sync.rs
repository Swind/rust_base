//! Producer/consumer over a *bounded* buffer built from the async sync
//! primitives: a `Mutex<VecDeque>` for the queue, two `Condvar`s for the
//! not-full / not-empty signals (backpressure), a `channel` to collect results,
//! and a `Barrier` so every task starts together.
//!
//! All tasks are `spawn`ed onto the global runtime's worker pool, so this is
//! real cross-thread coordination, not cooperative single-lane interleaving.
//!
//! Run with: `cargo run -p rust_async --example pipeline_sync`

use std::collections::VecDeque;
use std::sync::Arc;

use rust_async::sync::{Condvar, Mutex, channel};
use rust_async::{block_on, spawn};

const CAP: usize = 4; // queue capacity (backpressure kicks in here)
const TOTAL: u64 = 20; // items the producer emits
const CONSUMERS: usize = 3;

struct Shared {
    queue: Mutex<VecDeque<Option<u64>>>, // None is a "stop" sentinel
    not_full: Condvar,
    not_empty: Condvar,
}

fn main() {
    let total: u64 = block_on(async {
        let shared = Arc::new(Shared {
            queue: Mutex::new(VecDeque::new()),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        });
        // Barrier across the producer + all consumers.
        let barrier = Arc::new(rust_async::sync::Barrier::new(CONSUMERS + 1));
        let (results_tx, results_rx) = channel::<u64>();

        // Producer.
        let producer = {
            let shared = Arc::clone(&shared);
            let barrier = Arc::clone(&barrier);
            spawn(async move {
                barrier.wait().await;
                for i in 0..TOTAL {
                    let mut q = shared.queue.lock().await;
                    q = shared.not_full.wait_until(q, |q| q.len() < CAP).await;
                    q.push_back(Some(i));
                    drop(q);
                    shared.not_empty.notify_one();
                }
                // One stop sentinel per consumer.
                for _ in 0..CONSUMERS {
                    let mut q = shared.queue.lock().await;
                    q = shared.not_full.wait_until(q, |q| q.len() < CAP).await;
                    q.push_back(None);
                    drop(q);
                    shared.not_empty.notify_one();
                }
            })
        };

        // Consumers.
        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|id| {
                let shared = Arc::clone(&shared);
                let barrier = Arc::clone(&barrier);
                let results_tx = results_tx.clone();
                spawn(async move {
                    barrier.wait().await;
                    loop {
                        let mut q = shared.queue.lock().await;
                        q = shared.not_empty.wait_until(q, |q| !q.is_empty()).await;
                        let item = q.pop_front().unwrap();
                        drop(q);
                        shared.not_full.notify_one();
                        match item {
                            Some(v) => {
                                results_tx.send(v * v).ok();
                            }
                            None => break,
                        }
                    }
                    id
                })
            })
            .collect();

        // Drop our spare sender so the channel can report closure if needed.
        drop(results_tx);

        // Collect exactly TOTAL results (the squares).
        let mut sum = 0u64;
        for _ in 0..TOTAL {
            sum += results_rx.recv().await.unwrap();
        }

        producer.await;
        for c in consumers {
            let _id = c.await;
        }
        sum
    });

    let expected: u64 = (0..TOTAL).map(|i| i * i).sum();
    println!("sum of squares 0..{TOTAL} = {total} (expected {expected})");
    assert_eq!(total, expected);
}
