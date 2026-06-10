//! The time-driven pieces: `stream::interval` as a periodic stream (consumed
//! with `StreamExt`), `task::sleep`, and `task::timeout` resolving to a
//! `TimeoutError` when a future overruns its deadline.
//!
//! Note the crate's documented limitation: these timers are not cancellable —
//! dropping a timeout/interval future does not unschedule the delayed task
//! already queued on the reactor (it fires once into an empty waker slot).
//!
//! Run with: `cargo run -p rust_async --example tick_and_timeout`

use std::time::{Duration, Instant};

use rust_async::stream::{StreamExt, interval};
use rust_async::{block_on, sleep, timeout};

fn main() {
    block_on(async {
        // Take 5 ticks from a 200ms interval.
        let start = Instant::now();
        let mut ticks = interval(Duration::from_millis(200)).take(5);
        let mut n = 0;
        while ticks.next().await.is_some() {
            n += 1;
            println!("tick {n} at {:?}", start.elapsed());
        }

        // A future that finishes within the deadline.
        let ok = timeout(Duration::from_millis(200), async {
            sleep(Duration::from_millis(50)).await;
            "completed"
        })
        .await;
        println!("\ninside deadline: {ok:?}");

        // A future that overruns the deadline -> Err(TimeoutError).
        let late = timeout(Duration::from_millis(50), async {
            sleep(Duration::from_secs(10)).await;
            "never"
        })
        .await;
        println!("past deadline:   {late:?}");
    });
}
