//! Thread-per-core runtime: lanes run in parallel, work is sharded.

use std::collections::HashSet;
use std::sync::Arc;
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use rust_async::runtime::Runtime;
use rust_async::sleep;

#[test]
fn lanes_run_in_parallel() {
    let rt = Runtime::new(4);
    let rt2 = Arc::clone(&rt);

    let start = Instant::now();
    rt.run(async move {
        // One 50ms blocking sleep per lane. If the lanes were not parallel
        // threads, four of them would serialize to ~200ms.
        let handles: Vec<_> = (0..4)
            .map(|i| rt2.spawn_on(i, async move { thread::sleep(Duration::from_millis(50)) }))
            .collect();
        for h in handles {
            h.await;
        }
    });
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(150),
        "4 lanes x 50ms ran in {elapsed:?}; parallel should be ~50ms, serial ~200ms"
    );
}

#[test]
fn round_robin_spreads_across_distinct_lanes() {
    let rt = Runtime::new(3);
    let rt2 = Arc::clone(&rt);

    let ids: Vec<ThreadId> = rt.run(async move {
        let handles: Vec<_> = (0..3).map(|_| rt2.spawn(async { thread::current().id() })).collect();
        let mut v = Vec::new();
        for h in handles {
            v.push(h.await);
        }
        v
    });

    let uniq: HashSet<ThreadId> = ids.iter().copied().collect();
    assert_eq!(uniq.len(), 3, "expected 3 distinct lanes, got {ids:?}");
}

#[test]
fn each_lane_has_its_own_reactor_timer() {
    // sleep() must work on a non-zero lane too — proof that the reactor (epoll +
    // timer) is per-lane, not the global singleton.
    let rt = Runtime::new(2);
    let rt2 = Arc::clone(&rt);
    let start = Instant::now();
    rt.run(async move {
        rt2.spawn_on(1, async {
            sleep(Duration::from_millis(30)).await;
        })
        .await;
    });
    assert!(start.elapsed() >= Duration::from_millis(30));
}
