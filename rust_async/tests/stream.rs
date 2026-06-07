//! Stage 6: async streams and combinators.

use std::time::{Duration, Instant};

use rust_async::block_on;
use rust_async::stream::{self, StreamExt};

#[test]
fn from_iter_map_filter_collect() {
    block_on(async {
        let out: Vec<i32> =
            stream::from_iter(0..10).map(|x| x * 2).filter(|x| x % 4 == 0).collect().await;
        assert_eq!(out, vec![0, 4, 8, 12, 16]);
    });
}

#[test]
fn fold_sums_items() {
    block_on(async {
        let total = stream::from_iter(1..=5).fold(0, |acc, x| acc + x).await;
        assert_eq!(total, 15);
    });
}

#[test]
fn take_limits_and_next_drains() {
    block_on(async {
        let mut s = stream::repeat(7u8).take(3);
        assert_eq!(s.next().await, Some(7));
        assert_eq!(s.next().await, Some(7));
        assert_eq!(s.next().await, Some(7));
        assert_eq!(s.next().await, None);
    });
}

#[test]
fn once_and_empty() {
    block_on(async {
        assert_eq!(stream::once(42).collect().await, vec![42]);
        assert_eq!(stream::empty::<i32>().collect().await, Vec::<i32>::new());
    });
}

#[test]
fn for_each_visits_all() {
    block_on(async {
        let mut seen = Vec::new();
        stream::from_iter(["a", "b", "c"]).for_each(|x| seen.push(x)).await;
        assert_eq!(seen, vec!["a", "b", "c"]);
    });
}

#[test]
fn interval_ticks_are_spaced() {
    block_on(async {
        let start = Instant::now();
        let ticks: Vec<()> = stream::interval(Duration::from_millis(20)).take(3).collect().await;
        assert_eq!(ticks.len(), 3);
        // Three 20ms ticks should take at least ~40ms (first tick after one period).
        assert!(start.elapsed() >= Duration::from_millis(40), "elapsed = {:?}", start.elapsed());
    });
}
