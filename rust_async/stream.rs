//! Async streams, mirroring `async_std::stream`.
//!
//! Like the `io` layer (which reuses the ecosystem-standard
//! [`futures_io`](https://docs.rs/futures-io) traits), this re-exports the
//! de-facto-standard [`Stream`] trait from
//! [`futures_core`](https://docs.rs/futures-core) so our streams interoperate
//! with the wider `futures` ecosystem. On top of it we add a small
//! [`StreamExt`] of the common combinators and a few constructors — including
//! [`interval`], the one piece that is genuinely runtime-specific (it is driven
//! by the reactor's delayed-task timer).
//!
//! The combinator adapters require their inner stream to be [`Unpin`] (as
//! `futures`'s own simple forms do), which keeps every projection safe code.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

pub use futures_core::Stream;
use rust_task::TaskRunner;

use crate::reactor::io_runner;

// ── StreamExt ───────────────────────────────────────────────────────────────

/// Common combinators over a [`Stream`], mirroring `async_std`'s `StreamExt`.
pub trait StreamExt: Stream {
    /// Resolve to the next item, or `None` at end of stream.
    fn next(&mut self) -> Next<'_, Self>
    where
        Self: Unpin,
    {
        Next { stream: self }
    }

    /// Map each item through `f`.
    fn map<B, F>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        Map { stream: self, f }
    }

    /// Yield only items for which `predicate` returns `true`.
    fn filter<P>(self, predicate: P) -> Filter<Self, P>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, predicate }
    }

    /// Yield at most the first `n` items.
    fn take(self, n: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take { stream: self, remaining: n }
    }

    /// Accumulate every item into `init` via `f`.
    fn fold<B, F>(self, init: B, f: F) -> Fold<Self, B, F>
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        Fold { stream: self, acc: Some(init), f }
    }

    /// Run `f` for every item, to completion.
    fn for_each<F>(self, f: F) -> ForEach<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item),
    {
        ForEach { stream: self, f }
    }

    /// Collect every item into a `Vec`.
    fn collect(self) -> Collect<Self>
    where
        Self: Sized,
    {
        Collect { stream: self, items: Vec::new() }
    }
}

impl<S: Stream + ?Sized> StreamExt for S {}

/// Future returned by [`StreamExt::next`].
pub struct Next<'a, S: ?Sized> {
    stream: &'a mut S,
}

impl<S: Stream + Unpin + ?Sized> Future for Next<'_, S> {
    type Output = Option<S::Item>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.stream).poll_next(cx)
    }
}

/// Stream returned by [`StreamExt::map`].
pub struct Map<S, F> {
    stream: S,
    f: F,
}

impl<S, F, B> Stream for Map<S, F>
where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> B,
{
    type Item = B;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<B>> {
        // SAFETY: `stream` is `Unpin` (bound), so re-pinning it by reference is
        // sound; the closure `f` is never pinned. We never move out of a field.
        let this = unsafe { self.get_unchecked_mut() };
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some((this.f)(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream returned by [`StreamExt::filter`].
pub struct Filter<S, P> {
    stream: S,
    predicate: P,
}

impl<S, P> Stream for Filter<S, P>
where
    S: Stream + Unpin,
    P: FnMut(&S::Item) -> bool,
{
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        // SAFETY: see `Map` — inner stream is `Unpin`; we never move pinned data.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) if (this.predicate)(&item) => {
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Stream returned by [`StreamExt::take`].
pub struct Take<S> {
    stream: S,
    remaining: usize,
}

impl<S: Stream + Unpin> Stream for Take<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        // SAFETY: see `Map` — inner stream is `Unpin`; we never move pinned data.
        let this = unsafe { self.get_unchecked_mut() };
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.remaining -= 1;
                Poll::Ready(Some(item))
            }
            other => other,
        }
    }
}

/// Future returned by [`StreamExt::fold`].
pub struct Fold<S, B, F> {
    stream: S,
    acc: Option<B>,
    f: F,
}

impl<S, B, F> Future for Fold<S, B, F>
where
    S: Stream + Unpin,
    F: FnMut(B, S::Item) -> B,
{
    type Output = B;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<B> {
        // SAFETY: see `Map` — inner stream is `Unpin`; we never move pinned data.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let acc = this.acc.take().expect("fold polled after completion");
                    this.acc = Some((this.f)(acc, item));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(this.acc.take().expect("fold polled after completion"));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Future returned by [`StreamExt::for_each`].
pub struct ForEach<S, F> {
    stream: S,
    f: F,
}

impl<S, F> Future for ForEach<S, F>
where
    S: Stream + Unpin,
    F: FnMut(S::Item),
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: see `Map` — inner stream is `Unpin`; we never move pinned data.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => (this.f)(item),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Future returned by [`StreamExt::collect`].
pub struct Collect<S: Stream> {
    stream: S,
    items: Vec<S::Item>,
}

impl<S: Stream + Unpin> Future for Collect<S> {
    type Output = Vec<S::Item>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<S::Item>> {
        // SAFETY: see `Map` — inner stream is `Unpin`; we never move pinned data.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => this.items.push(item),
                Poll::Ready(None) => return Poll::Ready(std::mem::take(&mut this.items)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ── constructors ─────────────────────────────────────────────────────────────

/// A stream that yields `value` exactly once.
pub fn once<T>(value: T) -> Once<T> {
    Once { value: Some(value) }
}

/// Stream returned by [`once`].
pub struct Once<T> {
    value: Option<T>,
}

// Sound (and idiomatic — cf. `futures::stream::Iter`): this leaf stream holds
// no pinned self-references and only yields its value by move.
impl<T> Unpin for Once<T> {}

impl<T> Stream for Once<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<T>> {
        Poll::Ready(self.get_mut().value.take())
    }
}

/// An empty stream that ends immediately.
pub fn empty<T>() -> Empty<T> {
    Empty { _marker: std::marker::PhantomData }
}

/// Stream returned by [`empty`].
pub struct Empty<T> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> Stream for Empty<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<T>> {
        Poll::Ready(None)
    }
}

/// A stream that yields clones of `value` forever.
pub fn repeat<T: Clone>(value: T) -> Repeat<T> {
    Repeat { value }
}

/// Stream returned by [`repeat`].
pub struct Repeat<T> {
    value: T,
}

impl<T: Clone> Stream for Repeat<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<T>> {
        Poll::Ready(Some(self.value.clone()))
    }
}

/// Turn any iterator into a stream that yields its items.
pub fn from_iter<I: IntoIterator>(iter: I) -> FromIter<I::IntoIter> {
    FromIter { iter: iter.into_iter() }
}

/// Stream returned by [`from_iter`].
pub struct FromIter<I> {
    iter: I,
}

// Sound for the same reason as `Once`: a plain iterator adapter, no pinned
// self-references.
impl<I> Unpin for FromIter<I> {}

impl<I: Iterator> Stream for FromIter<I> {
    type Item = I::Item;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<I::Item>> {
        Poll::Ready(self.get_mut().iter.next())
    }
}

// ── interval (reactor-driven) ────────────────────────────────────────────────

/// A stream that yields `()` every `period`, driven by the reactor timer.
///
/// This is the runtime-specific constructor: each tick is a
/// `rust_io::IoTaskRunner::post_delayed_task` whose firing wakes the parked
/// task — the same delayed-task → `Waker` bridge as [`crate::sleep`].
///
/// The next tick is scheduled when the current one is consumed, so the cadence
/// drifts by however long the consumer takes between polls (it is not a
/// fixed-rate clock). Like [`crate::sleep`], a pending tick is not cancelled
/// when the `Interval` is dropped.
pub fn interval(period: Duration) -> Interval {
    Interval {
        period,
        state: Arc::new(Mutex::new(TickState { fired: false, waker: None })),
        scheduled: false,
    }
}

struct TickState {
    fired: bool,
    waker: Option<Waker>,
}

/// Stream returned by [`interval`].
pub struct Interval {
    period: Duration,
    state: Arc<Mutex<TickState>>,
    scheduled: bool,
}

impl Stream for Interval {
    type Item = ();
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        let this = self.get_mut();
        let mut s = this.state.lock().unwrap();
        if s.fired {
            s.fired = false;
            this.scheduled = false;
            return Poll::Ready(Some(()));
        }
        s.waker = Some(cx.waker().clone());
        if !this.scheduled {
            this.scheduled = true;
            let state = this.state.clone();
            io_runner().post_delayed_task(
                Box::new(move || {
                    let waker = {
                        let mut s = state.lock().unwrap();
                        s.fired = true;
                        s.waker.take()
                    };
                    if let Some(w) = waker {
                        w.wake();
                    }
                }),
                this.period,
            );
        }
        Poll::Pending
    }
}
