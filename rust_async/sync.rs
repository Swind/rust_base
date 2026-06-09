//! Async synchronization primitives, mirroring `async_std::sync`:
//! [`Mutex`], [`RwLock`], [`Barrier`], and an MPMC [`channel`].
//!
//! Unlike the rest of the crate these need nothing from `rust_task`/`rust_io` —
//! they are pure Future-level primitives. The shape is always the same: a
//! `std::sync::Mutex` guards the bookkeeping, and a task that cannot proceed
//! parks its [`Waker`] in a wait set; whoever releases the resource wakes the
//! parked tasks, which re-contend on their next poll.
//!
//! ## Simplification vs a production runtime
//!
//! Releases wake **all** waiters of the relevant kind (a small thundering herd)
//! rather than handing the resource to exactly one. This keeps the primitives
//! obviously correct under cancellation — a woken-then-dropped future can never
//! strand the others — at the cost of some redundant polls. Fairness is
//! best-effort FIFO via monotonic wait ids.
//!
//! [`RwLock`] is read-preferring with no writer preference: a reader acquires
//! whenever no writer is *active*, so a steady stream of readers can starve a
//! waiting writer. Use [`Mutex`] if you need writers to make progress under
//! continuous read load.

use std::cell::UnsafeCell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, Waker};

// ── wait set ────────────────────────────────────────────────────────────────

/// A set of parked wakers keyed by a monotonic id, so a future that is polled
/// repeatedly updates its single entry (instead of leaking duplicates) and can
/// remove itself on drop/cancellation.
#[derive(Default)]
struct Waiters {
    next: usize,
    map: BTreeMap<usize, Waker>,
}

impl Waiters {
    /// Park `waker` under the future's id, allocating one on first use.
    fn park(&mut self, id: &mut Option<usize>, waker: &Waker) {
        let key = *id.get_or_insert_with(|| {
            let k = self.next;
            self.next += 1;
            k
        });
        self.map.insert(key, waker.clone());
    }

    /// Forget the future's parked waker, if any.
    fn unpark(&mut self, id: Option<usize>) {
        if let Some(k) = id {
            self.map.remove(&k);
        }
    }

    /// Wake (and drop) every parked waker.
    fn wake_all(&mut self) {
        for (_, w) in std::mem::take(&mut self.map) {
            w.wake();
        }
    }
}

// ── Mutex ─────────────────────────────────────────────────────────────────

struct MutexInner {
    locked: bool,
    waiters: Waiters,
}

/// An async mutex. `lock().await` resolves to a guard; dropping the guard
/// releases the lock and wakes waiters.
pub struct Mutex<T: ?Sized> {
    inner: StdMutex<MutexInner>,
    data: UnsafeCell<T>,
}

// SAFETY: access to `data` is serialized by the boolean `locked` flag, which is
// itself guarded by `inner`. A guard handing out `&mut T` exists only while
// `locked` is true and no other guard can be created.
unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Create a new unlocked mutex.
    pub fn new(value: T) -> Mutex<T> {
        Mutex {
            inner: StdMutex::new(MutexInner { locked: false, waiters: Waiters::default() }),
            data: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Acquire the lock, waiting if it is held.
    pub fn lock(&self) -> Lock<'_, T> {
        Lock { mutex: self, id: None }
    }

    /// Try to acquire without waiting.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let mut s = self.inner.lock().unwrap();
        if s.locked {
            None
        } else {
            s.locked = true;
            Some(MutexGuard { mutex: self })
        }
    }
}

/// Future returned by [`Mutex::lock`].
pub struct Lock<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    id: Option<usize>,
}

impl<'a, T: ?Sized> Future for Lock<'a, T> {
    type Output = MutexGuard<'a, T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut s = this.mutex.inner.lock().unwrap();
        if !s.locked {
            s.locked = true;
            s.waiters.unpark(this.id.take());
            return Poll::Ready(MutexGuard { mutex: this.mutex });
        }
        s.waiters.park(&mut this.id, cx.waker());
        Poll::Pending
    }
}

impl<T: ?Sized> Drop for Lock<'_, T> {
    fn drop(&mut self) {
        if self.id.is_some() {
            self.mutex.inner.lock().unwrap().waiters.unpark(self.id);
        }
    }
}

/// RAII guard from [`Mutex::lock`]; releases on drop.
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: we hold the lock for our whole lifetime.
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: we hold the lock exclusively for our whole lifetime.
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        let mut s = self.mutex.inner.lock().unwrap();
        s.locked = false;
        s.waiters.wake_all();
    }
}

// ── RwLock ────────────────────────────────────────────────────────────────

struct RwInner {
    readers: usize,
    writer: bool,
    read_waiters: Waiters,
    write_waiters: Waiters,
}

/// An async reader–writer lock. Many readers or one writer.
pub struct RwLock<T: ?Sized> {
    inner: StdMutex<RwInner>,
    data: UnsafeCell<T>,
}

// SAFETY: as for `Mutex` — `inner` serializes the reader count / writer flag
// that gate access to `data`.
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    /// Create a new, unlocked lock.
    pub fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: StdMutex::new(RwInner {
                readers: 0,
                writer: false,
                read_waiters: Waiters::default(),
                write_waiters: Waiters::default(),
            }),
            data: UnsafeCell::new(value),
        }
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Acquire a shared read lock.
    pub fn read(&self) -> Read<'_, T> {
        Read { lock: self, id: None }
    }

    /// Acquire an exclusive write lock.
    pub fn write(&self) -> Write<'_, T> {
        Write { lock: self, id: None }
    }
}

/// Future returned by [`RwLock::read`].
pub struct Read<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    id: Option<usize>,
}

impl<'a, T: ?Sized> Future for Read<'a, T> {
    type Output = RwLockReadGuard<'a, T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut s = this.lock.inner.lock().unwrap();
        if !s.writer {
            s.readers += 1;
            s.read_waiters.unpark(this.id.take());
            return Poll::Ready(RwLockReadGuard { lock: this.lock });
        }
        s.read_waiters.park(&mut this.id, cx.waker());
        Poll::Pending
    }
}

impl<T: ?Sized> Drop for Read<'_, T> {
    fn drop(&mut self) {
        if self.id.is_some() {
            self.lock.inner.lock().unwrap().read_waiters.unpark(self.id);
        }
    }
}

/// Future returned by [`RwLock::write`].
pub struct Write<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    id: Option<usize>,
}

impl<'a, T: ?Sized> Future for Write<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut s = this.lock.inner.lock().unwrap();
        if !s.writer && s.readers == 0 {
            s.writer = true;
            s.write_waiters.unpark(this.id.take());
            return Poll::Ready(RwLockWriteGuard { lock: this.lock });
        }
        s.write_waiters.park(&mut this.id, cx.waker());
        Poll::Pending
    }
}

impl<T: ?Sized> Drop for Write<'_, T> {
    fn drop(&mut self) {
        if self.id.is_some() {
            self.lock.inner.lock().unwrap().write_waiters.unpark(self.id);
        }
    }
}

/// Shared read guard from [`RwLock::read`].
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a read guard guarantees no writer is active.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        let mut s = self.lock.inner.lock().unwrap();
        s.readers -= 1;
        if s.readers == 0 {
            s.write_waiters.wake_all();
        }
    }
}

/// Exclusive write guard from [`RwLock::write`].
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a write guard is exclusive.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: a write guard is exclusive.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        let mut s = self.lock.inner.lock().unwrap();
        s.writer = false;
        // Hand off to whoever is waiting; readers and writers re-contend.
        s.write_waiters.wake_all();
        s.read_waiters.wake_all();
    }
}

// ── Barrier ───────────────────────────────────────────────────────────────

struct BarrierInner {
    count: usize,
    generation: usize,
    waiters: Waiters,
}

/// A barrier that releases all participants once `n` of them are waiting.
pub struct Barrier {
    n: usize,
    inner: StdMutex<BarrierInner>,
}

/// Result of [`Barrier::wait`]; exactly one participant per generation is the
/// leader.
pub struct BarrierWaitResult {
    is_leader: bool,
}

impl BarrierWaitResult {
    /// Whether this participant was chosen as the leader (the one whose arrival
    /// completed the group).
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

impl Barrier {
    /// Create a barrier that trips once `n` participants are waiting.
    pub fn new(n: usize) -> Barrier {
        Barrier {
            n,
            inner: StdMutex::new(BarrierInner {
                count: 0,
                generation: 0,
                waiters: Waiters::default(),
            }),
        }
    }

    /// Wait until `n` participants (including this one) have called `wait`.
    pub fn wait(&self) -> BarrierWait<'_> {
        BarrierWait { barrier: self, arrived_at: None, id: None }
    }
}

/// Future returned by [`Barrier::wait`].
pub struct BarrierWait<'a> {
    barrier: &'a Barrier,
    /// Generation we joined; `None` until we have been counted.
    arrived_at: Option<usize>,
    id: Option<usize>,
}

impl Future for BarrierWait<'_> {
    type Output = BarrierWaitResult;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BarrierWaitResult> {
        let this = self.get_mut();
        let mut s = this.barrier.inner.lock().unwrap();
        match this.arrived_at {
            None => {
                // First poll: join the current generation.
                s.count += 1;
                let current_gen = s.generation;
                this.arrived_at = Some(current_gen);
                if s.count >= this.barrier.n {
                    // We complete the group: trip the barrier.
                    s.count = 0;
                    s.generation += 1;
                    s.waiters.wake_all();
                    Poll::Ready(BarrierWaitResult { is_leader: true })
                } else {
                    s.waiters.park(&mut this.id, cx.waker());
                    Poll::Pending
                }
            }
            Some(joined_gen) => {
                if s.generation != joined_gen {
                    s.waiters.unpark(this.id.take());
                    Poll::Ready(BarrierWaitResult { is_leader: false })
                } else {
                    s.waiters.park(&mut this.id, cx.waker());
                    Poll::Pending
                }
            }
        }
    }
}

// ── channel (unbounded MPMC) ────────────────────────────────────────────────

struct ChanInner<T> {
    items: VecDeque<T>,
    recv_waiters: Waiters,
    senders: usize,
    receivers: usize,
}

/// Create an unbounded multi-producer multi-consumer channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(StdMutex::new(ChanInner {
        items: VecDeque::new(),
        recv_waiters: Waiters::default(),
        senders: 1,
        receivers: 1,
    }));
    (Sender { chan: chan.clone() }, Receiver { chan })
}

/// Error returned by [`Sender::send`] when every receiver is gone; carries the
/// value back.
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed channel")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error returned by [`Receiver::recv`] when the channel is empty and every
/// sender is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receiving on an empty and closed channel")
    }
}

impl std::error::Error for RecvError {}

/// Sending half of [`channel`].
pub struct Sender<T> {
    chan: Arc<StdMutex<ChanInner<T>>>,
}

impl<T> Sender<T> {
    /// Send a value. Never blocks (the channel is unbounded). Fails only if all
    /// receivers have been dropped.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut s = self.chan.lock().unwrap();
        if s.receivers == 0 {
            return Err(SendError(value));
        }
        s.items.push_back(value);
        s.recv_waiters.wake_all();
        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.chan.lock().unwrap().senders += 1;
        Sender { chan: self.chan.clone() }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut s = self.chan.lock().unwrap();
        s.senders -= 1;
        if s.senders == 0 {
            // Wake blocked receivers so they observe the close.
            s.recv_waiters.wake_all();
        }
    }
}

/// Receiving half of [`channel`].
pub struct Receiver<T> {
    chan: Arc<StdMutex<ChanInner<T>>>,
}

impl<T> Receiver<T> {
    /// Receive the next value, waiting if the channel is empty. Returns
    /// [`RecvError`] once the channel is empty and all senders are gone.
    pub fn recv(&self) -> Recv<'_, T> {
        Recv { chan: &self.chan, id: None }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.chan.lock().unwrap().receivers += 1;
        Receiver { chan: self.chan.clone() }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.chan.lock().unwrap().receivers -= 1;
    }
}

/// Future returned by [`Receiver::recv`].
pub struct Recv<'a, T> {
    chan: &'a Arc<StdMutex<ChanInner<T>>>,
    id: Option<usize>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, RecvError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let this = self.get_mut();
        let mut s = this.chan.lock().unwrap();
        if let Some(v) = s.items.pop_front() {
            s.recv_waiters.unpark(this.id.take());
            return Poll::Ready(Ok(v));
        }
        if s.senders == 0 {
            s.recv_waiters.unpark(this.id.take());
            return Poll::Ready(Err(RecvError));
        }
        s.recv_waiters.park(&mut this.id, cx.waker());
        Poll::Pending
    }
}

impl<T> Drop for Recv<'_, T> {
    fn drop(&mut self) {
        if self.id.is_some() {
            self.chan.lock().unwrap().recv_waiters.unpark(self.id);
        }
    }
}
