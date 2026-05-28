//! Event bus backed by a SequencedTaskRunner.
//!
//! All subscribe / unsubscribe / publish operations are posted to the same
//! sequence, giving three guarantees for free:
//!
//!  1. **Ordering** — events are dispatched strictly in publication order.
//!  2. **Serialization** — an unsubscribe posted before a publish is guaranteed
//!     to take effect before that publish dispatches; no external locking
//!     needed.
//!  3. **Re-entrancy** — a callback that calls publish() is safe: the new event
//!     is appended to the sequence and dispatched *after* the current event
//!     finishes, never inline.
//!
//! Each internal task is posted via bind_once(Weak<Self>, ...).  Dropping the
//! EventBus frees the object immediately — pending tasks become no-ops rather
//! than keeping the bus alive until the queue drains.
//!
//! Run with:
//!   cargo run --example event_bus

use rust_task::{SequencedTaskRunner, TaskTraits, ThreadPool, bind_once};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

// ── Event type
// ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum AppEvent {
    UserLoggedIn(String),
    MessageSent { from: String, text: String },
    UserLoggedOut(String),
}

// ── EventBus
// ──────────────────────────────────────────────────────────────────

type Callback<E> = Arc<dyn Fn(&E) + Send + Sync + 'static>;

struct BusState<E> {
    subscribers: Vec<(u64, Callback<E>)>,
}

struct EventBus<E: Send + 'static> {
    // Mutex is embedded directly — no extra Arc needed because all tasks
    // reach state through Arc<Self>, not a separately-cloned Arc<Mutex<...>>.
    state: Mutex<BusState<E>>,
    runner: Arc<dyn SequencedTaskRunner>,
    next_id: AtomicU64,
}

impl<E: Send + 'static> EventBus<E> {
    fn new(pool: &Arc<ThreadPool>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BusState { subscribers: Vec::new() }),
            runner: pool.create_sequenced_task_runner(TaskTraits::default()),
            next_id: AtomicU64::new(0),
        })
    }

    // self: &Arc<Self> so we can call Arc::downgrade(self) for bind_once.
    // Callers write bus.subscribe(...) as usual — Rust auto-refs the Arc.
    fn subscribe(self: &Arc<Self>, cb: impl Fn(&E) + Send + Sync + 'static) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.runner.post_task(bind_once(Arc::downgrade(self), move |bus| {
            bus.state.lock().unwrap().subscribers.push((id, Arc::new(cb)));
        }));
        id
    }

    fn unsubscribe(self: &Arc<Self>, id: u64) {
        self.runner.post_task(bind_once(Arc::downgrade(self), move |bus| {
            bus.state.lock().unwrap().subscribers.retain(|(sid, _)| *sid != id);
        }));
    }

    fn publish(self: &Arc<Self>, event: E) {
        self.runner.post_task(bind_once(Arc::downgrade(self), move |bus| {
            let cbs: Vec<Callback<E>> = bus
                .state
                .lock()
                .unwrap()
                .subscribers
                .iter()
                .map(|(_, cb)| Arc::clone(cb))
                .collect();
            for cb in cbs {
                cb(&event);
            }
        }));
    }

    // flush does NOT use bind_once: the done callback must always fire so
    // the caller's barrier is never left waiting forever.
    fn flush(&self, done: impl FnOnce() + Send + 'static) {
        self.runner.post_task(Box::new(done));
    }
}

// ── Helpers
// ───────────────────────────────────────────────────────────────────

fn wait_flush(bus: &EventBus<AppEvent>) {
    let b = Arc::new(Barrier::new(2));
    let bc = Arc::clone(&b);
    bus.flush(move || {
        bc.wait();
    });
    b.wait();
}

// ── Demo ──────────────────────────────────────────────────────────────────────

fn main() {
    let pool = ThreadPool::new(4);
    let bus = EventBus::<AppEvent>::new(&pool);

    let log = Arc::new(Mutex::new(Vec::<String>::new()));

    println!("=== Event Bus Demo ===\n");

    // ── 1. Multiple subscribers ───────────────────────────────────────────────

    println!("Step 1: register logger + audit subscribers, publish login + message");

    let l = Arc::clone(&log);
    bus.subscribe(move |e| {
        let s = match e {
            AppEvent::UserLoggedIn(u) => format!("[logger] login:   {u}"),
            AppEvent::MessageSent { from, text } => format!("[logger] message: {from} → {text}"),
            AppEvent::UserLoggedOut(u) => format!("[logger] logout:  {u}"),
        };
        l.lock().unwrap().push(s);
    });

    let l = Arc::clone(&log);
    let audit_id = bus.subscribe(move |e| {
        if let AppEvent::UserLoggedIn(u) = e {
            l.lock().unwrap().push(format!("[audit ] {u} authenticated"));
        }
    });

    bus.publish(AppEvent::UserLoggedIn("alice".into()));
    bus.publish(AppEvent::MessageSent { from: "alice".into(), text: "hello, world".into() });

    wait_flush(&bus);
    print_log(&log);

    // ── 2. Unsubscribe serialized with publish ────────────────────────────────

    println!("Step 2: unsubscribe audit, then publish logout (audit must NOT see it)");

    bus.unsubscribe(audit_id);
    bus.publish(AppEvent::UserLoggedOut("alice".into()));

    wait_flush(&bus);
    print_log(&log);

    // ── 3. Re-entrant publish ─────────────────────────────────────────────────

    println!("Step 3: auto-welcome subscriber calls publish() inside callback (re-entrant)");

    let bus2 = Arc::clone(&bus);
    let l = Arc::clone(&log);
    bus.subscribe(move |e| {
        if let AppEvent::UserLoggedIn(u) = e {
            l.lock().unwrap().push(format!("[welcom] queuing welcome for {u}"));
            bus2.publish(AppEvent::MessageSent {
                from: "system".into(),
                text: format!("Welcome, {u}!"),
            });
        }
    });

    bus.publish(AppEvent::UserLoggedIn("bob".into()));

    wait_flush(&bus); // drain "login bob" (which enqueues the welcome message)
    wait_flush(&bus); // drain the welcome message

    print_log(&log);

    // ── 4. Dropping the bus cancels pending tasks ─────────────────────────────
    //
    // Because every task is posted via bind_once(Weak<Self>, ...), dropping the
    // last Arc<EventBus> immediately frees the object.  Tasks already in the
    // queue become no-ops — the bus does not stay alive until they drain.

    println!("Step 4: drop bus, verify object is freed immediately");

    {
        let tmp_bus = EventBus::<AppEvent>::new(&pool);
        let weak = Arc::downgrade(&tmp_bus);

        // Post a task that would run after we drop the bus.
        tmp_bus.publish(AppEvent::UserLoggedIn("ghost".into()));

        drop(tmp_bus); // strong count → 0: object freed here

        assert!(weak.upgrade().is_none(), "bus should be freed immediately");
        println!("  bus freed immediately after drop ✓");
    }
    println!();

    pool.shutdown();
}

fn print_log(log: &Mutex<Vec<String>>) {
    let mut guard = log.lock().unwrap();
    for entry in guard.drain(..) {
        println!("  {entry}");
    }
    println!();
}
