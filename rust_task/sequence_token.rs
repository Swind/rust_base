use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_SEQUENCE_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_SEQUENCE_TOKEN: Cell<Option<SequenceToken>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceToken(u64);

impl SequenceToken {
    pub fn create() -> Self {
        SequenceToken(NEXT_SEQUENCE_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub fn current() -> Option<Self> {
        CURRENT_SEQUENCE_TOKEN.with(|c| c.get())
    }

    pub fn set_current(token: Option<SequenceToken>) {
        CURRENT_SEQUENCE_TOKEN.with(|c| c.set(token));
    }
}
