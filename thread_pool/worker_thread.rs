use crate::sequence_token::SequenceToken;

// RAII guard: sets the thread-local sequence token on construction, clears it
// on drop.
pub struct ScopedSequenceToken;

impl ScopedSequenceToken {
    pub fn new(token: SequenceToken) -> Self {
        SequenceToken::set_current(Some(token));
        Self
    }
}

impl Drop for ScopedSequenceToken {
    fn drop(&mut self) {
        SequenceToken::set_current(None);
    }
}
