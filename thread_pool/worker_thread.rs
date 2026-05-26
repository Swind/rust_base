use crate::sequence_token::SequenceToken;

// RAII：建立時設定 thread-local sequence token，drop 時清除
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
