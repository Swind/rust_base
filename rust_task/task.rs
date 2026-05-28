use std::time::Instant;

pub struct Task {
    pub callback: Box<dyn FnOnce() + Send + 'static>,
    pub posted_from: &'static std::panic::Location<'static>,
    pub delayed_run_time: Option<Instant>,
    pub sequence_num: u64,
}

impl Task {
    #[track_caller]
    pub fn new(callback: Box<dyn FnOnce() + Send + 'static>) -> Self {
        Self {
            callback,
            posted_from: std::panic::Location::caller(),
            delayed_run_time: None,
            sequence_num: 0,
        }
    }

    #[track_caller]
    pub fn new_delayed(callback: Box<dyn FnOnce() + Send + 'static>, run_time: Instant) -> Self {
        Self {
            callback,
            posted_from: std::panic::Location::caller(),
            delayed_run_time: Some(run_time),
            sequence_num: 0,
        }
    }
}
