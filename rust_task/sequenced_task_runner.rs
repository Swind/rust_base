use std::cell::RefCell;
use std::sync::Arc;

use crate::sequence_token::SequenceToken;
use crate::task_runner::TaskRunner;

pub trait SequencedTaskRunner: TaskRunner {
    fn post_non_nestable_task(&self, task: Box<dyn FnOnce() + Send + 'static>) -> bool;
    fn runs_tasks_in_current_sequence(&self) -> bool;
    fn sequence_token(&self) -> SequenceToken;
}

thread_local! {
    static CURRENT_DEFAULT: RefCell<Option<Arc<dyn SequencedTaskRunner>>> =
        RefCell::new(None);
}

pub fn current_default() -> Option<Arc<dyn SequencedTaskRunner>> {
    CURRENT_DEFAULT.with(|c| c.borrow().clone())
}

pub fn has_current_default() -> bool {
    CURRENT_DEFAULT.with(|c| c.borrow().is_some())
}

pub struct CurrentDefaultHandle {
    previous: Option<Arc<dyn SequencedTaskRunner>>,
}

impl CurrentDefaultHandle {
    pub fn new(runner: Arc<dyn SequencedTaskRunner>) -> Self {
        let previous = CURRENT_DEFAULT.with(|c| c.borrow().clone());
        CURRENT_DEFAULT.with(|c| *c.borrow_mut() = Some(runner));
        Self { previous }
    }
}

impl Drop for CurrentDefaultHandle {
    fn drop(&mut self) {
        CURRENT_DEFAULT.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

pub fn delete_soon<T: Send + 'static>(runner: &dyn SequencedTaskRunner, value: Box<T>) -> bool {
    runner.post_task(Box::new(move || drop(value)))
}
