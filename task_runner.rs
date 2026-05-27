use std::time::Duration;

pub trait TaskRunner: Send + Sync {
    fn post_task(&self, task: Box<dyn FnOnce() + Send + 'static>) -> bool;

    fn post_delayed_task(&self, task: Box<dyn FnOnce() + Send + 'static>, delay: Duration) -> bool;

    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool;
}
