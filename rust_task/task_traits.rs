#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    BestEffort,
    UserVisible,
    UserBlocking,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskShutdownBehavior {
    ContinueOnShutdown,
    SkipOnShutdown,
    BlockShutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadPolicy {
    PreferBackground,
    MustUseForeground,
}

#[derive(Clone, Copy, Debug)]
pub struct TaskTraits {
    pub priority: TaskPriority,
    pub shutdown_behavior: TaskShutdownBehavior,
    pub thread_policy: ThreadPolicy,
    pub may_block: bool,
}

impl Default for TaskTraits {
    fn default() -> Self {
        Self {
            priority: TaskPriority::UserVisible,
            shutdown_behavior: TaskShutdownBehavior::SkipOnShutdown,
            thread_policy: ThreadPolicy::PreferBackground,
            may_block: false,
        }
    }
}
