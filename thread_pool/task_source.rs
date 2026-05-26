use std::sync::Arc;
use std::time::Instant;

use crate::sequence_token::SequenceToken;
use crate::sequenced_task_runner::SequencedTaskRunner;
use crate::task::Task;
use crate::task_traits::TaskPriority;

pub enum RunStatus {
    Disallowed,
    AllowedNotSaturated,
    AllowedSaturated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskSourceSortKey {
    pub priority: TaskPriority,
    pub ready_time: Instant,
}

pub struct ExecutionEnvironment {
    pub token: SequenceToken,
    // Populated once PooledSequencedTaskRunner is wired up in Phase 3.
    pub task_runner: Option<Arc<dyn SequencedTaskRunner>>,
}

pub trait TaskSource: Send + Sync {
    fn get_execution_environment(&self) -> ExecutionEnvironment;
    fn get_sort_key(&self) -> TaskSourceSortKey;
    fn has_ready_tasks(&self, now: Instant) -> bool;
    // All methods take &self; internal state is mutated through Mutex / AtomicBool.
    fn will_run_task(&self) -> RunStatus;
    fn take_task(&self) -> Option<Task>;
    // Returns true if more ready tasks remain; the caller should re-enqueue this source.
    fn did_process_task(&self) -> bool;
    fn will_re_enqueue(&self, now: Instant) -> bool;
}

pub struct RegisteredTaskSource {
    source: Arc<dyn TaskSource>,
}

impl RegisteredTaskSource {
    pub fn new(source: Arc<dyn TaskSource>) -> Self {
        Self { source }
    }

    pub fn source(&self) -> &Arc<dyn TaskSource> {
        &self.source
    }

    pub fn into_source(self) -> Arc<dyn TaskSource> {
        self.source
    }
}
