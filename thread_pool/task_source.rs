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
    // Phase 3 實作 PooledSequencedTaskRunner 後才會有值
    pub task_runner: Option<Arc<dyn SequencedTaskRunner>>,
}

pub trait TaskSource: Send + Sync {
    fn get_execution_environment(&self) -> ExecutionEnvironment;
    fn get_sort_key(&self) -> TaskSourceSortKey;
    fn has_ready_tasks(&self, now: Instant) -> bool;
    // 所有方法皆為 &self，內部透過 Mutex / AtomicBool 保護狀態
    fn will_run_task(&self) -> RunStatus;
    fn take_task(&self) -> Option<Task>;
    // 回傳 true 代表還有 ready task，呼叫端應重新 enqueue
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
