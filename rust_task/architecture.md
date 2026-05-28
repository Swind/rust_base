# Rust Task System 實作架構與計畫

## 1. 專案定位

在 Macadamia 專案中，以 Rust 重新實作 Chromium 的 **Sequenced Task Runner** 和 **Thread Pool** 核心功能。這不是替換 Chromium 的 `base::task`，而是在 `macadamia/task/` 目錄下建立一個全新的 Rust 實作，供未來元件或新架構使用。

### 參考文件

- `base/task/threading_and_tasks.md` — Chromium task 與 threading 架構總覽
- `base/task/sequenced_task_runner_overview.md` — SequencedTaskRunner 詳細流程
- Chromium 原始碼：
  - `base/task/task_runner.h` — TaskRunner 基礎介面
  - `base/task/sequenced_task_runner.h` — SequencedTaskRunner 介面
  - `base/task/task_traits.h` — TaskTraits 定義
  - `base/task/thread_pool.h` — ThreadPool 公開 API
  - `base/task/thread_pool/sequence.h` — Sequence 實作
  - `base/task/thread_pool/pooled_sequenced_task_runner.h` — PooledSequencedTaskRunner
  - `base/task/thread_pool/thread_pool_impl.h` — ThreadPoolImpl
  - `base/task/thread_pool/task_source.h` — TaskSource 抽象

### 範圍限制（第一階段不實作）

- SingleThreadTaskRunner（固定 OS thread）
- COM STA thread（Windows 平台特定）
- BrowserThread（Browser process 特定）
- MessagePump / SequenceManager / RunLoop
- Nested task / nestable task 機制
- PostJob（平行化 job API）
- CancelableTaskTracker
- UpdateableSequencedTaskRunner（動態優先序更新）
- SequenceLocalStorageSlot

---

## 2. 模組結構

```
macadamia/task/
├── BUILD.gn                             # GN 建置檔
├── lib.rs                               # crate root，re-export 公開 API
├── task.rs                              # Task 定義 (closure wrapper)
├── task_traits.rs                       # TaskPriority, TaskShutdownBehavior, TaskTraits
├── task_runner.rs                       # TaskRunner trait
├── sequenced_task_runner.rs             # SequencedTaskRunner trait + current_default helpers
├── sequence_token.rs                    # SequenceToken (唯一識別一個 sequence)
├── thread_pool/
│   ├── mod.rs
│   ├── thread_pool.rs                   # ThreadPool 公開 API
│   ├── sequence.rs                      # Sequence (immediate queue + delayed queue，單一 Mutex)
│   ├── task_source.rs                   # TaskSource trait + RegisteredTaskSource
│   ├── pooled_sequenced_task_runner.rs  # PooledSequencedTaskRunner 實作
│   ├── pooled_parallel_task_runner.rs   # PooledParallelTaskRunner 實作
│   ├── worker_thread.rs                 # Worker thread 管理
│   ├── thread_group.rs                  # ThreadGroup (worker pool + priority queue)
│   ├── priority_queue.rs               # PriorityQueue (排序 task source)
│   ├── delayed_task_manager.rs          # DelayedTaskManager (處理 delayed task)
│   └── task_tracker.rs                  # TaskTracker (shutdown 管理、task lifecycle)
```

---

## 3. 核心概念對應

| Chromium (C++) | Rust | 說明 |
|---|---|---|
| `base::OnceClosure` | `Box<dyn FnOnce() + Send + 'static>` | Rust closure trait，需 `'static` 防止捕捉 stack 引用 |
| `scoped_refptr<T>` | `Arc<T>` | 參考計數 |
| `base::TimeDelta` | `std::time::Duration` | 時間間隔 |
| `base::TimeTicks` | `std::time::Instant` | monotonic 時間 |
| `base::Lock` / `CheckedLock` | `std::sync::Mutex` / `parking_lot::Mutex` | 互斥鎖 |
| `base::ConditionVariable` | `std::sync::Condvar` | 條件變數 |
| `base::AtomicFlag` | `std::sync::atomic::AtomicBool` | 原子旗標 |
| `PooledTaskRunnerDelegate` | trait `PooledTaskRunnerDelegate` | 抽象介面 |
| `TaskSource` / `RegisteredTaskSource` | trait `TaskSource` + struct `RegisteredTaskSource` | task 來源抽象 |
| `Sequence::Transaction` | `MutexGuard<SequenceInner>`（lock guard pattern） | 交易機制，單一鎖保護所有佇列狀態 |
| `SequencedTaskRunner::CurrentDefaultHandle` | module-level `current_default()` + `thread_local!` | thread-local 儲存，不掛在 trait 上 |
| `FROM_HERE` / `base::Location` | `std::panic::Location` 或 `file!() / line!()` | 呼叫位置追蹤 |

---

## 4. 各模組詳細設計

### 4.1 `task.rs` — Task

封裝一個待執行的工作單元。

```rust
pub struct Task {
    pub callback: Box<dyn FnOnce() + Send + 'static>,
    pub posted_from: &'static std::panic::Location<'static>,
    pub delayed_run_time: Option<std::time::Instant>,
    // sequence_num 用於 delayed_queue 同時間時的決定性排序
    pub sequence_num: u64,
}
```

`'static` bound 是必要的：task 會跨 thread 執行，不能捕捉任何非 `'static` 的引用。

對應 Chromium 的 `base/task/thread_pool/task.h`。

### 4.2 `task_traits.rs` — TaskTraits

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    BestEffort,    // 最低優先序
    UserVisible,   // 使用者可見
    UserBlocking,  // 使用者阻擋
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
```

### 4.3 `sequence_token.rs` — SequenceToken

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceToken(u64);

static NEXT_SEQUENCE_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_SEQUENCE_TOKEN: Cell<Option<SequenceToken>> = Cell::new(None);
}

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
```

`Copy` 是必要的：token 只是 `u64`，到處 clone 會增加不必要的摩擦。

### 4.4 `task_runner.rs` — TaskRunner trait

```rust
pub trait TaskRunner: Send + Sync {
    fn post_task(&self, task: Box<dyn FnOnce() + Send + 'static>) -> bool;

    fn post_delayed_task(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        delay: Duration,
    ) -> bool;

    // reply 會被 post 回呼叫當下的 current_default sequence。
    // 實作時必須在 post_task_and_reply 被呼叫的當下捕捉
    // sequenced_task_runner::current_default()，
    // 並在 task 完成後將 reply 投遞到那個 task runner。
    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool;
}
```

### 4.5 `sequenced_task_runner.rs` — SequencedTaskRunner trait

**Object-safety 設計原則**：
- `delete_soon<T>` 改為 module-level free function，不放在 trait 內
- `current_default` / `has_current_default` 是 thread-local 操作，改為 module-level function

```rust
// trait 本身保持 object-safe
pub trait SequencedTaskRunner: TaskRunner {
    fn post_non_nestable_task(&self, task: Box<dyn FnOnce() + Send + 'static>) -> bool;
    fn runs_tasks_in_current_sequence(&self) -> bool;
    fn sequence_token(&self) -> SequenceToken;
}

// thread-local current default — 獨立於 trait
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

// RAII handle，drop 時恢復前一個 default
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

// delete_soon 作為 free function，避免破壞 object safety
pub fn delete_soon<T: Send + 'static>(
    runner: &dyn SequencedTaskRunner,
    value: Box<T>,
) -> bool {
    runner.post_task(Box::new(move || drop(value)))
}
```

### 4.6 `thread_pool/task_source.rs` — TaskSource

**Interior mutability 設計原則**：
`Arc<dyn TaskSource>` 被 `PriorityQueue` 持有，所有方法必須是 `&self`。
內部狀態的修改透過 `Mutex` 保護。這對應 Chromium 的 Transaction 機制（持有 lock 後才能操作內部狀態）。

```rust
pub enum RunStatus {
    Disallowed,
    AllowedNotSaturated,
    AllowedSaturated,
}

pub struct ExecutionEnvironment {
    pub token: SequenceToken,
    pub task_runner: Arc<dyn SequencedTaskRunner>,
}

pub trait TaskSource: Send + Sync {
    fn get_execution_environment(&self) -> ExecutionEnvironment;
    fn get_sort_key(&self) -> TaskSourceSortKey;
    fn has_ready_tasks(&self, now: Instant) -> bool;

    // 以下方法雖取 &self，內部透過 Mutex 保護狀態修改
    fn will_run_task(&self) -> RunStatus;
    fn take_task(&self) -> Option<Task>;
    fn did_process_task(&self) -> bool;  // 回傳 true 表示還有 ready task，需重新 enqueue
    fn will_re_enqueue(&self, now: Instant) -> bool;
}

// RAII wrapper，代表已從 PriorityQueue 取出、正在執行的 TaskSource。
// Drop 時通知 ThreadGroup 此 source 已完成，可以重新 enqueue。
pub struct RegisteredTaskSource {
    source: Arc<dyn TaskSource>,
    thread_group: Weak<ThreadGroup>,
}

impl RegisteredTaskSource {
    pub fn source(&self) -> &Arc<dyn TaskSource> {
        &self.source
    }
}

impl Drop for RegisteredTaskSource {
    fn drop(&mut self) {
        // 通知 ThreadGroup：若 did_process_task() 為 true 則重新 enqueue
        if let Some(group) = self.thread_group.upgrade() {
            if self.source.did_process_task() {
                group.re_enqueue(self.source.clone());
            }
        }
    }
}
```

### 4.7 `thread_pool/sequence.rs` — Sequence

**單一 Mutex 設計**：`immediate_queue` 和 `delayed_queue` 合併在同一個 `Mutex<SequenceInner>` 內，避免雙鎖競態。`take_task` 在持有鎖的情況下同時查看兩個佇列，對應 Chromium 的 Transaction。

```rust
struct SequenceInner {
    immediate_queue: VecDeque<Task>,
    // (ready_time, sequence_num) 用於排序，sequence_num 保證決定性順序
    delayed_queue: BinaryHeap<Reverse<(Instant, u64, Task)>>,
    next_sequence_num: u64,
}

pub struct Sequence {
    token: SequenceToken,
    inner: Mutex<SequenceInner>,
    has_worker: AtomicBool,          // 是否有 worker 正在執行此 sequence
    traits: TaskTraits,
    task_runner: Weak<dyn SequencedTaskRunner>,  // 避免循環引用
}

impl Sequence {
    // push_immediate_task / push_delayed_task 在 lock 內完成
    // take_task：先從 immediate_queue 取，若空則從 delayed_queue 取已到期的任務
    // did_process_task：設定 has_worker = false，回傳是否還有 ready task
}

impl TaskSource for Sequence {
    fn take_task(&self) -> Option<Task> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        // 先把到期的 delayed task 移至 immediate_queue
        while let Some(Reverse((ready_time, _, _))) = inner.delayed_queue.peek() {
            if *ready_time <= now {
                let Reverse((_, _, task)) = inner.delayed_queue.pop().unwrap();
                inner.immediate_queue.push_back(task);
            } else {
                break;
            }
        }
        inner.immediate_queue.pop_front()
    }
    // ...
}
```

**Delayed task 職責分工**（與 DelayedTaskManager 的邊界）：

- `Sequence.delayed_queue`：存放「已屬於此 sequence、但尚未到期」的 task。由 `DelayedTaskManager` 在時間到時通知 Sequence，Sequence 才把它移入 immediate_queue 供執行。
- `DelayedTaskManager`：作為全域 timer 觸發器，持有 `(ready_time, Arc<Sequence>)` 的 min-heap，喚醒後呼叫對應 Sequence 的 `notify_delayed_task_ready()`，再由 Sequence 自行處理佇列移動並通知 ThreadGroup 重新 enqueue。

這樣 Sequence 是唯一修改自己佇列的地方，DelayedTaskManager 只負責計時與通知。

### 4.8 `thread_pool/priority_queue.rs` — PriorityQueue

`Arc<dyn TaskSource>` 不實作 `Ord`，需要 newtype wrapper 讓 `BinaryHeap` 只比較 sort key：

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskSourceSortKey {
    priority: TaskPriority,    // 高優先序先執行（需 Reverse 或反轉 Ord）
    ready_time: Instant,       // 同 priority 時，越早 ready 越優先
}

struct QueueEntry {
    sort_key: TaskSourceSortKey,
    task_source: Arc<dyn TaskSource>,
}

// 只用 sort_key 比較，Arc<dyn TaskSource> 不參與排序
impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool { self.sort_key == other.sort_key }
}
impl Eq for QueueEntry {}
impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap 是 max-heap，priority 高且 ready_time 早的要排在前面
        other.sort_key.cmp(&self.sort_key)
    }
}

pub struct PriorityQueue {
    heap: BinaryHeap<QueueEntry>,
}

impl PriorityQueue {
    pub fn push(&mut self, task_source: Arc<dyn TaskSource>) {
        let sort_key = task_source.get_sort_key();
        self.heap.push(QueueEntry { sort_key, task_source });
    }

    pub fn pop(&mut self) -> Option<Arc<dyn TaskSource>> {
        self.heap.pop().map(|e| e.task_source)
    }
}
```

### 4.9 `thread_pool/pooled_sequenced_task_runner.rs` — PooledSequencedTaskRunner

```rust
pub struct PooledSequencedTaskRunner {
    sequence: Arc<Sequence>,
    delegate: Arc<dyn PooledTaskRunnerDelegate>,
    traits: TaskTraits,
}

impl TaskRunner for PooledSequencedTaskRunner {
    fn post_task(&self, callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
        let task = Task { callback, posted_from: std::panic::Location::caller(), .. };
        self.delegate.post_task_with_sequence(task, self.sequence.clone())
    }

    fn post_task_and_reply(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        reply: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool {
        // 在呼叫當下捕捉 caller 的 current_default，reply 投遞回去
        let reply_runner = sequenced_task_runner::current_default();
        let wrapped = Box::new(move || {
            task();
            if let Some(runner) = reply_runner {
                runner.post_task(reply);
            }
        });
        self.post_task(wrapped)
    }
    // ...
}

impl SequencedTaskRunner for PooledSequencedTaskRunner {
    fn runs_tasks_in_current_sequence(&self) -> bool {
        SequenceToken::current() == Some(self.sequence.token())
    }

    fn sequence_token(&self) -> SequenceToken {
        self.sequence.token()
    }
}
```

### 4.10 `thread_pool/worker_thread.rs` — WorkerThread

Worker 主迴圈在取得 `RegisteredTaskSource` 後，設定 thread-local 的 `SequenceToken` 和 `CurrentDefaultHandle`，執行 task 後清除。

```rust
pub struct WorkerThread {
    handle: Option<std::thread::JoinHandle<()>>,
    // 每個 worker 有獨立的 wake 信號
    wake: Arc<(Mutex<bool>, Condvar)>,
}

// Worker 主迴圈（pseudo code）：
// loop {
//     let registered = thread_group.get_work();  // 阻塞直到有 task 或 shutdown
//     if shutdown { break; }
//     if let Some(registered) = registered {
//         let env = registered.source().get_execution_environment();
//         SequenceToken::set_current(Some(env.token));
//         let _handle = CurrentDefaultHandle::new(env.task_runner.clone());
//         if let RunStatus::Allowed* = registered.source().will_run_task() {
//             if let Some(task) = registered.source().take_task() {
//                 (task.callback)();
//             }
//         }
//         SequenceToken::set_current(None);
//         // registered drop 時自動通知 ThreadGroup re-enqueue
//     }
// }
```

### 4.11 `thread_pool/thread_group.rs` — ThreadGroup

```rust
pub struct ThreadGroup {
    workers: Mutex<Vec<WorkerThread>>,
    priority_queue: Mutex<PriorityQueue>,
    // 每個 worker 的獨立 wake 信號，index 對應 workers
    worker_wakes: Vec<Arc<(Mutex<bool>, Condvar)>>,
    shutdown: AtomicBool,
}

impl ThreadGroup {
    // push_task_source：enqueue 後 notify_one 喚醒一個閒置 worker
    // get_work：worker 呼叫，若無 task 則在自己的 Condvar 上 wait；shutdown 時返回 None
    // re_enqueue：RegisteredTaskSource drop 時呼叫，重新放入 priority_queue 並 notify_one
    // join_all：broadcast shutdown，等待所有 worker thread 結束
}
```

**喚醒策略**：
- `push_task_source` / `re_enqueue`：`notify_one`，只喚醒一個 worker 避免 thundering herd
- `shutdown`：`notify_all`，確保所有 worker 都能看到 shutdown 信號

### 4.12 `thread_pool/delayed_task_manager.rs` — DelayedTaskManager

```rust
struct DelayedEntry {
    ready_time: Instant,
    sequence: Arc<Sequence>,
    task: Task,
}

pub struct DelayedTaskManager {
    inner: Mutex<BinaryHeap<Reverse<DelayedEntry>>>,
    condvar: Condvar,
    thread_group: Weak<ThreadGroup>,
    wake_thread: Option<std::thread::JoinHandle<()>>,
}

// timer 主迴圈（pseudo code）：
// loop {
//     let next = inner.lock().peek().map(|e| e.ready_time);
//     match next {
//         None => condvar.wait(lock),   // 無 task，等到有新的加入
//         Some(t) => {
//             let now = Instant::now();
//             if t <= now {
//                 // 取出到期 task，通知對應 Sequence，再通知 ThreadGroup
//                 let entry = inner.pop();
//                 entry.sequence.push_immediate_task(entry.task);
//                 if let Some(tg) = thread_group.upgrade() {
//                     tg.push_task_source(entry.sequence);
//                 }
//             } else {
//                 condvar.wait_timeout(lock, t - now);
//             }
//         }
//     }
// }
```

### 4.13 `thread_pool/task_tracker.rs` — TaskTracker

```rust
pub struct TaskTracker {
    shutdown_complete: AtomicBool,
    num_tasks_blocking_shutdown: AtomicUsize,
    shutdown_event: (Mutex<bool>, Condvar),
}

impl TaskTracker {
    pub fn will_post_task(&self, traits: &TaskTraits) -> bool {
        // shutdown 後拒絕 ContinueOnShutdown 以外的 task（依 behavior 決定）
    }
    pub fn run_task(&self, task: Task, traits: &TaskTraits) {
        // BlockShutdown task 執行前 num_tasks_blocking_shutdown += 1，完成後 -= 1
    }
    pub fn shutdown(&self) {
        // 等待 num_tasks_blocking_shutdown 歸零
    }
}
```

### 4.14 `thread_pool/thread_pool.rs` — ThreadPool

```rust
pub struct ThreadPool {
    task_tracker: Arc<TaskTracker>,
    delayed_task_manager: Arc<DelayedTaskManager>,
    foreground_thread_group: Arc<ThreadGroup>,
    background_thread_group: Option<Arc<ThreadGroup>>,
}

impl ThreadPool {
    pub fn new(name: &str) -> Self;
    pub fn start(&self, params: InitParams);
    pub fn shutdown(&self);

    pub fn post_task(
        &self,
        traits: TaskTraits,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> bool;

    pub fn post_delayed_task(
        &self,
        traits: TaskTraits,
        task: Box<dyn FnOnce() + Send + 'static>,
        delay: Duration,
    ) -> bool;

    pub fn create_task_runner(&self, traits: TaskTraits) -> Arc<dyn TaskRunner>;

    pub fn create_sequenced_task_runner(
        &self,
        traits: TaskTraits,
    ) -> Arc<dyn SequencedTaskRunner>;
}
```

---

## 5. 分階段實作計畫

### Phase 1：基礎建設

建構型別定義與 trait 介面，不涉及執行邏輯。

| # | 檔案 | 內容 |
|---|------|------|
| 1 | `lib.rs` | crate root，`pub mod` 所有模組 |
| 2 | `task.rs` | Task struct（含 `sequence_num`、`'static` bound） |
| 3 | `task_traits.rs` | TaskPriority, TaskShutdownBehavior, ThreadPolicy, TaskTraits（含 Default） |
| 4 | `sequence_token.rs` | SequenceToken（Copy + thread-local storage） |
| 5 | `task_runner.rs` | TaskRunner trait（含 `'static` bound） |
| 6 | `sequenced_task_runner.rs` | SequencedTaskRunner trait + module-level current_default helpers + CurrentDefaultHandle |

### Phase 2：Thread Pool 核心

| # | 檔案 | 內容 |
|---|------|------|
| 7 | `thread_pool/task_source.rs` | TaskSource trait（全 `&self` + interior mutability）+ RegisteredTaskSource RAII |
| 8 | `thread_pool/sequence.rs` | Sequence（單一 `Mutex<SequenceInner>`，兩個佇列合一鎖） |
| 9 | `thread_pool/priority_queue.rs` | PriorityQueue（QueueEntry newtype，自訂 Ord） |
| 10 | `thread_pool/worker_thread.rs` | WorkerThread（worker 主迴圈） |
| 11 | `thread_pool/thread_group.rs` | ThreadGroup（per-worker Condvar，notify_one/all 策略） |

### Phase 3：TaskRunner 實作

| # | 檔案 | 內容 |
|---|------|------|
| 12 | `thread_pool/pooled_sequenced_task_runner.rs` | PooledSequencedTaskRunner（含 post_task_and_reply reply dispatch） |
| 13 | `thread_pool/pooled_parallel_task_runner.rs` | PooledParallelTaskRunner |

### Phase 4：ThreadPool 整合

| # | 檔案 | 內容 |
|---|------|------|
| 14 | `thread_pool/delayed_task_manager.rs` | timer thread + Sequence 通知 |
| 15 | `thread_pool/task_tracker.rs` | Shutdown 管理、BlockShutdown 計數 |
| 16 | `thread_pool/thread_pool.rs` | ThreadPool 公開 API |

### Phase 5：建置與測試

| # | 項目 | 內容 |
|---|------|------|
| 17 | `BUILD.gn` | `rust_static_library` 模板 |
| 18 | 單元測試 | `#[cfg(test)]` |

---

## 6. BUILD.gn 模板

```gn
import("//build/rust/rust_static_library.gni")

rust_static_library("task") {
  crate_root = "lib.rs"
  allow_unsafe = true

  sources = [
    "lib.rs",
    "task.rs",
    "task_traits.rs",
    "task_runner.rs",
    "sequenced_task_runner.rs",
    "sequence_token.rs",
    "thread_pool/mod.rs",
    "thread_pool/task_source.rs",
    "thread_pool/sequence.rs",
    "thread_pool/priority_queue.rs",
    "thread_pool/worker_thread.rs",
    "thread_pool/thread_group.rs",
    "thread_pool/pooled_sequenced_task_runner.rs",
    "thread_pool/pooled_parallel_task_runner.rs",
    "thread_pool/delayed_task_manager.rs",
    "thread_pool/task_tracker.rs",
    "thread_pool/thread_pool.rs",
  ]
}
```

---

## 7. 關鍵設計決策

### 7.1 Object Safety 的保持

`SequencedTaskRunner` trait 保持 object-safe，只包含可以透過 `dyn` 呼叫的方法：
- `delete_soon<T>` 改為 module-level free function
- `current_default` / `has_current_default` 改為 module-level function（因為它們是 thread-local global state，掛在 trait 上不合語意）

### 7.2 Interior Mutability 一致性

`Arc<dyn TaskSource>` 被 `PriorityQueue` 持有，所有 `TaskSource` 方法取 `&self`。內部狀態修改一律透過 `Mutex`，對應 Chromium 的 Transaction 機制（持有 lock guard 期間才能安全操作佇列）。

### 7.3 Sequence 使用單一鎖

`immediate_queue` 和 `delayed_queue` 合在同一個 `Mutex<SequenceInner>` 內。`take_task` 持有鎖後先把到期的 delayed task 移進 immediate_queue，再從 immediate_queue 取出，避免雙鎖競態（TOCTOU）。

### 7.4 DelayedTaskManager 只負責計時通知

`DelayedTaskManager` 持有 `(ready_time, Arc<Sequence>, Task)` 的 min-heap，時間到時呼叫 `sequence.push_immediate_task(task)` 並通知 `ThreadGroup`。Sequence 是唯一修改自身佇列的地方。

### 7.5 喚醒策略

- 有新 task 時：`notify_one`，避免 thundering herd
- shutdown 時：`notify_all`，確保所有 worker 退出

### 7.6 `post_task_and_reply` 的 reply dispatch

在 `post_task_and_reply` 被呼叫的當下捕捉 `sequenced_task_runner::current_default()`，將其 clone 到 task closure 內。task 執行完成後，reply 被投遞到捕捉到的那個 task runner，確保 reply 在 caller 的 sequence 上執行。

### 7.7 不需要 Chromium 特有的機制

| 省略項目 | 原因 |
|----------|------|
| `base::OnceClosure` / `base::BindOnce` | `Box<dyn FnOnce() + Send + 'static>` 直接取代 |
| `scoped_refptr` | `Arc<T>` 完全等價 |
| `FROM_HERE` / `base::Location` | `std::panic::Location` 或 `file!() / line!()` |
| Nested task / non-nestable task | ThreadPool 不處理 nested run loop |
| COM STA / BrowserThread / MessagePump | 平台或 process 特有，不在範圍內 |

---

## 8. 測試策略

### 8.1 單元測試

| 模組 | 測試項目 |
|------|----------|
| `Sequence` | immediate/delayed 佇列操作、has_worker 狀態轉換、take_task 到期邏輯 |
| `PriorityQueue` | sort key 排序正確性（高 priority 先出、同 priority 按 ready_time） |
| `SequenceToken` | thread-local 設定 / 清除、多 thread 獨立性 |
| `CurrentDefaultHandle` | drop 恢復前一個 default |

### 8.2 整合測試

- 多 thread post task 到同一 `SequencedTaskRunner`，驗證執行順序
- 不同 `SequencedTaskRunner` 的 task 確實並行
- `post_task_and_reply`：reply 確實在 caller 的 sequence 上執行
- Delayed task 在正確時間執行
- Shutdown：未執行的 task 依 `TaskShutdownBehavior` 處理

---

## 9. 時程估計

| Phase | 工作項目 | 預估時間 |
|-------|----------|----------|
| Phase 1 | 基礎型別與 trait | 2-3 天 |
| Phase 2 | ThreadPool 核心引擎 | 5-7 天 |
| Phase 3 | TaskRunner 實作 | 2-3 天 |
| Phase 4 | ThreadPool 整合 | 3-4 天 |
| Phase 5 | 建置整合與測試 | 2-3 天 |
| **合計** | | **14-20 天** |
