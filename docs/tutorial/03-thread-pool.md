# 第 3 章 ThreadPool、TaskRunner 與 TaskTraits

> 本章 Chromium 素材：[`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) 的 Posting a Parallel
> Task、Annotating Tasks with TaskTraits 兩節、
> [`base/task/task_runner.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/task_runner.h)、
> [`base/task/task_traits.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/task_traits.h)。
> 本章 Rust 素材：Rust Book [ch6 enum 與 match](https://rust-lang.tw/book-tw/ch06-00-enums.html)、
> [ch5 struct](https://rust-lang.tw/book-tw/ch05-00-structs.html)、
> [ch10-02 trait](https://rust-lang.tw/book-tw/ch10-02-traits.html)、
> [ch15-04 Rc/Arc](https://rust-lang.tw/book-tw/ch15-04-rc.html)。
> 本章主角程式碼：[`rust_task/thread_pool/thread_pool.rs`](../../rust_task/thread_pool/thread_pool.rs)、
> [`rust_task/task_traits.rs`](../../rust_task/task_traits.rs)、
> [`rust_task/task_runner.rs`](../../rust_task/task_runner.rs)。

## 3.1 最小可用範例

Chromium 中 post 一個「平行任務」（不需要順序保證、可在任何 worker 上跑）：

```cpp
base::ThreadPool::PostTask(FROM_HERE, base::BindOnce(&Task));

// 帶 traits 的版本
base::ThreadPool::PostTask(
    FROM_HERE, {base::TaskPriority::BEST_EFFORT, base::MayBlock()},
    base::BindOnce(&Task));
```

本 repo 的等價寫法：

```rust
use rust_task::{ThreadPool, TaskTraits};

fn main() {
    let pool = ThreadPool::new(4);   // 4 條 worker thread，回傳 Arc<ThreadPool>

    pool.post_task(TaskTraits::default(), Box::new(|| {
        println!("hello from a worker thread");
    }));

    pool.shutdown();                 // 第 8 章詳述
}
```

兩個 API 層面的差異：

1. **單例 vs 實例。** Chromium 的 `ThreadPoolInstance` 是 process 級單例（文件
   "Using ThreadPool in a New Process" 一節描述它的初始化流程）；本 repo 讓你
   自己建立並持有 `Arc<ThreadPool>`。Rust 對全域可變狀態天生不友善（必須
   `static` + 同步包裝），而且實例化的設計讓測試可以各開各的 pool，互不干擾。
2. **回傳值。** `post_task` 回傳 `bool`：`false` 表示 task 被拒收（shutdown
   之後）。Chromium 的 `PostTask` 也回傳 bool，只是大多數呼叫端忽略它。

## 3.2 TaskTraits：描述任務的性質

Chromium 用 `base::TaskTraits` 告訴 thread pool「這個任務是什麼性質」，讓排程
做出更好的決策。文件給的例子：

```cpp
// 最高優先權：使用者正被擋住
base::ThreadPool::PostTask(
    FROM_HERE, {base::TaskPriority::USER_BLOCKING}, base::BindOnce(...));

// 最低優先權、允許阻塞（例如讀磁碟）
base::ThreadPool::PostTask(
    FROM_HERE, {base::TaskPriority::BEST_EFFORT, base::MayBlock()},
    base::BindOnce(...));

// 程式結束前必須跑完
base::ThreadPool::PostTask(
    FROM_HERE, {base::TaskShutdownBehavior::BLOCK_SHUTDOWN},
    base::BindOnce(...));
```

本 repo 的對應（[`rust_task/task_traits.rs`](../../rust_task/task_traits.rs)，
完整檔案就這 38 行）：

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    BestEffort,    // 最低：使用者察覺不到的工作（清快取、壓縮記錄檔）
    UserVisible,   // 使用者看得到結果，但沒有在等（載入下一頁縮圖）
    UserBlocking,  // 使用者正在等（點了按鈕還沒反應）
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

這 38 行濃縮了一整把 Rust 入門必備的概念，逐一拆解。

### 〔Rust 基礎〕`enum`：不只是整數常數

C++ 的 `enum class TaskPriority` 本質是整數常數；Rust 的 `enum`
（[ch6-01](https://rust-lang.tw/book-tw/ch06-01-defining-an-enum.html)）是完整的
**代數資料型別**——variant 可以帶資料（第 2 章的 `Option<T>` 就是
`enum Option<T> { None, Some(T) }`）。這裡的三個 enum 雖然只是單純列舉，但
配上 `match` 就比 C++ 的 switch 強一截：

```rust
fn describe(p: TaskPriority) -> &'static str {
    match p {
        TaskPriority::BestEffort   => "可以慢慢來",
        TaskPriority::UserVisible  => "別拖太久",
        TaskPriority::UserBlocking => "使用者在等！",
    }   // ← 少寫一個 variant 就編譯失敗（exhaustiveness check）
}
```

**窮盡性檢查**是 `match`（[ch6-02](https://rust-lang.tw/book-tw/ch06-02-match.html)）
的殺手級特性：日後有人加了第四個優先權等級，所有漏掉它的 `match` 一起編譯失敗
——重構的安全網。C++ 的 switch 要靠 `-Wswitch` 警告，而且常被 `default:` 分支
吃掉。

### 〔Rust 基礎〕`derive`：白嫖標準能力

`#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]` 一行讓編譯器
自動生成七個 trait 的實作：

- **`Copy`** — 賦值改為位元複製，不發生所有權移動。只有「淺」型別能標（含
  `String` 的 struct 不行）。`TaskTraits` 是純值的小 struct，標上 `Copy` 之後
  到處傳遞零摩擦——你會看到本 repo 的 API 都直接收 `TaskTraits` 值而非引用。
- **`Debug`** — 讓 `println!("{traits:?}")` 能用。
- **`PartialOrd, Ord`** — 可比較大小，**順序就是 variant 宣告順序**：
  `BestEffort < UserVisible < UserBlocking`。第 5 章的優先序佇列直接拿這個
  排序，一行比較邏輯都不用寫。這是「把語意編進型別」的小範例：優先權的高低
  不是文件約定，是 `derive(Ord)` + 宣告順序的機械事實。

### 〔Rust 基礎〕`Default` trait 與 struct update 語法

C++ 寫 `{base::TaskPriority::BEST_EFFORT, MayBlock()}` 這種花式 initializer
（背後是 `task_traits.h` 裡一套可變參數模板）。Rust 的慣用法樸素得多
（[ch5-01](https://rust-lang.tw/book-tw/ch05-01-defining-structs.html)）：

```rust
// 全用預設
let traits = TaskTraits::default();

// 改兩個欄位，其餘照預設 —— `..` 是 struct update 語法
let traits = TaskTraits {
    priority: TaskPriority::BestEffort,
    may_block: true,
    ..Default::default()
};
```

`Default` 是個普通 trait（`fn default() -> Self`），這裡手寫實作是因為預設值
有語意（`UserVisible` / `SkipOnShutdown`，跟 Chromium 的預設一致——文件說
default traits 適用於「不阻塞、與使用者活動相關、shutdown 行為隨便」的任務）。
若每個欄位都用其型別預設值，也可以直接 `#[derive(Default)]`。

## 3.3 TaskRunner trait：把「能收任務的東西」抽象出來

直接對 pool post 很方便，但更多時候你想持有一個「可以丟任務進去的東西」——
今天它是平行 runner，明天可能換成 sequenced runner，呼叫端不用改。Chromium 的
`base::TaskRunner`（[`base/task/task_runner.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/task_runner.h)）就是這個介面；文件說
它的價值在於：

> mainly useful when it isn't known in advance whether tasks will be posted
> in parallel, in sequence, or to a single-thread.

本 repo 的 trait（[`rust_task/task_runner.rs`](../../rust_task/task_runner.rs)，全文）：

```rust
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
```

### 〔Rust 基礎〕trait＝介面，但不只是介面

C++ 的「介面」＝抽象基底類別＋純虛擬函式＋繼承。Rust 的 **trait**
（[ch10-02](https://rust-lang.tw/book-tw/ch10-02-traits.html)）做同一件事但解耦更徹底：

```rust
// 定義介面
pub trait Greet {
    fn name(&self) -> String;
    fn greet(&self) -> String {            // 可以有預設實作
        format!("Hello, {}!", self.name())
    }
}

// 為任何型別實作它——包括別人的型別（孤兒規則限制內）
struct Alice;
impl Greet for Alice {
    fn name(&self) -> String { "Alice".into() }
}
```

用 trait 的兩種方式，對應靜態／動態分派：

```rust
fn static_dispatch(g: &impl Greet) { ... }      // 泛型，編譯期展開（單態化）
fn dynamic_dispatch(g: &dyn Greet) { ... }      // trait object，vtable（≈ C++ 虛擬函式）
```

`TaskRunner` 在本 repo 一律以 trait object 形式使用：`Arc<dyn TaskRunner>`，
等價於 Chromium 的 `scoped_refptr<base::TaskRunner>`。

### trait 自己也能掛 bound：`TaskRunner: Send + Sync`

宣告 `pub trait TaskRunner: Send + Sync` 表示「**任何**實作此 trait 的型別都
必須 `Send + Sync`」。為什麼？runner 的存在意義就是被一堆執行緒同時拿着 post
任務，不可跨執行緒共享的 runner 沒有意義。把需求寫在 trait 上，實作者忘了就
編不過，使用者拿到 `Arc<dyn TaskRunner>` 就**保證**能丟給任何執行緒。

### 為什麼方法都收 `&self` 而不是 `&mut self`

`post_task(&self, ...)`——共享引用。但 post 明明會改內部狀態（往佇列塞東西）？

這是 Rust 並行 API 的標準形狀：**會被並發呼叫的方法必須收 `&self`**，因為
`&mut self` 意味着獨占（同一時間只能有一個呼叫者），跟「多執行緒同時 post」
矛盾。內部可變性（`Mutex` 等）負責在 `&self` 底下安全地改狀態——第 4 章詳述。
讀本 repo 程式碼時這是個好用的線索：看到 `&self` ＋ 內部有 `Mutex`，就知道
這個型別設計為並發使用。

## 3.4 ThreadPool 的公開 API

[`rust_task/thread_pool/thread_pool.rs`](../../rust_task/thread_pool/thread_pool.rs)：

```rust
pub struct ThreadPool {
    task_tracker: Arc<TaskTracker>,             // 第 8 章
    delayed_task_manager: Arc<DelayedTaskManager>,  // 第 7 章
    thread_group: Arc<ThreadGroup>,             // 第 5 章
    monitor: Option<Arc<TaskMonitor>>,          // 選配的監控
}

impl ThreadPool {
    pub fn new(num_threads: usize) -> Arc<Self> { ... }

    pub fn post_task(&self, traits: TaskTraits,
                     callback: Box<dyn FnOnce() + Send + 'static>) -> bool { ... }

    pub fn create_sequenced_task_runner(&self, traits: TaskTraits)
        -> Arc<dyn SequencedTaskRunner> { ... }   // 第 4 章的主角

    pub fn create_task_runner(&self, traits: TaskTraits)
        -> Arc<dyn TaskRunner> { ... }            // 平行 runner

    pub fn shutdown(&self) { ... }                // 第 8 章
}
```

對照 Chromium：`post_task` ≈ `base::ThreadPool::PostTask`、
`create_sequenced_task_runner` ≈ `base::ThreadPool::CreateSequencedTaskRunner`、
`create_task_runner` ≈ `base::ThreadPool::CreateTaskRunner`。

`post_task` 的實作只有六行，値得現在就看一眼（細節留給第 5、8 章）：

```rust
pub fn post_task(&self, traits: TaskTraits,
                 callback: Box<dyn FnOnce() + Send + 'static>) -> bool {
    if !self.task_tracker.will_post_task(&traits) {
        return false;                                  // shutdown 後拒收
    }
    let seq = Arc::new(Sequence::new(traits));         // 一次性的單任務 sequence！
    seq.push_task(Task::new(self.wrap(traits, callback)));
    self.thread_group.push_task_source(seq);
    true
}
```

注意中間那行：**平行任務＝只裝了一個 task 的匿名 Sequence**。整個引擎只需要
理解一種東西（sequence／task source），「平行」和「循序」的差別只在 sequence
是一次性的還是常駐的。這個統一手法直接來自 Chromium 的 `TaskSource` 抽象
（[`base/task/thread_pool/task_source.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task_source.h)）。

### 〔Rust 教學〕為什麼 `new` 回傳 `Arc<Self>`

```rust
pub fn new(num_threads: usize) -> Arc<Self>
```

`Arc<T>`（[ch15-04 講 `Rc`](https://rust-lang.tw/book-tw/ch15-04-rc.html)，`Arc` 是
其原子版）是引用計數的共享指標，等價 Chromium 的 `scoped_refptr<T>` /
`base::RefCountedThreadSafe`（[`base/memory/scoped_refptr.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/memory/scoped_refptr.h)）：

```rust
let pool = ThreadPool::new(4);       // 強引用計數 = 1
let p2 = Arc::clone(&pool);          // 計數 = 2（複製指標，不複製 pool！）
drop(pool);                          // 計數 = 1
drop(p2);                            // 計數 = 0 → ThreadPool 被釋放
```

慣例提醒：寫 `Arc::clone(&pool)` 而非 `pool.clone()`——兩者等價，但前者讓讀者
一眼看出「這只是計數 +1」，不會誤以為深拷貝。

pool 注定被 main、各 runner、你的各種元件同時引用——典型的共享所有權。建構時
直接回傳 `Arc`，呼叫端複製即可。另一個原因要到第 5 章才完整揭曉：pool 內部的
worker thread 也需要引用 `ThreadGroup`，「自己內部的執行緒引用自己的一部分」
只能靠 `Arc` 表達。

## 本章小結

- `TaskTraits`＝「告訴排程器這個任務的性質」：優先權、shutdown 行為。預設值
  與 Chromium 一致（`UserVisible` + `SkipOnShutdown`）。
- `TaskRunner` trait＝可以收任務的東西；`Send + Sync` supertrait bound 保證
  它可跨執行緒共享；`&self` 方法＋內部可變性是並行 API 的標準形狀。
- 平行任務在引擎眼裡是「單任務的匿名 sequence」——統一抽象。
- Rust 概念入帳：`enum` 與窮盡性 `match`、`derive`、`Copy`、`Default` 與
  struct update 語法、trait 定義／實作／預設方法、靜態 vs 動態分派、
  `Arc` 與共享所有權。

## 動手做

1. 寫一個函式 `fn busy(pool: &ThreadPool, n: usize)`：post `n` 個任務，每個
   印出自己的編號。跑幾次觀察輸出順序——平行任務**沒有**順序保證，這是第 4 章
   的引子。
2. 把一個 `TaskPriority` 的 `match` 故意少寫一個 variant，讀編譯錯誤；再把
   `match` 改成 `if let`，想想為什麼 `if let` 不會報同樣的錯
   （[ch6-03](https://rust-lang.tw/book-tw/ch06-03-if-let.html)）。

## 延伸閱讀

- Chromium `task_traits.h`（[`base/task/task_traits.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/task_traits.h)）：每個 trait
  的詳盡註解，包括 `MayBlock()` 與 `WithBaseSyncPrimitives()` 的區別——本 repo
  的 `may_block` 欄位目前未參與排程，是保留欄位。
- TaskRunner 該由誰持有？文件 "TaskRunner ownership (encourage no dependency
  injection)" 一節主張**用它的元件自己建立它**，不要層層傳遞——測試時用
  `TaskEnvironment` 控制，而不是注入 mock runner。
