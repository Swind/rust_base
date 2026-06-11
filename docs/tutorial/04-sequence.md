# 第 4 章 Sequence：用「順序」取代「鎖」

> 本章是整個系列的核心。
> Chromium 素材：[`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) 的 Posting a Sequenced
> Task、Using Sequences Instead of Locks、Prefer Sequences to Physical Threads
> 三節、[`base/task/sequenced_task_runner.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/sequenced_task_runner.h)、
> [`base/task/thread_pool/sequence.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/sequence.h)。
> Rust 素材：Rust Book [ch15-03 Drop](https://rust-lang.tw/book-tw/ch15-03-drop.html)、
> [ch15-05 RefCell 與內部可變性](https://rust-lang.tw/book-tw/ch15-05-interior-mutability.html)、
> [ch15-06 引用循環](https://rust-lang.tw/book-tw/ch15-06-reference-cycles.html)、
> [ch16-03 共享狀態](https://rust-lang.tw/book-tw/ch16-03-shared-state.html)、
> [ch19-02 進階 trait（supertrait）](https://rust-lang.tw/book-tw/ch19-02-advanced-traits.html)。
> 主角程式碼：[`rust_task/sequenced_task_runner.rs`](../../rust_task/sequenced_task_runner.rs)、
> [`rust_task/thread_pool/sequence.rs`](../../rust_task/thread_pool/sequence.rs)、
> [`rust_task/examples/event_bus.rs`](../../rust_task/examples/event_bus.rs)。

## 4.1 理念：Sequences inherently provide thread-safety

Chromium 文件原文：

> Usage of locks is discouraged in Chrome. Sequences inherently provide
> thread-safety. Prefer classes that are always accessed from the same
> sequence to managing your own thread-safety with locks.

論證如下。假設某塊狀態的**所有存取**都以 task 的形式 post 到同一個 sequence：

- sequence 保證 task **一次只跑一個** → 互斥（mutual exclusion）✓
- sequence 保證 task **依 post 順序執行** → 操作有全序 ✓
- sequence 保證後一個 task **看得到前一個的所有副作用** → 記憶體可見性 ✓

鎖能給你的，sequence 全給了；而鎖會帶來的——死鎖、忘記上鎖、鎖粒度難拿捏、
持鎖時呼叫外部程式碼的重入炸彈——sequence 一個都沒有。

而且 sequence **不是執行緒**。一個 sequenced task 跑完，下一個 task 可能被
**不同的 worker thread** 撿走。綁定的是「順序」這個邏輯概念，不是某條 OS
執行緒——所以文件叫它 **virtual thread**，並整節論述「Prefer Sequences to
Physical Threads」：physical thread 是稀缺資源（每條都有 stack 等成本），
sequence 要多少有多少。

### Chromium 的寫法 vs 本 repo 的寫法

Chromium 中一個「綁定 sequence」的類別：

```cpp
class A {
 public:
  void AddValue(int v) {
    DCHECK_CALLED_ON_VALID_SEQUENCE(sequence_checker_);  // 執行期（debug）檢查
    values_.push_back(v);
  }
 private:
  SEQUENCE_CHECKER(sequence_checker_);
  std::vector<int> values_;   // 不需要鎖
};

// 所有存取都 post 到同一個 runner：
task_runner_for_a->PostTask(FROM_HERE,
    base::BindOnce(&A::AddValue, base::Unretained(&a), 42));
```

本 repo：

```rust
let runner = pool.create_sequenced_task_runner(TaskTraits::default());

runner.post_task(Box::new(|| println!("first")));
runner.post_task(Box::new(|| println!("second")));   // 必在 first 之後，永不重疊
```

輸出**保證**是 `first` 然後 `second`——對比第 3 章動手做第 1 題的平行任務，
這就是「sequenced」的意思。

## 4.2 SequencedTaskRunner trait 與 supertrait

[`rust_task/sequenced_task_runner.rs`](../../rust_task/sequenced_task_runner.rs)：

```rust
pub trait SequencedTaskRunner: TaskRunner {
    fn post_non_nestable_task(&self, task: Box<dyn FnOnce() + Send + 'static>) -> bool;
    fn runs_tasks_in_current_sequence(&self) -> bool;
    fn sequence_token(&self) -> SequenceToken;
}
```

### 〔Rust 教學〕supertrait：trait 的「繼承」

`pub trait SequencedTaskRunner: TaskRunner` 讀作「想實作 `SequencedTaskRunner`，
必須先實作 `TaskRunner`」（[ch19-02](https://rust-lang.tw/book-tw/ch19-02-advanced-traits.html)）。
效果近似 C++ 的 `class SequencedTaskRunner : public TaskRunner`：

```rust
fn use_it(runner: Arc<dyn SequencedTaskRunner>) {
    runner.post_task(...);                       // TaskRunner 的方法，直接可用
    runner.runs_tasks_in_current_sequence();     // 自己的方法
}
```

但語意上有微妙差異值得體會：C++ 繼承是「is-a ＋ 程式碼重用」混在一起；Rust 的
supertrait 純粹是**對實作者的要求**，沒有任何實作被繼承——`TaskRunner` 的三個
方法還是要由具體型別自己實作（第 5 章的 `PooledSequencedTaskRunner` 就是）。

### 〔Rust 教學〕object safety：為什麼 `delete_soon` 不在 trait 裡

Chromium 的 `SequencedTaskRunner` 有個方便方法 `DeleteSoon<T>(FROM_HERE, ptr)`
——把物件送回它所屬的 sequence 上解構（物件的解構式會碰只能在該 sequence 碰的
狀態時必用）。它是 C++ 模板方法。

Rust 這邊有個攔路虎：我們到處用 `Arc<dyn SequencedTaskRunner>`（trait object），
而 trait 要能 `dyn` 必須 **object-safe**——大致規則：方法不能有泛型參數、不能
收發 `Self` 值。帶 `<T>` 的方法一旦進 trait，整個 trait 就做不成 `dyn`。

本 repo 的解法：把它搬出 trait，做成**自由函式**：

```rust
pub fn delete_soon<T: Send + 'static>(runner: &dyn SequencedTaskRunner, value: Box<T>) -> bool {
    runner.post_task(Box::new(move || drop(value)))
}
```

一行實作，拆開看每一步都在教 Rust：

1. `value: Box<T>` ——函式**拿走**物件所有權；
2. `move ||` ——所有權再轉進 closure；
3. closure 被 post 到目標 sequence；
4. 在那邊執行 `drop(value)` ——`drop` 就是「現在立刻結束這個值的生命」，
   解構邏輯（`Drop::drop`）於是**在正確的 sequence 上執行**。

「物件死在哪條執行緒」在 C++ 是大坑（Chromium 專門有
`RefCountedDeleteOnSequence`）；Rust 的所有權轉移讓答案精確可控：值在哪裡
離開 scope／被 `drop`，解構式就在哪裡跑。

## 4.3 「我在哪個 sequence 上？」——SequenceToken 與 `thread_local!`

`runs_tasks_in_current_sequence()` 對應 Chromium 的
`RunsTasksInCurrentSequence()`，被 `SEQUENCE_CHECKER` 拿來做斷言。它怎麼知道
「現在」是哪個 sequence？答案：worker 執行每個 task 前，把 sequence 的識別證
放進 **thread-local**，跑完收走。

識別證本身（[`rust_task/sequence_token.rs`](../../rust_task/sequence_token.rs)）：

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SequenceToken(u64);          // 全域遞增的唯一編號
```

〔Rust 基礎〕這叫 **newtype**：用單欄位 tuple struct 包住 `u64`，讓「sequence
編號」成為獨立型別——你不可能誤把隨便一個 `u64` 當 token 用，也不可能拿 token
去做加法。零執行期成本的型別安全。

thread-local 側（[`rust_task/sequenced_task_runner.rs`](../../rust_task/sequenced_task_runner.rs)）：

```rust
thread_local! {
    static CURRENT_DEFAULT: RefCell<Option<Arc<dyn SequencedTaskRunner>>> =
        RefCell::new(None);
}

pub fn current_default() -> Option<Arc<dyn SequencedTaskRunner>> {
    CURRENT_DEFAULT.with(|c| c.borrow().clone())
}
```

對應 Chromium 的 `base::SequencedTaskRunner::GetCurrentDefault()`。

### 〔Rust 教學〕`thread_local!` ＋ `RefCell`：單執行緒的內部可變性

- **`thread_local!`** 宣告「每條執行緒一份」的變數，存取必須透過
  `.with(|c| ...)`——closure 形式確保你拿到的引用不會洩漏出該執行緒。
- 裡面為什麼套一層 **`RefCell`**？`static` 變數預設不可變，要改就需要內部
  可變性。第 1 章說過 `RefCell` 不是 `Sync` ——但 thread-local **保證永遠只有
  本執行緒碰它**，所以這裡用單執行緒版的 `RefCell` 就夠，不用付 `Mutex` 的
  鎖開銷。「並行範圍多大，就用多重的同步工具」是 Rust 的器材選擇哲學：

| 並行範圍 | 內部可變性工具 |
|---|---|
| 單執行緒 | `Cell` / `RefCell` |
| 跨執行緒 | `Mutex` / `RwLock` / atomics |

`RefCell` 的借用規則和編譯期一樣（多個讀 xor 一個寫），只是改到**執行期**
檢查，違反就 panic（[ch15-05](https://rust-lang.tw/book-tw/ch15-05-interior-mutability.html)）。

而 `PooledSequencedTaskRunner` 的 `runs_tasks_in_current_sequence` 就是拿
thread-local 跟自己的 token 比對（`rust_task/thread_pool/pooled_sequenced_task_runner.rs:94`）：

```rust
fn runs_tasks_in_current_sequence(&self) -> bool {
    SequenceToken::current() == Some(self.sequence.token())
}
```

## 4.4 CurrentDefaultHandle：RAII 與 `Drop`

worker 設定 thread-local 不是裸寫，而是透過一個 RAII 把手：

```rust
pub struct CurrentDefaultHandle {
    previous: Option<Arc<dyn SequencedTaskRunner>>,
}

impl CurrentDefaultHandle {
    pub fn new(runner: Arc<dyn SequencedTaskRunner>) -> Self {
        let previous = CURRENT_DEFAULT.with(|c| c.borrow().clone());
        CURRENT_DEFAULT.with(|c| *c.borrow_mut() = Some(runner));
        Self { previous }                       // 記住舊值
    }
}

impl Drop for CurrentDefaultHandle {
    fn drop(&mut self) {
        CURRENT_DEFAULT.with(|c| *c.borrow_mut() = self.previous.take());  // 還原
    }
}
```

### 〔Rust 教學〕`Drop` trait：C++ RAII 的 Rust 拼法

`Drop::drop`（[ch15-03](https://rust-lang.tw/book-tw/ch15-03-drop.html)）在值離開
scope 時自動執行——解構式。第 5 章的 worker 迴圈這樣用它：

```rust
let _default_handle = env.task_runner.map(CurrentDefaultHandle::new);
//  ↑ 變數活着 = thread-local 已設定
(task.callback)();
//  scope 結束，_default_handle 被 drop → thread-local 自動還原
```

重點：**就算 callback panic、就算中途 return，`drop` 都保證執行**——清理不可能
被跳過。C++ 程式設計師會覺得熟悉（解構式），但注意兩個 Rust 細節：

- 變數名以 `_` 開頭（`_default_handle`）告訴編譯器「我故意不用它，別警告」——
  但它**仍然活到 scope 結束**。若寫成純 `_`（`let _ = ...`）值會**立刻** drop，
  RAII 就失效了。這是新手常踩的坑。
- `self.previous.take()`：`Option::take` 把值取出、原地留下 `None`——在只有
  `&mut self` 時把所有權「偷」出來的標準手法。

`new` 存舊值、`drop` 還原舊值——這個「壓棧／彈棧」結構讓 handle 可以巢狀
（task 裡同步執行另一個 sequence 的 task 時不會弄丟外層的 default）。

## 4.5 引擎室：Sequence 怎麼兌現「一次一個、FIFO」

以上是使用者視角。現在看
[`rust_task/thread_pool/sequence.rs`](../../rust_task/thread_pool/sequence.rs)
怎麼實作承諾。對應 Chromium 的
[`base/task/thread_pool/sequence.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/sequence.h)。

```rust
struct SequenceInner {
    immediate_queue: VecDeque<Task>,
    delayed_queue: BinaryHeap<Reverse<DelayedTask>>,   // min-heap：最早到期者先出
    next_sequence_num: u64,
}

pub struct Sequence {
    token: SequenceToken,
    inner: Mutex<SequenceInner>,
    has_worker: AtomicBool,
    traits: TaskTraits,
    task_runner: Mutex<Option<Weak<dyn SequencedTaskRunner>>>,
}
```

四個設計決策，每個都是一課。

### (a) `Mutex<SequenceInner>`：用一把鎖、鎖一整組狀態

〔Rust 基礎〕先補 `Mutex` 的基本面（[ch16-03](https://rust-lang.tw/book-tw/ch16-03-shared-state.html)）。
Rust 的 `Mutex<T>` 和 C++ 的 `std::mutex` 有個根本差異：**它把被保護的資料包在
裡面**。C++ 的 mutex 和資料是兩個獨立變數，「忘了上鎖直接碰資料」編譯器管不着；
Rust 想碰 `T` 唯一的路是 `lock()`：

```rust
let m = Mutex::new(5);
{
    let mut guard = m.lock().unwrap();   // guard: MutexGuard<i32>
    *guard += 1;                         // 透過 guard 才碰得到資料
}                                        // guard 離開 scope → 自動解鎖（Drop！）
```

- `lock()` 回傳 `Result`——前一個持鎖者 panic 過，鎖就「中毒」（poisoned），
  `unwrap()` 是「那就跟着 panic」的決定，本 repo 一律如此。
- **`MutexGuard` 就是 4.4 節剛學的 RAII**：忘記解鎖在 Rust 寫不出來。

回到 Sequence。注意 `immediate_queue` 和 `delayed_queue` 在**同一個**
`Mutex<SequenceInner>` 裡。若拆成兩把鎖，「先看 delayed 有沒有到期、再去動
immediate」兩步之間就有空窗（TOCTOU 競態）。合成一把鎖後，`take_task` 持鎖
做完整套：

```rust
fn take_task(&self) -> Option<Task> {
    let mut inner = self.inner.lock().unwrap();
    Self::flush_ready_delayed_tasks(&mut inner, Instant::now());  // 到期的搬進 immediate
    inner.immediate_queue.pop_front()                             // 再取隊首
}
```

這對應 Chromium `Sequence::Transaction`——「拿到交易才能動佇列」。Chromium 用
程式約定（先 `BeginTransaction()`）；Rust 用 `MutexGuard` 的型別事實：沒有
guard，`SequenceInner` 的欄位你根本摸不到。

**鎖的設計準則**（第 8 章還會再遇到一次）：問「哪些狀態必須一起原子地變更？」
答案就是一把鎖該罩住的範圍。

### (b) `has_worker: AtomicBool`：兩行實現「永不並發」

兩個 worker 同時發現這個 sequence 有任務，誰執行？`Sequence` 用一個原子旗標
仲裁（這就是它對 `TaskSource::will_run_task` 的實作，第 5 章串起全流程）：

```rust
fn will_run_task(&self) -> RunStatus {
    // swap 回傳舊值：true = 已有 worker 持有（拒絕）；false = 我搶到了
    if self.has_worker.swap(true, AtomicOrdering::AcqRel) {
        RunStatus::Disallowed
    } else {
        RunStatus::AllowedNotSaturated
    }
}

fn did_process_task(&self) -> bool {
    self.has_worker.store(false, AtomicOrdering::Release);   // 歸還
    // ...回報還有沒有 ready task
}
```

〔Rust 教學〕`AtomicBool::swap(new, ordering)` 原子地「寫入新值並回傳舊值」。
兩個 worker 同時呼叫，硬體保證**恰好一個**拿到 `false`（搶到）——這是不用鎖的
互斥。`Ordering` 參數決定這個原子操作同時帶多強的記憶體屏障：

| 本 repo 用到的 ordering | 意義 | 用在 |
|---|---|---|
| `Relaxed` | 只保證這個操作本身原子 | 純計數器（如 token 編號產生） |
| `Release`（寫）/`Acquire`（讀） | 配對後形成 happens-before：寫方屏障前的修改，讀方屏障後保證可見 | 旗標（shutdown、has_worker） |
| `AcqRel` | 讀改寫操作兩頭都要 | `swap` |

`has_worker` 用 `AcqRel`/`Release` 的實際意義：worker B 搶到 sequence 時，
保證看得到 worker A 跑前一個 task 留下的**全部**寫入——這正是第 1 章說的
「sequence 的 happens-before 保證」的實作來源。初學者不需要精通 memory
ordering，但要建立直覺：**旗標類用 Acquire/Release，計數類用 Relaxed，
拿不準就用更強的**。

### (c) `Weak`：斷開引用循環

`PooledSequencedTaskRunner` 持有 `Arc<Sequence>`；而 sequence 需要回指 runner
（worker 執行它的 task 時，要把這個 runner 設成 thread-local 的
current_default）。如果回邊也用 `Arc`：A 引用 B、B 引用 A，計數永不歸零，
**雙雙洩漏**（[ch15-06](https://rust-lang.tw/book-tw/ch15-06-reference-cycles.html)）。

解法是回邊用 **`Weak`**——不增加強計數的「觀察者指標」：

```rust
task_runner: Mutex<Option<Weak<dyn SequencedTaskRunner>>>,
```

用時 `upgrade()`：物件還活着拿到 `Some(Arc)`（並暫時 +1 計數），死了拿到
`None`。對應 Chromium 的 `base::WeakPtr`（[`base/memory/weak_ptr.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/memory/weak_ptr.h)），
但有一個重大差異：Chromium 的 `WeakPtr` 只能在單一 sequence 上解參考，Rust 的
`Weak::upgrade` 是原子操作、跨執行緒安全。第 6 章整章圍繞 `Weak` 展開。

經驗法則：**所有權拓撲必須是 DAG（有向無環圖）；任何「回指」「父指標」
「觀察者」都用 `Weak`**。

### (d) 自訂 `Ord` 餵 `BinaryHeap`：延遲佇列

delayed task 按到期時間排序。`std::collections::BinaryHeap` 是 max-heap、
要求元素實作 `Ord`；但 `Task` 裡的 closure 沒法比較。慣用解法是包一個
「只比較鍵」的型別：

```rust
struct DelayedTask {
    ready_time: Instant,
    sequence_num: u64,
    task: Task,             // 不參與比較
}

impl Ord for DelayedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // 自然序：到期早 = 小。配合 BinaryHeap<Reverse<...>> 形成 min-heap。
        self.ready_time.cmp(&other.ready_time)
            .then(self.sequence_num.cmp(&other.sequence_num))
    }
}
```

三個小知識：

- `Reverse<T>` 是標準庫的「比較順序反轉」包裝——max-heap 裝 `Reverse` 就成
  min-heap，`peek()` 看到的是**最早**到期的任務。
- `.then(...)` 串接次要排序鍵：同時到期的任務按 `sequence_num`（第 2 章的
  遞增序號）分先後——排序**決定性**，同樣輸入永遠同樣順序，測試才能穩定。
- 手動實作 `Ord` 時必須一起實作 `PartialOrd` / `Eq` / `PartialEq` 且彼此一致
  （原始碼裡四個 impl 都在，這裡省略）。

## 4.6 綜合演練：免鎖的 Event Bus

[`rust_task/examples/event_bus.rs`](../../rust_task/examples/event_bus.rs)
（`cargo run -p rust_task --example event_bus`）把本章所有概念組裝成一個真實
元件：publish / subscribe 事件匯流排，**所有操作都 post 到同一個 sequence**，
於是免費獲得：

1. **順序** — 事件嚴格依發布順序派送。
2. **序列化** — 在 publish 之前 post 的 unsubscribe，保證先生效；不需要任何
   外部鎖協調。
3. **可重入** — 訂閱者的 callback 裡再呼叫 `publish()` 是安全的：新事件排到
   sequence 尾端，在當前事件**結束之後**派送，永不 inline 遞迴。

核心程式碼（`bind_once` 與 `Arc::downgrade` 第 6 章詳解，先看結構）：

```rust
struct EventBus<E: Send + 'static> {
    state: Mutex<BusState<E>>,                  // 訂閱者清單
    runner: Arc<dyn SequencedTaskRunner>,       // 一切操作的「家」
    next_id: AtomicU64,
}

fn publish(self: &Arc<Self>, event: E) {
    self.runner.post_task(bind_once(Arc::downgrade(self), move |bus| {
        let cbs: Vec<Callback<E>> = bus.state.lock().unwrap()
            .subscribers.iter().map(|(_, cb)| Arc::clone(cb)).collect();
        for cb in cbs {        // 注意：先複製清單、放開鎖，才逐一呼叫 callback
            cb(&event);
        }
    }));
}
```

做個思想實驗：不用 sequence，直接 `Mutex<Vec<Subscriber>>` ＋同步呼叫，會發生
什麼？訂閱者 callback 裡再呼叫 `publish` → `publish` 要拿鎖 → 鎖還被外層的
自己持着 → **死鎖**（`std::sync::Mutex` 不可重入）。sequence 版把「再進來的
publish」變成佇列尾端的新任務，問題在架構層面消失。這就是 Chromium 把
「用順序取代鎖」奉為圭臬的原因——**鎖在呼叫外部程式碼時是地雷，sequence 不是**。

（範例裡仍有一個 `Mutex<BusState>`——因為 `subscribe` 從外部執行緒也會碰
`next_id`／state。注意它的鎖區間極短且絕不在持鎖時呼叫 callback：上面程式碼
先把訂閱者清單 clone 出來、guard 隨即釋放，才開始派送。「鎖內不呼叫外部碼」
是用鎖時的保命守則。）

## 本章小結

- Sequence = 互斥 + 順序 + 可見性，鎖的全部好處、沒有鎖的任何地雷；且不
  獨占 physical thread。
- 對外：`SequencedTaskRunner`（supertrait）、`current_default()`
  （`thread_local!`）、`runs_tasks_in_current_sequence()`（token 比對）。
- 對內：一把 `Mutex` 鎖整組佇列（Transaction）、`AtomicBool::swap` 仲裁
  「誰執行」、回指用 `Weak` 斷環、`Reverse` + 自訂 `Ord` 做 min-heap。
- Rust 概念入帳：newtype、`thread_local!`、`RefCell` vs `Mutex` 的選用、
  `Drop`/RAII 與 `_` 前綴陷阱、`Option::take`、`MutexGuard`、atomics 與
  memory ordering 直覺、`Weak`、`Ord`/`Reverse`/`BinaryHeap`。

## 動手做

1. 跑 `cargo run -p rust_task --example event_bus`，對照輸出與 4.6 的三個保證。
2. 寫一個「計數器」兩種版本：(a) `Arc<Mutex<i64>>` 給 8 條執行緒直接加；
   (b) 一個 `SequencedTaskRunner`，8 條執行緒 post「加一」任務。兩版都正確——
   體會差異在哪：版本 (b) 的「加一」如果變成「呼叫不可信的 callback」，
   依然安全；版本 (a) 就要開始擔心了。
3. 把 4.4 的 `let _default_handle = ...` 改成 `let _ = ...`，預測會發生什麼，
   再跑 `cargo test -p rust_task` 驗證（提示：`post_task_and_reply` 相關測試）。

## 延伸閱讀

- Chromium `sequence.h`（[`base/task/thread_pool/sequence.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/sequence.h)）開頭
  的類別註解：C++ 版的 `Sequence` 狀態機（`PushImmediateTask` /
  `TakeTask` / `DidProcessTask`…）比 Rust 版多了不少狀態，對照讀能看出本 repo
  簡化了哪些。
- 為什麼鎖只該用來「換入共享資料結構」：文件 Using Sequences Instead of
  Locks 一節結尾的 `PluginList::LoadPlugins` 例子。
