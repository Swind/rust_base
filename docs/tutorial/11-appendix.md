# 第 11 章 附錄

## 附錄 A：概念對照速查表

### A.1 Chromium ↔ 本 workspace ↔ Rust 語言

| Chromium (C++) | 本 workspace (Rust) | 背後的 Rust 概念 | 詳見 |
|---|---|---|---|
| `base::OnceClosure` / `BindOnce` | `Box<dyn FnOnce() + Send + 'static>` / move closure | closure、trait object、`'static` | 第 2 章 |
| `base::RepeatingCallback` / `BindRepeating` | `Arc<dyn Fn() + Send + Sync>` / `bind_repeating` | `Fn` trait | 第 2、6 章 |
| `std::move(cb).Run()` 約定 | `FnOnce` 呼叫即消耗 | 所有權 | 第 2 章 |
| `FROM_HERE` / `base::Location` | `Task::posted_from` | `#[track_caller]` + `Location::caller()` | 第 2 章 |
| `scoped_refptr<T>` / `RefCountedThreadSafe` | `Arc<T>` | 原子引用計數 | 第 3 章 |
| `base::WeakPtr<T>` + `WeakPtrFactory` | `Weak<T>` + `bind_once` | `upgrade() -> Option<Arc<T>>`（且跨執行緒安全） | 第 6 章 |
| `base::Unretained(ptr)` | **寫不出來** | `'static` bound 編譯期拒絕 | 第 2、6 章 |
| `base::ThreadPoolInstance`（單例） | `Arc<ThreadPool>`（實例） | 共享所有權 | 第 3 章 |
| `base::TaskRunner` | `trait TaskRunner` | trait、`&self` + 內部可變性 | 第 3 章 |
| `base::SequencedTaskRunner` | `trait SequencedTaskRunner` | supertrait | 第 4 章 |
| `SequencedTaskRunner::DeleteSoon<T>` | `sequenced_task_runner::delete_soon` 自由函式 | object safety | 第 4 章 |
| `base::SingleThreadTaskRunner` | 不實作（IO 例外：`IoTaskRunner`） | — | 第 1、10 章 |
| `GetCurrentDefault()` | `current_default()` | `thread_local!` + `RefCell` | 第 4 章 |
| `SEQUENCE_CHECKER`（debug 斷言） | `runs_tasks_in_current_sequence()` | token 比對 | 第 4 章 |
| `base::TaskTraits` initializer list | struct update 語法 + `Default` | `..Default::default()` | 第 3 章 |
| `Sequence::Transaction` | `MutexGuard<SequenceInner>` | guard 即交易 | 第 4 章 |
| `base::Lock` / `ConditionVariable` | `Mutex` / `Condvar` | wait 收走 guard | 第 4、5 章 |
| `base::AtomicFlag` | `AtomicBool` | `swap` / ordering | 第 4 章 |
| `TaskShutdownBehavior`（同名三值） | `TaskShutdownBehavior` | 鎖內不變量 | 第 8 章 |
| `PostTaskAndReply(WithResult)` | `post_task_and_reply`（+ 自己寫泛型版） | 捕捉時機、所有權轉移 | 第 9 章 |
| `base::RepeatingTimer` | `RepeatingTimer` | `Weak` 失效 = 取消 | 第 7 章 |
| `base::TimeDelta` / `TimeTicks` | `Duration` / `Instant` | std::time | 第 2 章 |
| `base::MessagePump(ForIO)` | `trait MessagePumpForIo` + `EpollMessagePump` | trait 作平台縫隙 | 第 10 章 |
| `MessagePumpForIO::FdWatcher` | `trait FdWatcher` | `Weak` watcher（不延命） | 第 10 章 |
| `MessagePumpForIO::FdWatchController` | `FdWatchController` | `Drop` RAII + 世代號 | 第 10 章 |

### A.2 本系列教過的 Rust 概念索引

| 概念 | 首次出現 | Rust Book |
|---|---|---|
| 所有權、move、借用 | 1.4 | [ch4](../../reference/book/src/ch04-00-understanding-ownership.md) |
| `thread::spawn`、`JoinHandle`、`move` closure | 1.5 | [ch16-01](../../reference/book/src/ch16-01-threads.md) |
| `Send` / `Sync`、auto trait | 1.6 | [ch16-04](../../reference/book/src/ch16-04-extensible-concurrency-sync-and-send.md) |
| channel 與訊息傳遞 | 1.7 | [ch16-02](../../reference/book/src/ch16-02-message-passing.md) |
| closure 三種捕捉、`FnOnce`/`FnMut`/`Fn` | 2.2 | [ch13-01](../../reference/book/src/ch13-01-closures.md) |
| trait object、`dyn`、裝箱 | 2.3 | [ch18-02](../../reference/book/src/ch18-02-trait-objects.md)、[ch15-01](../../reference/book/src/ch15-01-box.md) |
| `'static` 與生命週期 | 2.3 | [ch10-03](../../reference/book/src/ch10-03-lifetime-syntax.md) |
| `Option<T>`、`take`、`map` | 2.4、4.4、5.3 | [ch6-01](../../reference/book/src/ch06-01-defining-an-enum.md) |
| `enum`、`match` 窮盡性、`if let` | 3.2、9.2 | [ch6](../../reference/book/src/ch06-00-enums.md) |
| `derive`、`Copy`、`Default`、struct update | 3.2 | [ch5](../../reference/book/src/ch05-00-structs.md) |
| trait 定義／實作／預設方法、靜態 vs 動態分派 | 3.3、10.2 | [ch10-02](../../reference/book/src/ch10-02-traits.md) |
| `Arc`、`Arc::clone` 慣例 | 3.4 | [ch15-04](../../reference/book/src/ch15-04-rc.md) |
| supertrait、object safety | 4.2 | [ch20-02](../../reference/book/src/ch20-02-advanced-traits.md) |
| newtype | 4.3 | [ch20-03](../../reference/book/src/ch20-03-advanced-types.md) |
| `thread_local!`、`RefCell`、內部可變性 | 4.3 | [ch15-05](../../reference/book/src/ch15-05-interior-mutability.md) |
| `Drop` / RAII、`_` 前綴 vs `_` 陷阱 | 4.4 | [ch15-03](../../reference/book/src/ch15-03-drop.md) |
| `Mutex` / `MutexGuard`、鎖中毒 | 4.5 | [ch16-03](../../reference/book/src/ch16-03-shared-state.md) |
| atomics、memory ordering 直覺 | 4.5 | （Book 未深入；見 std 文件） |
| `Weak`、引用循環 | 4.5、6.2 | [ch15-06](../../reference/book/src/ch15-06-reference-cycles.md) |
| `BinaryHeap`、自訂 `Ord`、`Reverse` | 4.5、5.5 | （std 文件） |
| `while let`、`Condvar` 三鐵律、lost wake-up | 5.3、5.4 | — |
| `mem::take` | 5.4 | — |
| 泛型、`where`、trait bound、單態化 | 6.3 | [ch10-01](../../reference/book/src/ch10-01-syntax.md) |
| `self: &Arc<Self>` | 6.5 | — |
| `let-else` | 7.2 | [ch19](../../reference/book/src/ch19-00-patterns.md) |
| `matches!` | 8.2 | — |
| closure 裝飾器（包 closure） | 8.3 | — |
| `unsafe` 與 FFI 的安置 | 10.2 | [ch20-01](../../reference/book/src/ch20-01-unsafe-rust.md) |

## 附錄 B：本 repo 刻意未實作的 Chromium 機制

來自 [`rust_task/architecture.md`](../../rust_task/architecture.md) 的範圍
限制。初學可略過，但讀 Chromium 原始碼時會遇到：

| 機制 | 它是什麼 | 為何未移植 |
|---|---|---|
| `SingleThreadTaskRunner`（pool 版） | 綁定 physical thread 的 runner | 文件自己都說 prefer sequences；IO 場景由 `IoTaskRunner` 涵蓋 |
| COM STA thread | Windows COM 單執行緒套間 | 平台特定 |
| `BrowserThread` | browser process 的具名執行緒 | 產品層概念 |
| `SequenceManager` / `RunLoop` | main thread 的多佇列排程器與巢狀迴圈 | 本 repo 無 UI thread 需求 |
| nested / non-nestable task | 巢狀訊息迴圈下的任務語意 | 無巢狀迴圈（`post_non_nestable_task` 退化為 `post_task`） |
| `base::PostJob` | 平行 job 的 work-stealing API | 進階排程 |
| `CancelableTaskTracker` | 跨 sequence 取消 | `Weak` 已覆蓋主要場景 |
| `SequenceLocalStorageSlot` | sequence 級的「thread-local」 | 尚無需求 |

## 附錄 C：動手練習總集

各章「動手做」之外的綜合題，依難度排序：

1. **（暖身）跑全部範例**，每跑完一個，說出它示範哪幾章的概念：

   ```bash
   cargo run -p rust_task --example event_bus        # 第 4、6 章
   cargo run -p rust_task --example repeating_timer  # 第 7 章
   cargo run -p rust_task --example task_monitor     # 監控（系列未展開，讀 task_monitor.rs）
   cargo run -p rust_io   --example io_task_runner   # 第 10 章
   cargo run -p rust_io   --example file_proxy       # 第 10 章延伸
   ```

2. **（驗證順序保證）** 8 條執行緒對同一個 `SequencedTaskRunner` post 帶
   編號任務，驗證執行順序＝post 完成順序。注意你得自己定義「post 順序」——
   8 條執行緒同時 post 時順序由誰決定？（答：由 `Sequence` 內 `Mutex` 的
   取得順序決定；測試要嘛單執行緒 post、要嘛只驗證「每條執行緒自己的任務
   相對有序」。）
3. **（破壞並修復）** 第 5 章動手做 2 的進階版：破壞 `claimed` 不變量後，
   用 `cargo test -p rust_task -- --test-threads=1` 和預設並行模式各跑十次，
   統計失敗率差異——體會並行 bug 為什麼難重現，以及測試的並行度本身就是
   測試條件。
4. **（造輪子）** 不看原始碼，憑第 4、5 章的描述自己實作一個
   `MiniSequence`：`push_task` / `take_task` / `has_worker` 仲裁，配兩條
   worker 執行緒，用測試驗證 FIFO 與永不並發。寫完比對
   `rust_task/thread_pool/sequence.rs`，看你少考慮了哪些（提示通常是：
   did_process_task 的回傳值語意、到期 delayed task 的搬移時機）。
5. **（讀原文）** 帶着全系列的概念回去通讀
   [`reference/threading_and_tasks.md`](../../reference/threading_and_tasks.md)
   ——這次每一節你都該有對應的程式碼畫面。讀到陌生的（`PostJob`、
   `TaskEnvironment`…）就是附錄 B 的未移植清單。
6. **（往下走）** 進入 `rust_io` / `rust_net`：讀兩個 crate 的 `README.md`，
   跑 `cargo run -p rust_net --example tcp_echo`，然後追蹤一次
   `read_if_ready` 從 `EWOULDBLOCK` 到 callback 的完整路徑（第 10.5 節的
   流程圖落到實碼）。

## 附錄 D：指令速查

```bash
# CI 的四道門檻（改完任何程式碼都跑）
cargo +nightly fmt --all --check
cargo +stable clippy --workspace -- -D warnings
cargo +stable test --workspace
cargo +stable build --workspace --examples

# 單一 crate / 單一測試
cargo test -p rust_task
cargo test -p rust_task task_tracker
cargo test -p rust_task --test thread_pool_integration

# tls feature 不在預設測試範圍
cargo test -p rust_net --features tls
```
