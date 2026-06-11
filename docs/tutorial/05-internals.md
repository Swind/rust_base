# 第 5 章 ThreadPool 內部：worker、佇列與喚醒

> 本章打開引擎蓋。讀懂它，你就掌握了 Chromium `base/task/thread_pool/` 目錄的
> 骨架，以及 Rust 並行工具箱裡最後幾件大型器材（`Condvar`、`JoinHandle` 管理）。
> Chromium 素材：[`base/task/thread_pool/task_source.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task_source.h)、
> [`thread_group.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/thread_group.h)、
> [`priority_queue.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/priority_queue.h)。
> Rust 素材：Rust Book [ch16-01 threads](https://rust-lang.tw/book-tw/ch16-01-threads.html)、
> [ch20-02 多執行緒 Web Server](https://rust-lang.tw/book-tw/ch20-02-multithreaded.html)
> （Book 終章手寫的迷你 thread pool，是本章絕佳的「玩具版對照組」）。
> 主角程式碼：[`rust_task/thread_pool/thread_group.rs`](../../rust_task/thread_pool/thread_group.rs)、
> [`task_source.rs`](../../rust_task/thread_pool/task_source.rs)、
> [`priority_queue.rs`](../../rust_task/thread_pool/priority_queue.rs)。

## 5.1 全景圖與一個 task 的一生

```
ThreadPool
  ├── TaskTracker          — shutdown 生命週期、BlockShutdown 計數（第 8 章）
  ├── DelayedTaskManager   — 計時執行緒，(deadline, Sequence) 的 min-heap（第 7 章）
  └── ThreadGroup          — worker threads ＋ PriorityQueue<TaskSource>
        └── worker 競爭並執行 Sequence 的 task
              └── Sequence — immediate VecDeque ＋ delayed BinaryHeap（第 4 章）
```

一個 task 的一生：

1. `post_task(traits, callback)` → `TaskTracker::will_post_task`（shutdown 後
   拒收，第 8 章）；
2. callback 被 `wrap()` 包一層，將在執行期落實 shutdown 行為與監控計時；
3. task 進入某個 `Sequence`（平行任務→新建的一次性 sequence；sequenced 任務→
   runner 常駐的 sequence），sequence 被 push 進 `ThreadGroup` 的優先序佇列；
4. 一個 worker 醒來，從佇列取出 sequence：`will_run_task`（搶 `has_worker`）→
   `take_task` → 執行 callback → `did_process_task`；
5. 若 sequence 還有剩餘任務，worker 把它**重新排隊**——所以一個長長的 sequence
   不會霸佔 worker，每跑一個 task 就回去重新競爭，公平性由佇列保證。

## 5.2 TaskSource：引擎唯一認識的東西

worker 不認識「平行任務」「sequenced 任務」「延遲任務」，它只認識
`TaskSource`（[`rust_task/thread_pool/task_source.rs`](../../rust_task/thread_pool/task_source.rs)）：

```rust
pub trait TaskSource: Send + Sync {
    fn get_execution_environment(&self) -> ExecutionEnvironment;  // token + runner
    fn get_sort_key(&self) -> TaskSourceSortKey;                  // 排程用
    fn has_ready_tasks(&self, now: Instant) -> bool;
    fn will_run_task(&self) -> RunStatus;       // 搶執行權
    fn take_task(&self) -> Option<Task>;        // 取一個 task
    fn did_process_task(&self) -> bool;         // 歸還執行權；回報還有沒有貨
    fn will_re_enqueue(&self, now: Instant) -> bool;
}
```

`Sequence` 是它唯一的實作（第 4 章已看過 `will_run_task` / `take_task` /
`did_process_task` 的內容）。對應 Chromium 的
[`base/task/thread_pool/task_source.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task_source.h) ——那邊還有 `JobTaskSource`
（平行 job）等其他實作，本 repo 未移植。

〔Rust 教學〕注意所有方法都收 `&self`（第 3 章講過的並行 API 形狀）：
`Arc<dyn TaskSource>` 被佇列和多個 worker 同時持有，內部狀態變更全靠
`Mutex` / atomics。Chromium 對應物的註解裡滿是「must hold a Transaction」
的警句；Rust 版把警句變成了型別。

## 5.3 ThreadGroup 與 worker 主迴圈

[`rust_task/thread_pool/thread_group.rs`](../../rust_task/thread_pool/thread_group.rs)：

```rust
struct ThreadGroupInner {
    priority_queue: PriorityQueue,
    handles: Vec<JoinHandle<()>>,        // worker 的 join handle
}

pub struct ThreadGroup {
    inner: Mutex<ThreadGroupInner>,
    condvar: Condvar,
    shutdown: AtomicBool,
}

impl ThreadGroup {
    pub fn new(num_threads: usize, monitor: Option<Arc<TaskMonitor>>) -> Arc<Self> {
        let group = Arc::new(Self { /* ... */ });
        {
            let mut inner = group.inner.lock().unwrap();
            for _ in 0..num_threads {
                let group_clone = Arc::clone(&group);
                let handle = thread::spawn(move || worker_loop(group_clone, ...));
                inner.handles.push(handle);
            }
        }
        group
    }
}
```

〔Rust 教學〕看 `new` 怎麼解決「worker 需要引用建立它的 group」：先把 group
裝進 `Arc`，再為每個 worker `Arc::clone` 一份 move 進 closure。這就是第 3 章
預告的「`new` 回傳 `Arc<Self>` 的第二個理由」——**物件內部的執行緒要引用物件
本身，唯一安全的表達就是共享所有權**。Rust Book ch20-02 的迷你 thread pool
用 channel 繞開了這個需求，兩種解法對照讀很有收穫。

worker 主迴圈（`thread_group.rs:96`）：

```rust
fn worker_loop(group: Arc<ThreadGroup>, monitor: Option<Arc<TaskMonitor>>) {
    while let Some(registered) = group.get_work() {       // 阻塞等工作；shutdown → None → 迴圈結束
        let source = registered.into_source();
        let env = source.get_execution_environment();

        let _token_guard = ScopedSequenceToken::new(env.token);             // 第 4.3 節
        let _default_handle = env.task_runner.map(CurrentDefaultHandle::new); // 第 4.4 節

        let claimed = match source.will_run_task() {
            RunStatus::Disallowed => false,     // 別的 worker 已持有此 sequence
            _ => {
                if let Some(task) = source.take_task() {
                    (task.callback)();          // ← 你 post 的 closure 在這裡執行！
                }
                true
            }
        };

        if claimed && source.did_process_task() {
            group.push_task_source(source);     // 還有貨 → 重新排隊
        }
    }
}
```

〔Rust 基礎〕`while let Some(x) = expr`：每輪對 `expr` 做模式匹配，`Some` 就
解出值進迴圈體，`None` 就結束——「一直取直到沒有」的標準寫法。shutdown 信號
就藏在這裡：`get_work()` 回 `None` ⇒ 迴圈自然結束 ⇒ worker thread 落幕。

〔Rust 基礎〕`env.task_runner.map(CurrentDefaultHandle::new)`：`Option::map`
——`Some(runner)` 就包成 `Some(handle)`、`None` 原樣傳過。比 `match` 短，是
`Option` 鏈式處理三兄弟（`map` / `and_then` / `unwrap_or`）之一。

### 一條藏在 `claimed` 裡的不變量

`did_process_task()` 會清掉 `has_worker` 旗標。**只有 `will_run_task()` 沒回
`Disallowed` 的 worker 才可以呼叫它**。想像違反的後果：

1. worker A 搶到 sequence（`has_worker = true`），開始跑 task；
2. worker B 也來，被拒（`Disallowed`）——但它「順手」呼叫了 `did_process_task`，
   把 `has_worker` 清成 `false`；
3. worker C 到場，`will_run_task` 成功——**現在 A 和 C 同時在跑同一個
   sequence**，FIFO 與互斥全毀。

這就是 `claimed` 這個區域變數存在的全部意義。這類「**誰擁有狀態、誰才有資格
改它**」的推理是並行程式碼 review 的日常；本章動手做第 2 題會讓你親手破壞它、
看測試怎麼炸。

## 5.4 等待與喚醒：`Condvar`

沒工作時 worker 不能空轉燒 CPU，要睡；新工作來了要叫得醒。這是條件變數
（condition variable）的標準舞台：

```rust
pub fn get_work(&self) -> Option<RegisteredTaskSource> {
    let mut inner = self.inner.lock().unwrap();
    loop {
        if self.shutdown.load(Ordering::Acquire) {
            return None;
        }
        if let Some(source) = inner.priority_queue.pop() {
            return Some(RegisteredTaskSource::new(source));
        }
        // wait：原子地「釋放鎖＋睡眠」；被喚醒時重新取得鎖後返回
        inner = self.condvar.wait(inner).unwrap();
    }
}

pub fn push_task_source(&self, source: Arc<dyn TaskSource>) {
    {
        let mut inner = self.inner.lock().unwrap();
        inner.priority_queue.push(source);
    }                                  // ← 先放鎖
    self.condvar.notify_one();         // 再喚醒，縮短被喚醒者的等鎖時間
}
```

〔Rust 教學〕`Condvar` 三件必知的事：

1. **`wait` 的簽名強迫你守規矩。** `wait(guard) -> guard` 收走你的
   `MutexGuard`、還你一個新的——「等待前必須持鎖、醒來時自動重新持鎖」這條
   條件變數鐵律被寫進了型別，想違反都沒有語法。C++ 的
   `std::condition_variable::wait(unique_lock&)` 靠約定，忘了先 lock 是
   未定義行為。
2. **`wait` 必須包在 `loop` 裡重新檢查條件。** 作業系統允許**虛假喚醒**
   （spurious wakeup）——沒人 notify 也可能醒。醒來就重查「shutdown 了嗎？
   佇列有貨嗎？」，都不成立就再睡。
3. **notify 策略要對症。** 來一件新工作 → `notify_one`（叫醒一個就夠，全叫醒
   是驚群／thundering herd，醒來搶不到工作又得睡回去）；shutdown →
   `notify_all`（每個 worker 都必須看到退出信號）。

### lost wake-up：為什麼 shutdown 旗標要在鎖內設定

`join_all`（shutdown 路徑）的開頭有個容易被當成多餘的動作：

```rust
pub fn join_all(&self) {
    {
        let _guard = self.inner.lock().unwrap();   // 看似沒用的鎖？
        self.shutdown.store(true, Ordering::Release);
    }
    self.condvar.notify_all();

    let handles = {
        let mut inner = self.inner.lock().unwrap();
        std::mem::take(&mut inner.handles)         // 把 Vec 整個搬出來
    };                                             // ← 放鎖之後才慢慢 join
    for handle in handles {
        let _ = handle.join();
    }
}
```

假設不持鎖直接設旗標，看這個交錯：

1. worker 在 `get_work` 裡查 `shutdown` → 還是 `false`；
2. **此刻** shutdown 方設旗標、`notify_all()` ——但 worker 還沒進 `wait`，
   通知無人接收，蒸發；
3. worker 進 `wait` ——睡死，再也沒有下一次 notify。

這叫 **lost wake-up**。修法：設定旗標時持有「worker 檢查旗標與進入 wait」所用
的同一把鎖。worker 要嘛在設定前就完成檢查＋進入 wait（此時 `notify_all` 一定
在它之後、叫得醒它），要嘛在設定後才檢查（看得到新旗標）。中間的縫隙被鎖焊死。

〔Rust 基礎〕`std::mem::take(&mut x)`：把 `x` 的值搬出來、原地留下
`Default::default()`（這裡是空 `Vec`）。為什麼需要它？因為我們只有 `&mut`
（透過 guard），不能直接 move 欄位出去。搬出來再 join 的另一個理由：**join 會
阻塞很久，絕不能持鎖做**——不然 worker 退出前若需要這把鎖（例如最後一次
re-enqueue）就死鎖了。

## 5.5 PriorityQueue：優先權排程

[`rust_task/thread_pool/priority_queue.rs`](../../rust_task/thread_pool/priority_queue.rs)
給 `ThreadGroup` 的佇列加上優先序：

```rust
pub struct TaskSourceSortKey {
    priority: TaskPriority,    // 第 3 章 derive(Ord) 的 enum：宣告順序＝優先權
    ready_time: Instant,       // 同優先權 → 先就緒者先跑
}

struct QueueEntry {
    sort_key: TaskSourceSortKey,
    task_source: Arc<dyn TaskSource>,   // 不參與比較
}
```

和第 4.5(d) 節完全同一招：`Arc<dyn TaskSource>` 沒法比較，包一個 newtype、只
用 sort key 實作 `Ord`、丟進 `BinaryHeap`。兩處唯一的差別是這裡要 max-heap
語意（優先權高者先出）而 heap 預設就是 max-heap，於是在 `Ord` 裡反轉比較方向
而不是套 `Reverse`——兩種等價手法本 repo 各示範了一次，對照讀有助於把
「`BinaryHeap` ＋自訂序」這招練熟。

對應 Chromium 的 [`base/task/thread_pool/priority_queue.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/priority_queue.h) 與
`task_source_sort_key.h`。Chromium 的 sort key 還包含「worker 數」等欄位用於
更精細的公平性，本 repo 取最小子集。

## 5.6 `attach_current_thread`：把你的執行緒借給 pool

`ThreadPool` 還有個彩蛋 API，能把**呼叫者自己的執行緒**變成 worker：

```rust
let pool = ThreadPool::new(0);            // 零個內建 worker！
let p = Arc::clone(&pool);
let extra = thread::spawn(move || p.attach_current_thread());  // 這條執行緒加入打工

// ... post 工作 ...

pool.shutdown();          // 通知打工仔退出迴圈
extra.join().unwrap();    // 它不歸 pool 管，自己 join 自己的
```

實作就一行——直接在當前執行緒跑 `worker_loop`。測試
`attached_thread_runs_posted_tasks`（`thread_pool.rs`）用「零內建 worker」
證明任務真的是被外來執行緒跑掉的。這個 API 對應的場景：main thread 想在等待
期間幫忙消化任務，或嵌入方想完全掌控執行緒的建立。

## 本章小結

- 引擎只認識 `TaskSource`；`Sequence` 是唯一實作；worker 的生命＝
  「取 source → 搶執行權 → 跑一個 task → 歸還 → 視情況重新排隊」。
- `Condvar` 三鐵律：wait 持鎖（型別強制）、loop 重查（虛假喚醒）、notify 策略
  對症（one vs all）。
- lost wake-up 靠「旗標在鎖內設定」焊死；join 絕不持鎖做。
- Rust 概念入帳：`while let`、`Option::map`、`Condvar`、`mem::take`、
  「`Arc<Self>` ＋內部執行緒」的自引用模式。

## 動手做

1. 讀 Rust Book [ch20-02](https://rust-lang.tw/book-tw/ch20-02-multithreaded.html) 的
   迷你 thread pool（約 100 行），列出它與 `rust_task` 的三個差異
   （提示：任務分發機制、順序保證、shutdown 細緻度）。
2. 把 `worker_loop` 裡 `RunStatus::Disallowed` 分支改成也呼叫
   `source.did_process_task()`，跑 `cargo test -p rust_task`，觀察哪些測試
   開始**間歇性**失敗（並行 bug 的典型面貌——不是每次都炸）。改回來。
3. 把 `join_all` 裡設定 shutdown 旗標的 `_guard` 那層大括號拿掉試試——為什麼
   編譯器不讓你（提示：第二次 `lock()` 在同一 scope，`std::sync::Mutex` 不可
   重入，但這裡是死鎖風險而非編譯錯誤；想清楚 guard 的生命週期）。

## 延伸閱讀

- Chromium 真版 worker 迴圈：[`base/task/thread_pool/worker_thread.cc`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/worker_thread.cc)
  的 `WorkerThread::RunWorker`——多了 idle 超時回收、動態擴編等本 repo 未移植
  的機制。
- `RegisteredTaskSource` 在 Chromium 是個複雜的 RAII 狀態機
  （`task_source.h` 內），本 repo 簡化為薄包裝；對照可見「move 語意讓 RAII
  簡單多少」。
