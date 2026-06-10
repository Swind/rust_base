# 第 7 章 延遲任務與 RepeatingTimer

> Chromium 素材：`reference/threading_and_tasks.md` 的 Posting a Task with a
> Delay 一節、[`reference/base/timer/timer.h`](../../reference/base/timer/timer.h)、
> [`reference/base/task/thread_pool/delayed_task_manager.h`](../../reference/base/task/thread_pool/delayed_task_manager.h)。
> 主角程式碼：[`rust_task/timer.rs`](../../rust_task/timer.rs)、
> [`rust_task/thread_pool/delayed_task_manager.rs`](../../rust_task/thread_pool/delayed_task_manager.rs)。
> 範例：`cargo run -p rust_task --example repeating_timer`。

## 7.1 一次性延遲：`post_delayed_task`

Chromium：

```cpp
task_runner->PostDelayedTask(FROM_HERE, base::BindOnce(&Task), base::Hours(1));
```

本 repo：

```rust
use std::time::Duration;
runner.post_delayed_task(Box::new(|| println!("later")), Duration::from_millis(500));
```

（Chromium 文件提醒：延遲一小時的任務多半不急，記得配 `BEST_EFFORT` 優先權，
免得到期那一刻反而擠掉要緊事。`TaskTraits` 在本 repo 是 runner 建立時指定的。）

### 內部分工：誰負責「等」？

延遲任務牽涉兩個元件，職責邊界劃得很清楚：

- **`Sequence.delayed_queue`**（第 4.5 節的 min-heap）存「已屬於此 sequence、
  尚未到期」的任務。Sequence 是**唯一**修改自己佇列的人。
- **`DelayedTaskManager`** 是全域計時器：一條專屬執行緒守着
  `(deadline, Arc<Sequence>)` 的 min-heap，睡到最近的 deadline，醒來把到期的
  sequence push 回 `ThreadGroup` 的佇列——讓 worker 去跑它（worker 的
  `take_task` 自己會把到期任務從 delayed 搬到 immediate，見 4.5(a)）。

計時執行緒的主迴圈是 `Condvar` 的第三種姿勢（前兩種見第 5 章）——**限時等待**：

```rust
// delayed_task_manager.rs 主迴圈（節錄概念）
loop {
    let next_deadline = /* peek min-heap */;
    match next_deadline {
        None => { guard = condvar.wait(guard).unwrap(); }            // 沒任務：睡到有人 add
        Some(t) if t <= Instant::now() => { /* pop、push 給 ThreadGroup */ }
        Some(t) => {
            let timeout = t - Instant::now();
            (guard, _) = condvar.wait_timeout(guard, timeout).unwrap();  // 睡到 deadline 或被吵醒
        }
    }
}
```

`wait_timeout` 的喚醒原因有三：到期、有人 `notify`（新任務加入，deadline
可能更早，重算）、虛假喚醒。三種都走同一條「醒來重查」的路——又一次印證
第 5 章的鐵律：**條件變數醒來永遠重新檢查條件**。

對應 Chromium 的 `delayed_task_manager.h`；那邊掛在 service thread 上，
本 repo 用獨立執行緒，角色相同。

## 7.2 `RepeatingTimer`：把「取消」建模成「所有權消失」

對應 Chromium 的 `base::RepeatingTimer`（`reference/base/timer/timer.h`）。
文件範例：

```cpp
class A {
  void StartDoingStuff() { timer_.Start(FROM_HERE, Seconds(1), this, &A::DoStuff); }
  void StopDoingStuff()  { timer_.Stop(); }
  base::RepeatingTimer timer_;   // 解構時自動停
};
```

本 repo 用法一致：

```rust
let runner = pool.create_sequenced_task_runner(TaskTraits::default());
let timer = RepeatingTimer::new(runner);
timer.start(Duration::from_secs(1), || println!("tick"));
// ...
timer.stop();    // 或者讓 timer 被 drop，效果相同
```

### 取消機制：沒有旗標、沒有世代計數

天真的實作會給每發 pending 任務配一個「被取消了嗎」旗標，或用世代號比對。
[`rust_task/timer.rs`](../../rust_task/timer.rs) 的做法漂亮得多——**整個取消
機制就是第 6 章的 `Weak` 語意**：

```rust
struct TimerInner {
    interval: Duration,
    callback: Arc<dyn Fn() + Send + Sync + 'static>,
    runner: Arc<dyn SequencedTaskRunner>,
}

pub struct RepeatingTimer {
    runner: Arc<dyn SequencedTaskRunner>,
    active: Mutex<Option<Arc<TimerInner>>>,   // ← TimerInner 唯一的強引用在這
}

pub fn start(&self, interval: Duration, callback: impl Fn() + Send + Sync + 'static) {
    let inner = Arc::new(TimerInner { /* ... */ });
    *self.active.lock().unwrap() = Some(Arc::clone(&inner));
    schedule_next(Arc::downgrade(&inner));     // 排程只拿 Weak
}

pub fn stop(&self) {
    *self.active.lock().unwrap() = None;       // 唯一強引用消失
    // → 所有在途任務手裡的 Weak 全部失效 → 下次 fire 時 upgrade 失敗 → 靜默結束
}
```

每一發 pending 的延遲任務只持有 `Weak<TimerInner>`。`stop()`（或 drop 整個
timer）把唯一的 `Arc` 丟掉，**所有在途任務瞬間集體作廢**——不用追蹤有幾發在
飛、不用通知誰。狀態機消失了，伴生的競態也消失了。

### 自我接力與「先排程、後執行」

```rust
fn schedule_next(weak: Weak<TimerInner>) {
    let Some(inner) = weak.upgrade() else { return };   // 已 stop → 靜默結束接力
    let interval = inner.interval;
    let runner = Arc::clone(&inner.runner);
    drop(inner);                       // post 前放掉強引用，別讓佇列延命

    runner.post_delayed_task(
        bind_once(weak, |inner| {
            let cb = Arc::clone(&inner.callback);
            // 先排下一發、再跑 callback（仿 Chromium RepeatingTimer::RunUserTask）：
            // 即使 callback 內呼叫 stop()，下一發拿的 Weak 也已注定 upgrade 失敗。
            schedule_next(Arc::downgrade(&inner));
            drop(inner);               // 跑 callback 前放掉強引用
            cb();
        }),
        interval,
    );
}
```

值得品味的三處：

1. **「先排程、後執行」**的順序刻意模仿 Chromium `RunUserTask`：保證 callback
   裡呼叫 `stop()` 也乾淨——下一發已入列，但它持的 `Weak` 升級必敗。順序反過來
   的話，「callback 跑到一半時 timer 算不算還在跑」會變成模糊地帶。
2. **兩處顯式 `drop(inner)`**：升級拿到的臨時 `Arc` 用完即放，確保「唯一
   強引用在 `active` 欄位裡」這個機制前提成立。顯式 `drop` 在 Rust 不常見，
   出現時幾乎都是在精確控制引用計數——閱讀訊號。
3. **〔Rust 基礎〕`let-else`**：

   ```rust
   let Some(inner) = weak.upgrade() else { return };
   ```

   模式匹配失敗就走 `else`（必須發散：`return` / `break` / `panic!`），成功則
   解構的變數直接在當前 scope 可用。比對舊寫法
   `let inner = match weak.upgrade() { Some(i) => i, None => return };`
   ——「驗完就用、不行就撤」的守衛語句首選。

### 因為 runner 是 sequenced，所以 tick 永不重疊

`RepeatingTimer` 收 `Arc<dyn SequencedTaskRunner>` 而非一般 runner——每發 tick
都是同一 sequence 上的 task，**前一發沒跑完，下一發絕不開始**。callback 寫慢了
不會自我踩踏，只會順延。這是第 4 章「sequence 即互斥」的免費紅利：計時器自身
完全沒寫任何防重入邏輯。

## 本章小結

- 延遲任務 = Sequence 的 delayed min-heap ＋ DelayedTaskManager 的計時執行緒
  （`wait_timeout`、醒來重查）。
- `RepeatingTimer` 的取消 = 丟掉唯一的 `Arc`，讓所有在途 `Weak` 集體失效——
  把「取消」建模成「所有權消失」，免旗標、免世代號、免競態。
- 先排下一發、再跑 callback；sequenced runner 保證 tick 永不重疊。
- Rust 概念入帳：`Condvar::wait_timeout`、`let-else`、顯式 `drop` 作為引用
  計數控制、用所有權結構替代狀態機。

## 動手做

1. 跑 `cargo run -p rust_task --example repeating_timer`，再讀
   `timer.rs` 裡的四個測試——`stop_before_first_firing_is_noop` 和
   `dropping_object_stops_timer_via_bind_repeating` 分別驗證本章兩個核心語意。
2. 思考題：如果 `schedule_next` 裡忘了第一個 `drop(inner)` 就去 `post_delayed_task`
   （closure 改捕捉 `inner` 強引用），`stop()` 還能立刻取消嗎？
   （答：不能——在途任務持強引用，要等它 fire 完才放；取消會「慢一拍」。）
3. 用 `RepeatingTimer` ＋ `bind_repeating`（第 6.5 節）寫一個「每秒回報自身
   狀態的元件」，然後 drop 元件、觀察 tick 自動停止。

## 延伸閱讀

- `reference/base/timer/timer.h`：Chromium 的 timer 家族還有 `OneShotTimer`、
  `DeadlineTimer`、`MetronomeTimer`；讀它們的註解，想想各自用本章的零件怎麼拼。
