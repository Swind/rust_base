# 第 8 章 Shutdown：結束不是把一切丟掉

> Chromium 素材：[`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) 的 Annotating Tasks with
> TaskTraits 與 Using ThreadPool in a New Process（結尾 Shutdown 段）、
> [`base/task/task_traits.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/task_traits.h)
> 中 `TaskShutdownBehavior` 的註解、
> [`base/task/thread_pool/task_tracker.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task_tracker.h)。
> Rust 素材：Rust Book [ch20-03 Graceful Shutdown](https://rust-lang.tw/book-tw/ch20-03-graceful-shutdown-and-cleanup.html)
> （Book 版只有「等所有 worker 跑完」一種語意——對照本章可看出 Chromium 模型
> 精細在哪）。
> 主角程式碼：[`rust_task/thread_pool/task_tracker.rs`](../../rust_task/thread_pool/task_tracker.rs)（含測試 197 行）。

## 8.1 問題：程式要退出了，佇列裡的任務怎麼辦？

天真做法二選一：「全部跑完才退」（使用者按了關閉卻要等一堆不重要的任務）或
「全部丟掉」（正在寫一半的檔案直接攔腰斬）。Chromium 的答案：**讓每個任務
自己宣告**。Chromium 文件對 shutdown 後狀態的描述：

```cpp
base::ThreadPoolInstance::Get()->Shutdown();
// 此後保證：BLOCK_SHUTDOWN 的任務、以及 Shutdown 前已開跑的 SKIP_ON_SHUTDOWN
// 任務都已完成；CONTINUE_ON_SHUTDOWN 的任務可能還在跑。
```

三種行為（第 3 章 `TaskTraits` 的 `shutdown_behavior` 欄位）：

| `TaskShutdownBehavior` | shutdown 時的待遇 | 適用 |
|---|---|---|
| `SkipOnShutdown`（預設） | 還沒開跑的直接丟棄；新 post 拒收 | 可隨時放棄的工作（預載、統計） |
| `ContinueOnShutdown` | 不擋 shutdown，自生自滅；新 post 照收 | 真正無關緊要、自己會收尾的 |
| `BlockShutdown` | `shutdown()` **阻塞等它跑完** | 必須完成的工作（flush 設定檔、提交資料庫交易） |

`ThreadPool::shutdown()` 的全貌就三行，順序即語意：

```rust
pub fn shutdown(&self) {
    self.task_tracker.shutdown();          // 1. 標記開始，等 BlockShutdown 任務歸零
    self.thread_group.join_all();          // 2. 通知 worker 退出並 join（第 5.4 節）
    self.delayed_task_manager.shutdown();  // 3. 收掉計時執行緒
}
```

## 8.2 TaskTracker：一把鎖守住一條不變量

[`rust_task/thread_pool/task_tracker.rs`](../../rust_task/thread_pool/task_tracker.rs)
是整個 crate 最小、也最值得精讀的並行設計：

```rust
struct TaskTrackerInner {
    shutdown_started: bool,
    // 已 post、尚未跑完的 BlockShutdown 任務數。
    // 在 post 時（will_post_task）就遞增——所以 shutdown() 連「已排隊、
    // 還沒開始跑」的 BlockShutdown 任務也會等。
    num_tasks_blocking_shutdown: usize,
}

pub struct TaskTracker {
    inner: Mutex<TaskTrackerInner>,
    shutdown_done: Condvar,
}

impl TaskTracker {
    pub fn will_post_task(&self, traits: &TaskTraits) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutdown_started {
            return matches!(traits.shutdown_behavior,
                            TaskShutdownBehavior::ContinueOnShutdown);
        }
        if traits.shutdown_behavior == TaskShutdownBehavior::BlockShutdown {
            inner.num_tasks_blocking_shutdown += 1;
        }
        true
    }

    pub fn after_run_task(&self, traits: &TaskTraits) {
        if traits.shutdown_behavior == TaskShutdownBehavior::BlockShutdown {
            let mut inner = self.inner.lock().unwrap();
            inner.num_tasks_blocking_shutdown -= 1;
            if inner.num_tasks_blocking_shutdown == 0 {
                self.shutdown_done.notify_all();
            }
        }
    }

    pub fn shutdown(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.shutdown_started = true;
            if inner.num_tasks_blocking_shutdown == 0 {
                return;
            }
        }
        let mut inner = self.inner.lock().unwrap();
        while inner.num_tasks_blocking_shutdown > 0 {
            inner = self.shutdown_done.wait(inner).unwrap();   // 第 5 章學過的 Condvar
        }
    }
}
```

### 為什麼旗標和計數必須在同一把鎖裡

把 `shutdown_started` 和計數器拆成兩個 `AtomicBool` / `AtomicUsize` 看似更
輕量。但看這個交錯：

1. 執行緒 A 走進 `will_post_task`，讀到 `shutdown_started == false`，決定放行
   一個 `BlockShutdown` 任務——**但還沒來得及遞增計數**；
2. 執行緒 B 呼叫 `shutdown()`：設旗標、讀計數＝0，認定「沒有要等的」，返回；
3. A 這才把計數 +1、任務入列。

結果：`shutdown()` 已經回報「完成」，一個**被承諾必定執行**的 `BlockShutdown`
任務卻還躺在佇列裡——而 worker 馬上要被 join 走了。承諾跳票。

放進同一把 `Mutex`，「檢查旗標＋遞增計數」成為原子整體：A 要嘛整段發生在 B
之前（B 會看到計數 1，乖乖等），要嘛整段在 B 之後（A 看到旗標，拒收任務）。
中間狀態不存在。

這條教訓在第 4.5(a)（兩個佇列一把鎖）已出現過一次，值得升格為準則：

> **不變量橫跨幾個變數，鎖就要罩住幾個變數。**
> 拆鎖、改 atomic 是優化；先用一把鎖把正確性釘死，profiler 說太慢再說。

`task_tracker.rs` 下半部的測試 `shutdown_waits_for_queued_block_shutdown_task`
正是對這個競態的回歸測試——先 `will_post_task`（模擬入列未執行）、開背景執行緒
跑 `shutdown()`、確認它阻塞、補 `after_run_task` 後才放行。**用測試固定並行
不變量**，比註解可靠。

〔Rust 基礎〕`matches!(expr, Pattern)`：模式匹配版的布林判斷，等價
`match expr { Pattern => true, _ => false }`。比 `==` 強在可以匹配帶資料的
variant（如 `matches!(x, Some(n) if n > 0)`）。

## 8.3 執行期的最後一道閘：`wrap`

`will_post_task` 擋得住 shutdown **之後**的新任務，擋不住「shutdown 之前已
入列、之後才被 worker 撿到」的任務。所以 `ThreadPool` post 任何任務前都用
`wrap` 把 callback 再包一層（`rust_task/thread_pool/thread_pool.rs:139`）：

```rust
fn wrap(&self, traits: TaskTraits,
        callback: Box<dyn FnOnce() + Send + 'static>)
        -> Box<dyn FnOnce() + Send + 'static> {
    let callback = match self.monitor.as_ref() {
        Some(m) => m.wrap_task(callback),     // 選配的計時監控，最內層
        None => callback,
    };
    let tracker = Arc::clone(&self.task_tracker);
    Box::new(move || {
        if tracker.is_shutdown_started()
            && traits.shutdown_behavior == TaskShutdownBehavior::SkipOnShutdown
        {
            return;                            // 執行當下才做最終判定：放棄
        }
        callback();
        tracker.after_run_task(&traits);       // BlockShutdown 計數 -1
    })
}
```

兩道閘的分工：

| 時點 | 機制 | 擋什麼 |
|---|---|---|
| post 時 | `will_post_task` | shutdown 後的新 post（回傳 `false`） |
| 執行時 | `wrap` 的 closure | shutdown 前已入列的 `SkipOnShutdown` 任務 |

〔Rust 教學〕**「closure 包 closure」**是這套架構的標準擴充手法：監控計時、
shutdown 行為，層層洋蔥往上裹，核心引擎（Sequence、worker）完全不知情。注意
包裝順序的講究——監控在最內層，所以量到的 `execution_time` 只含使用者 callback、
不含 shutdown 簿記；被 skip 的任務根本不會走到監控層，不污染統計。裝飾器模式
在有所有權的語言裡格外乾淨：每層 `Box<dyn FnOnce>` 進、`Box<dyn FnOnce>` 出，
誰持有誰一目了然。

## 8.4 與 Rust Book ch20 對照

Rust Book 終章的迷你 thread pool 也做了 graceful shutdown
（[ch20-03](https://rust-lang.tw/book-tw/ch20-03-graceful-shutdown-and-cleanup.html)）：
drop channel 的發送端 → worker 的 `recv()` 回 `Err` → 迴圈結束 → join。
乾淨，但只有**一種**語意：「佇列裡的全部跑完」。

對照本章可以精確說出 Chromium 模型多出了什麼：

1. **per-task 的 shutdown 合約**（三種行為 vs 一種）；
2. **「已排隊未執行」的 BlockShutdown 任務也被等待**（計數在 post 時遞增）；
3. **shutdown 後的 post 有明確語意**（拒收／照收，回傳值告訴你）。

複雜度的來源不是技術炫耀，是瀏覽器的真實需求：使用者按下關閉的瞬間，磁碟上
可能有寫到一半的書籤、設定、cookie——哪些必須等、哪些立刻砍，得是每個呼叫點
自己最清楚。

## 本章小結

- shutdown 是三方合約：任務宣告行為（traits）、tracker 記帳（鎖內不變量）、
  `wrap` 在執行期兌現。
- 跨變數的不變量 ⇒ 同一把鎖。先正確，再優化。
- 並行不變量要用測試釘住（`task_tracker.rs` 的測試是範本）。
- Rust 概念入帳：`matches!`、closure 裝飾器模式、用 `Condvar` 實作
  「等待計數歸零」。

## 動手做

1. 跑 `cargo test -p rust_task task_tracker`，逐一讀懂七個測試各釘住哪條語意。
2. 寫個小程式：post 一個睡 200ms 的 `BlockShutdown` 任務和十個睡 200ms 的
   `SkipOnShutdown` 任務，立刻 `shutdown()`，量總耗時——驗證只等了前者。
3. 思考題：`wrap` 裡的 `is_shutdown_started()` 檢查和真正執行 `callback()`
   之間，shutdown 可能恰好開始——一個 `SkipOnShutdown` 任務於是「漏跑」了。
   這是 bug 嗎？（答：不是。`SkipOnShutdown` 的合約本來就是「可能被跳過」；
   合約沒承諾的事，實作不必擋。對照 `BlockShutdown` 為什麼就必須用鎖精確
   記帳——合約強度決定實作強度。）

## 延伸閱讀

- [`base/task/thread_pool/task_tracker.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task_tracker.h)：真版 tracker 連
  `CONTINUE_ON_SHUTDOWN` 在 shutdown 後啟動的任務數都有原子簿記，註解詳述了
  每種行為在「post / start / complete」三個時點的判定矩陣。
- [`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) 的 Testing 一節：Chromium 測試用
  `TaskEnvironment` + `RunLoop` 精確控制任務執行，本 repo 的對應手法是
  Barrier ＋ flush task（見各測試檔）。
