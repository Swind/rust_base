# 第 9 章 post_task_and_reply：跨 sequence 的請求／回覆

> Chromium 素材：`reference/threading_and_tasks.md` 的 Keeping the Browser
> Responsive 一節、[`reference/base/task/task_runner.h`](../../reference/base/task/task_runner.h)
> 中 `PostTaskAndReply` 的註解。
> 主角程式碼：[`rust_task/thread_pool/pooled_sequenced_task_runner.rs`](../../rust_task/thread_pool/pooled_sequenced_task_runner.rs)
> 的 `post_task_and_reply`（九行）。

## 9.1 模式：重活外包，結果送回家

Chromium 文件的招牌反例——在 main thread 上直接做磁碟 I/O：

```cpp
// GetHistoryItemsFromDisk() 可能阻塞很久；
// AddHistoryItemsToOmniboxDropdown() 更新 UI，必須在 main thread。
AddHistoryItemsToOmniboxDropdown(GetHistoryItemsFromDisk("keyword"));  // ❌ UI 凍結
```

正解：

```cpp
base::ThreadPool::PostTaskAndReplyWithResult(
    FROM_HERE, {base::MayBlock()},
    base::BindOnce(&GetHistoryItemsFromDisk, "keyword"),     // pool 上跑
    base::BindOnce(&AddHistoryItemsToOmniboxDropdown));      // 回 main thread 跑
```

這個模式之所以重要，是因為它配合第 4 章的「狀態綁定 sequence」原則構成完整
工作流：**狀態永遠只在自己的 sequence 上被碰；要做重活，把「活」送出去、
把「結果」接回來，狀態本身永不離家**。Chromium 文件 "Memory ordering
guarantees" 一節描述的所有權接力棒模型（每個任務獨占地存取某些物件，post
即交棒）說的就是這件事。

## 9.2 實作：九行

[`pooled_sequenced_task_runner.rs:69`](../../rust_task/thread_pool/pooled_sequenced_task_runner.rs)：

```rust
fn post_task_and_reply(
    &self,
    task: Box<dyn FnOnce() + Send + 'static>,
    reply: Box<dyn FnOnce() + Send + 'static>,
) -> bool {
    // 在「呼叫的當下」捕捉呼叫者的 current_default runner
    let reply_runner = crate::sequenced_task_runner::current_default();
    let wrapped: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
        task();
        if let Some(runner) = reply_runner {
            runner.post_task(reply);
        }
    });
    self.post_task(wrapped)
}
```

把前面各章的零件串起來看它為什麼成立：

1. 呼叫者此刻正在某個 sequence 的任務裡執行 → worker 早已透過
   `CurrentDefaultHandle`（4.4 節）把該 sequence 的 runner 放進 thread-local；
2. `current_default()` 在 **post 的當下**讀出它，move 進 `wrapped` closure
   ——closure 成了一個「帶着回郵信封的包裹」；
3. `task` 在目標 runner（可能是另一個 sequence、可能是平行 pool）跑完後，
   `reply` 被 post 回信封上的地址。

### 捕捉時機是全部的精髓

試想錯誤版本——在 closure **裡面**才查 thread-local：

```rust
let wrapped = Box::new(move || {
    task();
    if let Some(runner) = crate::sequenced_task_runner::current_default() {  // ❌
        runner.post_task(reply);
    }
});
```

執行 `wrapped` 的是**目標 sequence 的 worker**，此刻 thread-local 裡是
**目標** sequence 的 runner——reply 會被 post 回工作現場，而不是呼叫者家裡。
編譯器抓不到這種錯（型別完全相同），只有語意推理能抓。

〔Rust 基礎〕這也是複習 move closure 語意的好例子：`let reply_runner = ...`
這行在 `post_task_and_reply` 的執行緒上執行（值此刻就固定了），`move` 把
**值**搬進 closure；closure 之後在哪條執行緒跑、何時跑，都不影響它捕捉到
的內容。「**closure 捕捉的是建構當下的值，不是執行當下的環境**」——在跨執行緒
場景裡，這個區別就是正確與錯誤的分界線。

### 〔Rust 基礎〕`if let`：只關心一種情況的 `match`

```rust
if let Some(runner) = reply_runner {
    runner.post_task(reply);
}
```

等價於 `match reply_runner { Some(runner) => {...}, None => {} }`
（[ch6-03](../../reference/book/src/ch06-03-if-let.md)）。`None` 在這裡的
語意：呼叫者不在任何 sequence 上（例如從 main 函式直接呼叫）——沒有「家」可回，
reply 被靜默丟棄。這是個值得商榷的設計決策（Chromium 版會 DCHECK），本 repo
選擇了寬容；讀開源程式碼時注意這種「邊界情況的態度」往往藏在最不起眼的
`if let` 裡。

## 9.3 沒有 `WithResult`？所有權讓它變普通寫法

細心的讀者會發現本 repo 只有 `post_task_and_reply`，沒有 Chromium 的
`PostTaskAndReplyWithResult`（work 的回傳值自動變成 reply 的參數）。原因：
C++ 需要專門的模板支援來安全地把值從一條執行緒搬到另一條；Rust 的所有權系統
讓你直接寫：

```rust
let (tx, rx) = std::sync::mpsc::channel();      // 或共享一個 Arc<Mutex<Option<T>>>

runner.post_task_and_reply(
    Box::new(move || {
        let result = heavy_compute();           // 在目標 sequence 算
        tx.send(result).unwrap();               // 所有權隨訊息轉移（第 1.7 節）
    }),
    Box::new(move || {
        let result = rx.recv().unwrap();        // 回到家再取出
        update_state(result);
    }),
);
```

或者更輕的慣用法——用 closure 鏈把結果「縫」進 reply（練習 1 會讓你把它包成
泛型函式）。`Send` bound 保證搬過去的東西是安全的，剩下的只是管道選擇。

## 本章小結

- `post_task_and_reply` ＝「狀態不動，活動」模式的基礎設施：task 出差、
  reply 回家，家的地址在 **post 當下**從 thread-local 取得。
- closure 捕捉的是建構時的值——跨執行緒程式裡，「何時捕捉」與「在哪執行」
  必須分開推理。
- Rust 概念入帳：`if let`、move 捕捉時機、用 channel / 所有權轉移取代
  `WithResult` 模板。

## 動手做

1. 實作泛型版 `post_task_and_reply_with_result`：

   ```rust
   fn post_task_and_reply_with_result<R: Send + 'static>(
       runner: &dyn TaskRunner,
       work: impl FnOnce() -> R + Send + 'static,
       reply: impl FnOnce(R) + Send + 'static,
   ) -> bool
   ```

   提示：在 work closure 裡算出 `R`，直接讓內層 closure 捕捉它再 post——
   你會發現連 channel 都不用（一個 `Option<R>` 都不用），純 closure 嵌套
   就夠。寫完對照第 4.2 節 `delete_soon` 的「泛型自由函式」手法。
2. 寫測試驗證 reply 真的回到呼叫者的 sequence：在 sequence A 的任務裡對
   sequence B 呼叫 `post_task_and_reply`，在 reply 裡斷言
   `runner_a.runs_tasks_in_current_sequence()`。

## 延伸閱讀

- `reference/base/task/bind_post_task.h`：Chromium 把「callback 綁定回某個
  runner」抽象成 `BindPostTask`——任何人拿到這個 callback，呼叫它都等於 post
  回指定 sequence。想想用本章零件在 Rust 怎麼實作（提示：closure 捕捉
  `Arc<dyn TaskRunner>` ＋ inner callback）。
