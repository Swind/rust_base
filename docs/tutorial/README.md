# 以 Rust 重現 Chromium 的 Threading 與 Task Runner 架構 — 教學系列

這是一份「一魚兩吃」的教學系列：

1. **學 Chromium 的並行架構** — Chromium 如何用 Task、Sequence、TaskRunner、
   ThreadPool 組織一個高度並行的程式，以及為什麼它強烈主張「用順序取代鎖」。
2. **從零開始學 Rust 的並行程式設計** — 本 workspace（`rust_task` / `rust_io` /
   `rust_net`）把上述架構移植成 idiomatic Rust。每一章在講解架構的同時，會穿插
   〔Rust 基礎〕與〔Rust 教學〕段落，從所有權、closure、trait 一路講到
   `Send` / `Sync`、`Arc` / `Weak`、`Mutex` / `Condvar`、atomics 與 RAII。
   假設讀者是 **Rust 初學者**：用到的語法都會解釋或給出 Rust Book 的精確章節。

## 三份對照素材

| 素材 | 位置 | 用途 |
|---|---|---|
| Chromium 設計文件 | [`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) | 每章開頭引用的「官方說法」 |
| Chromium `base/` 原始碼 | [`base/`](https://source.chromium.org/chromium/chromium/src/+/main:base/) | C++ 實作對照（如 `task/task_runner.h`、`memory/weak_ptr.h`） |
| The Rust Programming Language | [rust-lang.tw/book-tw](https://rust-lang.tw/book-tw/)（繁體中文譯本） | Rust 語法的權威出處（下稱 *Rust Book*）。本系列引用的章節編號以這份譯本為準；最新英文版多了 async 一章（ch17），其後章節編號比譯本大 1 |

而被講解的主角是本 workspace 的原始碼，以 `rust_task/` 為主：crate root 是
`rust_task/lib.rs`（注意本 workspace 的 crate root 都是 `<crate>/lib.rs`，不在
`src/` 下）。

## 章節目錄

| 章 | 標題 | Chromium 主題 | Rust 主題 |
|---|---|---|---|
| [01](01-philosophy.md) | 並行哲學與 Send / Sync | 多執行緒架構、三條守則、核心詞彙 | 所有權速覽、`thread::spawn`、**`Send` / `Sync` 詳解**、訊息傳遞 vs 共享狀態 |
| [02](02-task.md) | Task：可被攜帶的工作單元 | `base::OnceClosure` / `BindOnce` | **closure 的三種捕捉方式**、`FnOnce` / `FnMut` / `Fn`、`Box` 與 trait object、`'static` |
| [03](03-thread-pool.md) | ThreadPool、TaskRunner 與 TaskTraits | post task、TaskTraits、TaskRunner 介面 | `enum` 與 `match`、`struct` 與 `Default`、定義 trait、`Arc` 入門 |
| [04](04-sequence.md) | Sequence：用順序取代鎖 | sequence ≠ thread、`SEQUENCE_CHECKER` | supertrait、object safety、`thread_local!`、`RefCell`、`Drop` / RAII、`Mutex`、`AtomicBool`、`Weak` 斷循環、自訂 `Ord` |
| [05](05-internals.md) | ThreadPool 內部：worker、佇列與喚醒 | `TaskSource`、worker loop、優先序排程 | `Condvar`、spurious wakeup、lost wake-up、`while let`、`mem::take` |
| [06](06-bind-once.md) | bind_once：物件生命週期與回呼 | `base::WeakPtr`、`base::Unretained` | `Arc` / `Weak` 深入、**泛型與 trait bound**、為同一 trait 實作多個型別、單態化 |
| [07](07-timers.md) | 延遲任務與 RepeatingTimer | `PostDelayedTask`、`base::RepeatingTimer` | `let-else`、用 `Weak` 失效實作取消 |
| [08](08-shutdown.md) | Shutdown：結束不是把一切丟掉 | `TaskShutdownBehavior`、`TaskTracker` | `matches!`、鎖內不變量、與 Rust Book ch20 的 graceful shutdown 對照 |
| [09](09-post-task-and-reply.md) | post_task_and_reply：跨 sequence 協作 | `PostTaskAndReply(WithResult)` | move closure 的捕捉時機、`Option` 的鏈式處理 |
| [10](10-io-message-pump.md) | IO Thread 與 MessagePump | `MessagePumpForIO`、`FdWatcher` | trait 作為平台抽象、trait 預設方法、`RawFd` |
| [11](11-appendix.md) | 附錄 | 概念對照速查表、未實作清單 | Rust Book 章節對照、動手練習 |

## 建議閱讀路徑

- **完全照順序讀**。第 1、2 章是後面所有章節的語言基礎；第 4 章是整個架構的
  核心思想；第 5 章開引擎蓋，可以在第一輪先略讀、回頭再精讀。
- 每章末尾有「動手做」與「延伸閱讀」。範例都可以直接跑，例如：

  ```bash
  cargo run -p rust_task --example event_bus
  cargo run -p rust_task --example repeating_timer
  cargo test -p rust_task
  ```

- 讀完第 8 章後，強烈建議去讀 Rust Book 的終章
  [ch20：多執行緒 Web Server](https://rust-lang.tw/book-tw/ch20-02-multithreaded.html)——
  它從零手寫一個迷你 thread pool（含 graceful shutdown），正好是 `rust_task`
  的「玩具版」。先讀過本系列再看它，你會發現每一行都認得；反過來，比較兩者的
  差距（sequence、優先序、shutdown 行為）就是 Chromium 架構的價值所在。

## 一張圖總覽

```
                你的程式碼
                    │  post_task(traits, callback)
                    ▼
   ┌────────────────────────────────────┐
   │ ThreadPool                         │
   │  ├─ TaskTracker     （第 8 章）      │   shutdown 生命週期
   │  ├─ DelayedTaskManager（第 7 章）    │   計時執行緒
   │  └─ ThreadGroup     （第 5 章）      │   workers + PriorityQueue
   │       └─ worker 競爭 TaskSource      │
   │            └─ Sequence（第 4 章）    │   FIFO、永不並發 ＝ 免鎖
   └────────────────────────────────────┘
                    │ 跑在某個 worker 上
                    ▼
        callback()  ←─ Box<dyn FnOnce() + Send + 'static>（第 2 章）
                       由 bind_once(Arc/Weak, f) 建立（第 6 章）

   rust_io（第 10 章）：IoTaskRunner ＝ 專屬 IO 執行緒 + epoll MessagePump
   rust_net：建立在 rust_io 之上的非同步 TCP / TLS（本系列僅概述）
```
