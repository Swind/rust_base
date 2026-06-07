# 把 rust_base 做成 Rust async runtime 的可行性評估

這份文件評估「將 `rust_base`（`rust_task` + `rust_io` + `rust_net`）發展成一個
Rust async runtime」的可行性，對照組是 `reference/async-std`（見其
`docs/runtime-overview.md` 的 runtime 拆解）。

## 核心結論

**結構上非常適合 —— 因為 `rust_base` 本身已經是一個 runtime，只是它是 callback /
CPS（continuation-passing style）風格，不是 Future 風格的。**

async-std 那份分析裡列出的每一個 runtime 核心元件，`rust_base` 都已經有完整的對應實作，
不是雛形。因此「做成 Rust runtime」對 `rust_base` 而言**不是重寫，而是在現有核心上補一層
Future / poll / Waker 的薄門面**。

要誠實點出的本質差異：一般人講「Rust runtime」時，預期的是 `async` / `.await` /
`Future` / `Waker`。`rust_base` 現在的調度單位是 `Box<dyn FnOnce() + Send + 'static>`
（一次性 callback、手動串接），不是可暫停、可續跑的 Future。這個差異決定了工作量落在哪裡。

## 元件對照

async-std 把 runtime 拆成五個依賴 crate，`rust_base` 幾乎每個都有現成對應：

| async-std 拆法 | 角色 | rust_base 對應 |
|---|---|---|
| `async-executor` | Runnable queue + poll loop + waker 重排 | `ThreadPool` + `TaskRunner`（FnOnce queue） |
| `async-global-executor` | 全域 thread pool、`spawn`、`block_on` | `ThreadPool`（已是可共享的 `Arc`） |
| `async-io` reactor | epoll → 找 Source → `waker.wake()` | `rust_io` 的 `EpollMessagePump` → `FdWatcher` callback |
| `async-io` Timer | timer map、deadline 喚醒 | `post_delayed_task` / `RepeatingTimer`（已有 timer thread） |
| `blocking`（`spawn_blocking`） | 把阻塞 syscall 丟 thread pool | `FileProxy` 把 `pread`/`pwrite` offload 到 `ThreadPool` 再回 IO thread |

關鍵觀察：async-io 那套「reactor 收到 epoll event → wake task → 重新排進 queue」的流程
（`runtime-overview.md` 的 Reactor 喚醒 Task 一節），對應到 `rust_io` 就是
「epoll event → 找 watcher → 呼叫 callback」。**唯一的差別是最後一步是 `wake()`
還是直接呼叫 callback。**

## 真正的差距（要補的東西）

差距不在架構，在「調度單位」。需要補三件事：

### 1. Runnable / Task 抽象（把 Future 包成可重排的東西）

這是 `async-executor` 的核心：一個 Runnable 被 poll 一次，`Ready` 就結束，`Pending`
就停著等 Waker 把它重新 push 回 queue。在 `rust_base` 裡，「push 回 queue」天然就是
`task_runner.post_task(...)`。

### 2. Waker ↔ reactor 整合（最關鍵、但最小的一塊）

今天 `FdWatcher::on_file_can_read_without_blocking` 是在 IO thread 上**呼叫
callback**。要支援 Future，需要一個 `Async<T>` wrapper（對應 async-io 的
`Async` / `Source`）：`poll_readable(cx)` 把 `cx.waker()` 存起來，readiness 來時改成
`waker.wake()`（而 wake 的效果就是 re-post Runnable）。

這是整個轉換的樞紐 —— 但它其實很小，就是一個「持有 wakers 的 `FdWatcher` 實作」。
epoll loop 已經存在。

### 3. `block_on` + `JoinHandle: Future` + detach 語意

`block_on` 要在當前 thread 跑 executor、idle 時 pump reactor 直到 root future 完成。
`IoTaskRunner` 本來就是個 blocking 的 epoll loop，改造空間很自然。

### 最省力的實作路徑

`async-global-executor` 本身是用 `async-task` crate（提供 `Runnable` / `Task` 機器）
\+ `async-executor` 拼出來的。可以直接：

```text
async_task::spawn(future, schedule_fn)
   其中 schedule_fn = move |runnable| task_runner.post_task(Box::new(move || runnable.run()))
```

這樣第 1、3 點幾乎白送，只需自己寫第 2 點（reactor 接 Waker），而第 2 點已有 epoll loop。
一個 spike 原型的工作量是「天」等級，不是「月」。

## rust_base 拿來做 runtime 的獨特優勢

這些是相較 async-std / Tokio **更強**的地方，值得當賣點：

- **`SequencedTaskRunner`** —— 不用 mutex、不綁定特定 thread 的「有序、不併發」執行單位。
  Tokio / async-std 沒有乾淨的對應物（只有 `LocalSet` / 單執行緒），是個真正好的原語，
  可以包成 actor / 結構化併發的基礎。
- **分級 shutdown**（`SkipOnShutdown` / `ContinueOnShutdown` / `BlockShutdown`）——
  async-std / Tokio 的 graceful shutdown 是出了名的弱（detached task、沒有 drain 保證）。
  這是實打實的差異化。
- **內建 `TaskMonitor`**（metrics + hang detection）—— Tokio 要靠 tokio-console，
  `rust_base` 原生就有。
- `TaskTraits` 優先級、`bind_once` 的 weak/strong 生命週期語意（天然對應 cancellation）。

## 要付的代價 / 風險

- **沒有 `async` / `await` 之前，使用者寫的是 CPS callback chain** —— 正是 Rust 生態
  當年想擺脫的東西。要稱得上「runtime」，`.await` 幾乎是門檻。
- **生態互通成本高**：要好用，future 得實作 std `Future`，最好還實作 `futures-io` 的
  `AsyncRead` / `AsyncWrite`，hyper 等才能接上。這是個大表面積。
- **單一 IO thread**：reactor 是單執行緒 epoll（Chromium 模型），所有 socket 的 poll
  都漏斗過這一條 IO thread；當通用 runtime 的高併發場景可能要考慮多 reactor。
- **設計理念張力**：`rust_base` 刻意鏡像 Chromium，而 Chromium **故意不用** stackless
  coroutine / future，就是 CPS callback。把它變成 Future runtime 某種程度上逆著它的
  設計初衷。建議當成明確決策：保留 Chromium CPS 身份，還是轉向 Future runtime？
  其實**兩者可並存** —— callback 核心不動，加一層 Future façade。
- `rust_io` / `rust_net` 目前 Linux only。

## 建議

值得做，路徑清楚。動手的話建議先做一個最小 spike 驗證樞紐（第 2 點）：

1. 用 `async-task` 把 schedule fn 接到一個 `TaskRunner`。
2. 寫一個 reactor-backed `Async<TcpStream>`（`poll_readable` 存 waker，`FdWatcher`
   readiness 時 `wake()`）。
3. 跑通「`block_on` 一個 `connect().await` + `read().await`」。

這個例子能跑通，整條路就驗證了，剩下都是補表面積。

## 兩種「做成 runtime」的解讀

- **(a) 加一層 Future executor**（`async` / `await`）—— 本文主要按此評估，也是一般講
  「Rust runtime」的意思。
- **(b) 以現在的 callback 風格包裝成可發佈的 runtime** —— 技術上它已經是了，方向會偏向
  打磨 API、文件、跨平台 backend，而非引入 Future。

兩者並非互斥；最務實的做法是保留 (b) 的 callback 核心，在其上長出 (a) 的 Future 門面。

## Spike 結果（已驗證）

已實作一個 proof-of-concept crate `rust_async`，**完全不修改 `rust_task` / `rust_io`，
只消費它們的公開 API**。它驗證了上面「樞紐」那條路是通的：

| 元件 | 實作 | 檔案 |
|---|---|---|
| `block_on` | thread-parking waker，在呼叫端 thread 上 drive root future | `rust_async/block_on.rs` |
| `spawn` / `JoinHandle` | `async-task` 的 `Runnable`/`Task` + schedule fn = `ThreadPool::post_task` | `rust_async/executor.rs` |
| reactor（epoll → `Waker`）| 一個持有 wakers 的 `FdWatcher`，readiness 時 `wake()` 而非跑 callback | `rust_async/reactor.rs` |
| `Async<TcpStream>` | non-blocking connect/read/write，`WouldBlock` 時 `.await` 等 readiness | `rust_async/async_tcp.rs` |

驗證方式：`block_on` 跑通 `connect().await → write_all().await → read().await` 對一個
echo server，以及 `spawn` 兩個 future 後 `.await` 取結果。見
`rust_async/examples/async_tcp_echo.rs` 與 `rust_async/tests/tcp_echo.rs`。

```bash
cargo run  -p rust_async --example async_tcp_echo
cargo test -p rust_async
```

**結論被證實**：樞紐（reactor callback → `Waker`）就是一次替換，整個 spike 的核心邏輯
不到 ~400 行，且既有 crate 一行未改、現有測試全綠。

### Spike 的已知限制（刻意留白，非阻礙）

- `epoll` 是 level-triggered，故 `Async` 採 one-shot 重新 arm（`read_if_ready` pattern），
  且**一次只等一個方向**（sequential connect→write→read 夠用，同 fd 同時等讀寫未支援）。
- `Async::connect` 只支援 IPv4。
- combinators（select/join）、graceful shutdown 接線 —— 「表面積」，非可行性問題。
- 單一 reactor thread（沿用 `IoTaskRunner`）。

## 通往 async-std 對等的路線圖

目標：讓 `rust_async` 達到與 `async-std` 相同的支援度，並以 async-std 為參考實作。
async-std 本身是「async 版標準庫 facade」，其能力可拆成以下幾塊，由易到難排序：

| 階段 | 對標 async-std 模組 | 內容 | 狀態 |
|---|---|---|---|
| 0 | `task::{block_on, spawn}` | executor + reactor 樞紐 | ✅ 已完成（spike） |
| 1 | `io`（trait 層） | `futures_io::AsyncRead`/`AsyncWrite` 互通 | ✅ 已完成 |
| 2 | `net` | `TcpStream`（Clone/split）、`TcpListener`、`UdpSocket`、IPv6 | ✅ |
| 3 | `task`（補完） | `sleep`/`timeout`、`spawn_blocking`、`yield_now`、task-local（`task_local!`）、`JoinHandle` detach-on-drop | ✅ |
| 4 | `fs` | `File`（read/write/append/positional）、`fs::{read,write}` —— 包 `rust_io::FileProxy` | ✅ |
| 5 | `sync` | `Mutex`/`RwLock`/`Barrier`/`channel`（async 版） | ✅ |
| 6 | `stream` | `Stream` trait + combinators | ✅ |
| 7 | `prelude` + docs | 對齊 async-std 的 re-export 與文件 | ⬜ |

設計原則維持不變：**每一階段都只站在 `rust_task` / `rust_io` 的公開 API 上**，不修改既有
crate。其中階段 3 的 `sleep`/`timeout` 與階段 4 的 `fs` 特別划算 —— `post_delayed_task`
與 `FileProxy` 本來就是現成的對應物，幾乎是直接包一層 Future 門面。
