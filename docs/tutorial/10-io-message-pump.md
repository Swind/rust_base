# 第 10 章 IO Thread 與 MessagePump

> 本章從 `rust_task` 跨進 `rust_io`，對應 Chromium 文件的 Internals 一節
> （SequenceManager / MessagePump）與 IO thread 的角色。
> Chromium 素材：[`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) 的 Threads 與 MessagePump
> 兩節、[`base/message_loop/message_pump.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/message_loop/message_pump.h)、
> [`message_pump_epoll.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/message_loop/message_pump_epoll.h)。
> 主角程式碼：[`rust_io/message_pump.rs`](../../rust_io/message_pump.rs)、
> [`rust_io/io_task_runner.rs`](../../rust_io/io_task_runner.rs)、
> [`rust_io/epoll_pump.rs`](../../rust_io/epoll_pump.rs)。
> 範例：`cargo run -p rust_io --example io_task_runner`。

## 10.1 IO thread：第三種執行緒

第 1 章的詞彙表說過，每個 Chromium process 有 main thread、一個 thread pool
——和一條 **IO thread**：所有 IPC 訊息在這裡到達，大部分非同步 I/O（透過
`base::FileDescriptorWatcher`）也在這裡發生。

它和 pool worker 的根本差異在「**睡在哪裡**」：

| | pool worker（第 5 章） | IO thread |
|---|---|---|
| 等待點 | `Condvar`（只有 task 一種事件） | **epoll**（task 到來 ＋ fd 就緒，兩種事件） |
| 喚醒源 | `notify_one` | epoll 監看的任何 fd ＋ 一個專用喚醒 fd |
| 執行緒綁定 | 無（sequence 漂移） | **有**——epoll 註冊表歸這條執行緒管 |

「同時等兩種事件」的迴圈就是 **MessagePump**。Chromium 文件對它的職責描述：

> MessagePumps are responsible for processing native messages as well as for
> giving cycles to their delegate (SequenceManager) periodically.

——既要處理原生事件（fd 就緒），也要定期把 CPU 讓給 task 層，兩邊都不能餓死。

## 10.2 rust_io 的分層

`rust_io` 把這套搬過來，層次切在和 Chromium 相同的位置：

```
IoTaskRunner（task 層：佇列、延遲任務、監控——不含任何 epoll 細節）
    │ 實作 SequencedTaskRunner（第 4 章的 trait！跨 crate 復用）
    ▼
trait MessagePumpForIo（平台抽象的「縫」）
    ▼
EpollMessagePump（Linux 後端；kqueue / IOCP 可另行實作而不動上層）
```

縫隙本身（[`rust_io/message_pump.rs`](../../rust_io/message_pump.rs)）：

```rust
/// 對應 base::MessagePumpForIO（具體說是 MessagePumpEpoll）。
pub trait MessagePumpForIo: Send + Sync + 'static {
    /// 跑事件迴圈直到 quit()。在專屬 IO 執行緒上呼叫。
    fn run(&self, delegate: Arc<dyn MessagePumpDelegate>);
    fn quit(&self);
    /// 喚醒被阻塞的迴圈（任何執行緒可呼叫）——新 task post 進來時用。
    fn schedule_work(&self);
    /// 註冊 fd 的就緒監看。**只能在 IO 執行緒上呼叫。**
    fn register_fd(&self, fd: RawFd, persistent: bool, mode: WatchMode,
                   watcher: Weak<dyn FdWatcher + Send + Sync>) -> Option<u64>;
    fn unregister_fd(&self, fd: RawFd, generation: u64) -> bool;
}

/// fd 就緒通知。對應 MessagePumpForIO::FdWatcher。
pub trait FdWatcher: Send + Sync + 'static {
    fn on_file_can_read_without_blocking(&self, fd: RawFd);
    fn on_file_can_write_without_blocking(&self, fd: RawFd);
}

/// pump 反過來驅動 task 層的介面。對應 base::MessagePump::Delegate。
pub trait MessagePumpDelegate: Send + Sync + 'static {
    /// 跑掉所有「現在就緒」的任務；回傳最早的未到期 deadline，
    /// 讓 pump 據此決定 epoll_wait 的 timeout。None = 無限等。
    fn do_work(&self) -> Option<Instant>;
    fn on_run_start(&self) {}          // ↓ 預設實作：不想管的 hook 可以不寫
    fn on_run_end(&self) {}
    fn begin_work_item(&self) {}
    fn end_work_item(&self) {}
}
```

〔Rust 教學〕三個語言點：

- **trait 作為平台縫隙**。上層只依賴 `dyn MessagePumpForIo`，Linux 給
  `EpollMessagePump`，將來 macOS 給 kqueue 版——C++ 用抽象基底類別做同一件事
  （`message_pump.h`），Rust 的 trait object 一一對應。注意這跟第 6 章
  `IntoArc` 用泛型做靜態多型不同：這裡**刻意**用動態分派，因為「換平台後端」
  不需要內聯效能，需要的是隔離。
- **trait 預設方法**（第 3.3 節埋過伏筆）：`on_run_start` 等 hook 給了空的
  預設實作，實作者只需覆寫在乎的。介面演進時不破壞既有實作的標準手法。
- **`RawFd`**：`std::os::unix::io::RawFd` 就是 Unix 的 `int` fd——Rust 對 OS
  原語不另起爐灶，`rust_io` 用 `libc` crate 直接呼叫
  `epoll_create1 / epoll_ctl / epoll_wait`（見 `epoll_pump.rs`，內含本
  workspace 少數的 `unsafe` 區塊——FFI 必經之路，每處都附帶安全性論證註解）。

`do_work` 的回傳值設計值得停一秒：task 層告訴 pump「我最早的延遲任務何時
到期」，pump 拿它當 `epoll_wait` 的 timeout——**兩種等待（等 fd、等時間）合併
成一次系統呼叫**。這就是 IO thread 不需要 `DelayedTaskManager`（第 7 章）的
原因：epoll 自己會掐錶。

## 10.3 IoTaskRunner：把兩個世界縫起來

[`rust_io/io_task_runner.rs`](../../rust_io/io_task_runner.rs)：

```rust
pub struct IoTaskRunner {
    pump: Arc<dyn MessagePumpForIo>,
    tasks: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,        // 立即佇列
    delayed_tasks: Mutex<BinaryHeap<DelayedTask>>,           // 又是 min-heap！
    shutdown: AtomicBool,
    token: SequenceToken,                                    // 來自 rust_task
    thread_handle: Mutex<Option<thread::JoinHandle<()>>>,
    // ...監控欄位
}

impl IoTaskRunner {
    pub fn new() -> Arc<Self> { /* 建 epoll pump、spawn IO 執行緒、開跑 */ }
    pub fn current() -> Option<Arc<IoTaskRunner>> { /* thread-local，IO 執行緒上才有 */ }
    pub fn watch_file_descriptor(/* fd, mode, controller, watcher */) -> bool { /* IO 執行緒限定 */ }
}
```

它**實作了 `rust_task` 的 `SequencedTaskRunner` trait**——所以第 4 章的一切
（`post_task`、`current_default`、`runs_tasks_in_current_sequence`、
`post_task_and_reply`）在 IO thread 上原樣可用。這是 trait 抽象的回報時刻：
`RepeatingTimer`（第 7 章）收 `Arc<dyn SequencedTaskRunner>`，拿一個
`IoTaskRunner` 餵它，計時器就跑在 IO thread 上，**一行都不用改**。

對照表也就完整了——本 workspace 唯一「綁定 physical thread」的 runner 就是它，
對應 Chromium 的「IO thread 上的 `SingleThreadTaskRunner` + `MessagePumpForIO`」。

## 10.4 兩條鐵律（與它們的失敗模式）

`rust_io` / `rust_net` 的所有 API 共用兩條規矩，**違反時都不報錯、只是行為
靜默消失**——先知道失敗長什麼樣，比知道規則更救命：

**鐵律一：碰 epoll 的操作只能在 IO 執行緒上做。**
`watch_file_descriptor` 等函式必須從 IO thread 呼叫——不在那裡？先
`post_task` 過去。epoll 註冊表是 IO 執行緒的私產，這是設計（單執行緒化的
註冊表免掉一整層鎖），不是疏忽。Chromium 的 `FileDescriptorWatcher` 同款。

**鐵律二：I/O 物件必須活到 callback fire 為止。**
回頭看 `register_fd` 的簽名——收的是 `Weak<dyn FdWatcher>`：

```rust
fn register_fd(&self, ..., watcher: Weak<dyn FdWatcher + Send + Sync>) -> Option<u64>;
```

事件迴圈**不持強引用**：你的 watcher（socket、connection 物件）drop 了，
下次 fd 就緒時 `upgrade()` 失敗，callback 靜默不發。這正是第 6 章
`bind_once(Weak, ...)` 語意的系統級放大——**誰擁有 I/O 物件，誰就決定
callback 的生死**；框架絕不靠佇列替你延命（否則一個忘記取消的 watch 會把
socket 押到天荒地老）。代價就是鐵律二：忘了把 `FileProxy` / `SocketPosix`
存活在某處，你的讀寫完成通知會人間蒸發。

配套的 RAII 件——`FdWatchController`：

```rust
pub struct FdWatchController {
    pump: Option<Weak<dyn MessagePumpForIo>>,
    fd: RawFd,
    generation: u64,        // 世代標記
}

impl Drop for FdWatchController {
    fn drop(&mut self) {
        self.stop_watching_file_descriptor();   // drop 即取消監看
    }
}
```

每個監看註冊對應一個 controller（慣例：放在實作 `FdWatcher` 的 struct 裡，
讀寫各一個），drop 自動 `unregister`——第 4.4 節 `Drop` RAII 的又一應用。
`generation` 解決的是 fd 重用問題：fd 是整數，關掉再開可能拿到同一個號碼；
舊 controller 的 drop 不該誤殺新註冊的 watch，世代號不符就不取消。Chromium
`FdWatchController` 同款設計。

## 10.5 再往上：rust_net 一瞥

`rust_net` 在這層地基上蓋了非同步 TCP（`SocketPosix` → `TcpClientSocket` /
`TcpServerSocket`，對應 Chromium `net/socket/socket_posix.h`）和 TLS
（`TlsClientSocket`，rustls，`tls` feature）。模式全是本章的延伸：

```text
read_if_ready(buf, callback):
    嘗試非阻塞 read(fd)
    ├─ 成功/真錯誤 → 直接呼叫 callback(結果)
    └─ EWOULDBLOCK → 存起 callback，watch_file_descriptor(fd, Read)
                      ── fd 就緒 → on_file_can_read_without_blocking
                         → 取出 callback，重試 read，回報結果
```

深入屬於另一個系列；`rust_io/README.md`、`rust_net/README.md` 與各自的
`examples/`（`file_proxy`、`tcp_echo`、`https_get`）是下一站。

## 本章小結

- IO thread = 睡在 epoll 上的 sequence：MessagePump 統一等「fd 就緒」與
  「task 到來」，`do_work` 回傳 deadline 讓兩種等待合併成一次 `epoll_wait`。
- 分層：`IoTaskRunner`（task 層）—`trait MessagePumpForIo`（縫）—
  `EpollMessagePump`（平台層）；trait 換骨架，跨 crate 復用 `SequencedTaskRunner`。
- 兩條鐵律：IO 執行緒限定（註冊表免鎖）；I/O 物件自己保活（`Weak` watcher，
  框架不延命）。失敗模式都是靜默的。
- Rust 概念入帳：trait 預設方法、動態 vs 靜態多型的選用理由、`RawFd` 與
  FFI/`unsafe` 的位置、`Drop` + 世代號的 RAII 取消。

## 動手做

1. 跑 `cargo run -p rust_io --example io_task_runner`，對照 10.3 的結構讀
   `examples/io_task_runner.rs`。
2. 用 `IoTaskRunner` 餵第 7 章的 `RepeatingTimer`，驗證 tick 跑在 IO 執行緒上
   （在 callback 裡用 `IoTaskRunner::current().is_some()` 斷言）。
3. 讀 `rust_io/epoll_pump.rs` 裡的 `unsafe` 區塊，數一數共幾處、每處的註解
   論證了什麼。「`unsafe` 集中在最底層、上層全安全」是 Rust 系統程式的標準
   剖面，這個 crate 是小而完整的樣本。

## 延伸閱讀

- [`base/message_loop/message_pump_epoll.cc`](https://source.chromium.org/chromium/chromium/src/+/main:base/message_loop/message_pump_epoll.cc)：Chromium 真版 epoll
  pump，比對 `rust_io/epoll_pump.rs`——喚醒 fd（eventfd）、interest list 管理
  等核心結構幾乎逐一對應。
- Chromium 文件 Internals 一節的 SequenceManager / RunLoop：本 repo 刻意
  未移植的上層排程設施（見第 11 章附錄 B）。
