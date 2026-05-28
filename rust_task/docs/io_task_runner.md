# IoTaskRunner 實作說明

Linux-specific，對應 Chromium 的 `MessagePumpEpoll` + `MessagePumpForIO` 設計。

---

## 整體架構

```
主執行緒 / 其他執行緒
    │
    │ post_task() / watch_file_descriptor()
    │
    ▼
┌─────────────────────────────────────┐
│           IoTaskRunner              │
│                                     │
│  tasks: Mutex<VecDeque<...>>        │◄── post_task 把 callback 放這裡
│  delayed_tasks: Mutex<BinaryHeap>   │◄── post_delayed_task 放這裡
│  watches: Mutex<HashMap<fd, entry>> │◄── watch_file_descriptor 放這裡
│  wake_fd: eventfd                   │◄── 用來喚醒 epoll
│  epoll_fd: epoll                    │
└─────────────────────────────────────┘
                │
                │ (background thread)
                ▼
           run_loop()
         ┌──────────┐
         │ 1. drain │  ← 先把所有 immediate tasks 跑完
         │  tasks   │
         ├──────────┤
         │ 2. run   │  ← 跑到期的 delayed tasks
         │  delayed │
         ├──────────┤
         │ 3. epoll │  ← 等事件（或 timeout）
         │   wait   │
         └──────────┘
```

---

## 喚醒機制：eventfd

最關鍵的問題是：`epoll_wait` 在等待 IO 事件，但另一個執行緒呼叫 `post_task`，IO 執行緒怎麼知道有新 task？

答案是 **eventfd**（`wake_fd`）：

```rust
// 初始化時，把 wake_fd 也加入 epoll 監聽
let wake_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: wake_fd as u64 };
libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, wake_fd, &mut ev);

// post_task 把 callback 放進 queue 後，寫入 wake_fd
fn wake(&self) {
    let val: u64 = 1;
    libc::write(self.wake_fd, &raw const val as *const libc::c_void, 8);
}
```

所以 epoll 同時監聽兩類事件：

| 事件來源 | 觸發原因 |
|---|---|
| 用戶的 fd（socket / pipe） | 資料到了，可以讀寫 |
| `wake_fd`（eventfd） | 有新 task 被 post，需要從 `epoll_wait` 返回 |

這跟 Chromium `MessagePumpEpoll` 裡的 `wake_event_` 設計完全一樣。

---

## FdWatchController 的生命週期管理

這是設計上最細緻的部分，需要解決三個問題。

### 問題 1：如何取消 watch？

`FdWatchController` 持有 `Weak<IoTaskRunner>` + fd + generation。取消時：

```rust
fn stop_watching_file_descriptor(&mut self) -> bool {
    let Some(weak) = self.runner.take() else { return false };
    let Some(runner) = weak.upgrade() else { return false };
    runner.unregister_fd(self.fd, self.generation) // 從 epoll 移除
}
```

`Drop` 自動呼叫它，所以 controller 離開 scope 就自動取消 watch，不需要手動清理。

### 問題 2：同一個 fd 重新 register 時，舊的 controller 不能誤刪新的 watch

用 **generation counter** 解決。每次 `watch_file_descriptor` 拿到一個新的 generation：

```
第一次 register fd=5: watches[5] = {generation: 0}, controller_old.generation = 0
第二次 register fd=5: watches[5] = {generation: 1}, controller_new.generation = 1

controller_old 被 drop → unregister_fd(5, 0)
  → watches[5].generation 是 1，不等於 0 → 什麼都不做 ✓
```

### 問題 3：watcher 物件被 drop 後，callback 要自動靜音

`watches` 裡存的是 `Weak<dyn FdWatcher>`，不是 `Arc`。epoll 觸發時：

```rust
let watcher = entry.watcher.upgrade(); // 嘗試升級
// ...
if let Some(w) = watcher_opt {
    w.on_file_can_read_without_blocking(fd); // 只有物件還活著才呼叫
}
```

watcher 被 drop 後，`upgrade()` 返回 `None`，callback 自動跳過，不需要任何額外清理。

---

## Deadlock 防範

epoll 觸發後，**一定要先釋放 `watches` 的 lock，才能呼叫 callback**。

原因是 Chromium 的 per-operation 使用模式：callback 內部會再次呼叫 `watch_file_descriptor` 以重新 arm watch，而 `watch_file_descriptor` 需要 acquire `watches` lock。如果 callback 呼叫時還持著 lock，就會 deadlock。

`run_loop` 裡的解法：

```rust
// ① 持 lock 期間只做取值，不呼叫任何使用者程式碼
let (watcher_opt, can_read, can_write) = {
    let mut watches = runner.watches.lock().unwrap();
    let watcher = entry.watcher.upgrade();
    if !entry.persistent {
        watches.remove(&fd); // EPOLL_CTL_DEL
    }
    (watcher, can_read, can_write)
}; // ← lock 在這裡釋放

// ② lock 釋放後才呼叫 callback
if let Some(w) = watcher_opt {
    w.on_file_can_read_without_blocking(fd); // 此時可以安全地 re-arm
}
```

---

## Non-persistent Watch（Per-operation 模式）

這是 Chromium `SocketPosix::ReadIfReady` 的核心模式。`persistent = false` 時，watch 觸發一次就自動從 epoll 移除，使用者必須主動再次呼叫 `watch_file_descriptor` 來重新 arm。

```
read_if_ready(cb):
    try read()
      ├─ 成功 → 直接呼叫 cb
      └─ EAGAIN → watch_file_descriptor(fd, persistent=false, ...)

                    ↓ 資料到了，epoll 觸發

               on_file_can_read_without_blocking()
               [watch 已自動移除，FdWatchController 仍存在但 is_watching() = false]
               呼叫 cb
               → 使用者在 cb 內再次呼叫 read_if_ready 以重新 arm
```

對比 `persistent = true`：watch 持續有效直到 `controller.stop_watching_file_descriptor()` 被呼叫，適合需要持續監聽的場景（例如 accept loop）。

---

## 與現有 SequencedTaskRunner 的整合

`IoTaskRunner` 完整實作 `SequencedTaskRunner` trait，行為與 `PooledSequencedTaskRunner` 一致：

| 功能 | 行為 |
|---|---|
| `runs_tasks_in_current_sequence()` | 在 IO 執行緒上回傳 `true` |
| `current_default()` | 在 IO 執行緒上回傳此 runner |
| `post_task_and_reply` | reply 回到 caller 的 runner（與 thread pool 完全一致） |
| `IoTaskRunner::current()` | 對應 Chromium 的 `CurrentIOThread::Get()` |

IO 執行緒啟動時，`run_loop` 設定兩個 thread-local：

```rust
let _token_guard = ScopedSequenceToken::new(runner.token);
let _default_handle = CurrentDefaultHandle::new(Arc::clone(&runner) as Arc<dyn SequencedTaskRunner>);
```

這讓跑在 IO 執行緒上的任何 task 都能透過 `current_default()` 取回這個 runner，用於 `post_task_and_reply` 的 reply 路由。

---

## Chromium 對應關係

| 本實作 | Chromium 對應 |
|---|---|
| `FdWatcher` trait | `MessagePumpForIO::FdWatcher` |
| `FdWatchController` | `MessagePumpForIO::FdWatchController` |
| `WatchMode` | `MessagePumpForIO::Mode` |
| `IoTaskRunner::watch_file_descriptor` | `MessagePumpForIO::WatchFileDescriptor` |
| `IoTaskRunner::current()` | `CurrentIOThread::Get()` |
| `wake_fd` (eventfd) | `MessagePumpEpoll::wake_event_` |
