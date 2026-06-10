# 第 1 章 並行哲學與 Send / Sync

> 本章 Chromium 素材：[`reference/threading_and_tasks.md`](../../reference/threading_and_tasks.md)
> 的 Overview、Quick start guide、Core Concepts、Threading Lexicon 各節。
> 本章 Rust 素材：Rust Book [ch4 所有權](../../reference/book/src/ch04-00-understanding-ownership.md)、
> [ch16-01 執行緒](../../reference/book/src/ch16-01-threads.md)、
> [ch16-02 訊息傳遞](../../reference/book/src/ch16-02-message-passing.md)、
> [ch16-03 共享狀態](../../reference/book/src/ch16-03-shared-state.md)、
> [ch16-04 Sync 與 Send](../../reference/book/src/ch16-04-extensible-concurrency-sync-and-send.md)。

## 1.1 Chromium 要解的問題：回應性

Chromium 是多 process 架構，且每個 process 內高度多執行緒。它的首要目標不是榨乾
每一顆 CPU 核心，而是**保持回應性（responsiveness）**——使用者的每一次點擊、捲動、
打字都要立刻有反應。官方文件的說法是：Chrome 試圖成為一個

> highly concurrent, but not necessarily parallel system.
> （高度並行〔concurrent〕、但不必然平行〔parallel〕的系統）

這兩個詞的差別值得先釐清：

- **Concurrency（並行性）**：很多件事「在進行中」，彼此交錯推進。重點是**組織**
  工作，讓誰也不擋誰。
- **Parallelism（平行性）**：很多件事「同一瞬間」在不同核心上執行。重點是**吞吐**。

Chromium 在乎的是前者：UI thread 上的下一個事件永遠能很快被處理。為此它建立了
一整套「把工作切成 task、丟到該去的地方執行」的機制——也就是本系列的主題。

## 1.2 三條守則

Chromium 文件開頭的 Quick start guide 給了三條守則，全文照引（翻譯）：

1. **不要在 main thread（UI thread）或 IO thread 上做昂貴運算或阻塞 I/O。**
   忙碌的 UI / IO thread 會造成使用者可感知的延遲，把這類工作丟給 thread pool。
2. **永遠避免從不同執行緒／sequence 讀寫同一塊記憶體**——那會導致資料競爭
   （data race）。優先用「跨 sequence 傳遞訊息」；用鎖之類的替代方案是被勸退的。
3. **跨 sequence 操作多個物件時，小心物件生命週期。** 優先讓每個類別只在單一
   sequence 上使用；經驗法則是避免 `base::Unretained`（裸指標），通常可以用
   weak pointer 取代。

把這三條記住——本系列的每一章其實都是在落實其中一條：

| 守則 | 對應章節 |
|---|---|
| 1. 重活丟給 pool | 第 3 章（ThreadPool）、第 9 章（post_task_and_reply） |
| 2. 用訊息傳遞，不要共享記憶體 | 第 4 章（Sequence 取代鎖） |
| 3. 小心物件生命週期 | 第 6 章（bind_once 與 Weak） |

## 1.3 核心詞彙

照 Chromium 文件 Core Concepts 一節的定義，加上本 repo 的對應物：

- **Task**：一個工作單元，本質是「函式＋綁定的狀態」。Chromium 用
  `base::OnceCallback`（由 `base::BindOnce` 建立，見
  [`reference/base/functional/callback.h`](../../reference/base/functional/callback.h)）；
  本 repo 用 `Box<dyn FnOnce() + Send + 'static>`（第 2 章拆解這串型別）。
- **Task queue**：等待被處理的 task 佇列。
- **Physical thread**：作業系統的執行緒。Chromium 的抽象是
  `base::PlatformThread`；Rust 是 `std::thread`。**兩邊的共同建議：幾乎永遠
  不要直接用它。**
- **Thread pool**：共用 task queue 的一群 physical thread。Chromium 是
  `base::ThreadPoolInstance`（每個 process 恰好一個）；本 repo 是
  `rust_task::ThreadPool`。
- **Sequence / Virtual thread**：由框架管理的「執行緒概念」。同一個 sequence 上
  一次只有一個 task 在跑，後一個 task 看得到前一個 task 的所有副作用——但實際執行
  的 physical thread 每次可以不同。**這是整個架構最重要的概念**，第 4 章專章討論。
- **Task runner**：可以對它 post task 的介面。Chromium 是 `base::TaskRunner`
  （[`reference/base/task/task_runner.h`](../../reference/base/task/task_runner.h)）；
  本 repo 是 `rust_task::TaskRunner` trait。
- **Sequenced task runner**：保證 task 依 post 順序、一次一個執行。Chromium 是
  `base::SequencedTaskRunner`；本 repo 是 `rust_task::SequencedTaskRunner` trait。
- **Single-thread task runner**：進一步保證所有 task 跑在**同一條 physical
  thread** 上。Chromium 文件強調「能用 sequence 就不要綁定 physical thread」
  （"Prefer Sequences to Physical Threads" 一節），所以本 repo 第一階段刻意
  **不實作**它；唯一例外是 `rust_io::IoTaskRunner`——IO 事件迴圈天生綁一條執行緒
  （第 10 章）。

---

從這裡開始是 Rust 部分。在能讀懂「task 是 `Box<dyn FnOnce() + Send + 'static>`」
之前，我們需要三塊地基：所有權、執行緒、以及 `Send` / `Sync`。

## 1.4 〔Rust 基礎〕所有權 60 秒速覽

Rust 沒有 GC，也不要求手動 free。它靠三條編譯期規則管理記憶體
（[Rust Book ch4-01](../../reference/book/src/ch04-01-what-is-ownership.md)）：

1. 每個值都有**唯一一個擁有者**（owner）。
2. 擁有者離開 scope，值被**自動釋放**（呼叫 `drop`）。
3. 賦值或傳參預設是**移動（move）**：所有權轉手，舊變數失效。

```rust
let s1 = String::from("hello");
let s2 = s1;                  // 所有權從 s1「移動」到 s2
// println!("{s1}");          // ❌ 編譯錯誤：value borrowed here after move
```

不想轉移所有權時用**借用（borrow）**——`&T` 是共享引用（可多個、唯讀）、
`&mut T` 是可變引用（同時只能一個）：

```rust
fn len(s: &String) -> usize { s.len() }   // 借走看一眼，不拿走
let s = String::from("hello");
let n = len(&s);
println!("{s} 長度 {n}");                  // ✅ s 還能用
```

「同一時間，要嘛多個唯讀引用、要嘛一個可變引用」這條規則，在單執行緒裡防的是
迭代器失效之類的 bug；**到了多執行緒，它防的就是資料競爭**——這正是本章後半的
主題。先記住一個結論：Rust 的並行安全不是另外一套機制，而是所有權規則的自然
延伸，所以 Rust Book 把它叫做 *fearless concurrency*。

## 1.5 〔Rust 基礎〕用 `thread::spawn` 開執行緒

雖然這套架構的賣點是「你不必自己開執行緒」，但框架內部就是用
`std::thread::spawn` 實作的（[Rust Book ch16-01](../../reference/book/src/ch16-01-threads.md)），
理解它是讀懂第 5 章 worker 的前提：

```rust
use std::thread;

let v = vec![1, 2, 3];

let handle = thread::spawn(move || {        // move：把 v 的所有權搬進 closure
    println!("在新執行緒裡用 v：{v:?}");
});

handle.join().unwrap();                     // 等新執行緒跑完
```

兩個重點：

- **`move` 關鍵字**：強迫 closure 取得捕捉變數的**所有權**而非借用。為什麼必須？
  因為新執行緒可能比目前的函式活得久，借用 stack 上的 `v` 會變懸空引用。拿掉
  `move` 試試，編譯器會說：

  ```text
  error[E0373]: closure may outlive the current function, but it borrows `v`,
                which is owned by the current function
  help: to force the closure to take ownership of `v`, use the `move` keyword
  ```

  連修法都告訴你了。這個「跨執行緒就必須擁有」的原則，到第 2 章會變成 task 型別
  上的 `'static` bound。

- **`join()`**：`spawn` 回傳 `JoinHandle`，`join` 阻塞到該執行緒結束。本 repo 的
  `ThreadGroup::join_all()`（第 5 章）就是對一排 `JoinHandle` 逐一 `join`。

本 repo 全部程式碼裡，`thread::spawn` 只出現在三個地方：`ThreadGroup`（worker）、
`DelayedTaskManager`（計時執行緒）、`IoTaskRunner`（IO 執行緒）。**其餘所有並行
都用 post task 表達**——這就是 Chromium 守則的落實。

## 1.6 〔Rust 教學〕Send 與 Sync：把執行緒規範寫進型別系統

這一節是本章的重頭戲。

### Chromium 怎麼防資料競爭：文件＋執行期檢查

C++ 語言本身不知道「執行緒」是什麼。Chromium 靠的是：

- **命名與文件**：文件的 Threading Lexicon 一節定義了 thread-unsafe /
  thread-affine / thread-safe 等詞彙，要求工程師讀文件、守規矩。
- **執行期檢查**：`SEQUENCE_CHECKER` / `THREAD_CHECKER`
  （[`reference/base/sequence_checker.h`](../../reference/base/sequence_checker.h)）
  ——只在 debug build 生效，寫錯了要等程式跑起來、而且剛好踩到，才會炸。

### Rust 怎麼防：兩個編譯期 marker trait

Rust 把這件事交給兩個 trait
（[Rust Book ch16-04](../../reference/book/src/ch16-04-extensible-concurrency-sync-and-send.md)）：

- **`Send`**：型別的值可以安全地**移動到另一條執行緒**。
- **`Sync`**：型別可以安全地**被多條執行緒同時引用**。精確定義：
  `T: Sync` ⟺ `&T: Send`（「把 `&T` 送去別的執行緒」安全，就代表多執行緒
  可以同時持有 `&T`）。

它們是 **marker trait**——沒有任何方法，純粹是貼在型別上的標籤；而且是
**auto trait**——編譯器自動推導：所有欄位都 `Send`，struct 就 `Send`，以此類推。
你幾乎永遠不會手動實作它們，但你會**不斷地在 trait bound 裡要求它們**。

### 範例一：`Rc` 不能跨執行緒——看編譯器擋下你

`Rc<T>`（引用計數指標，[ch15-04](../../reference/book/src/ch15-04-rc.md)）的計數
器是**非原子**的普通整數，兩條執行緒同時 `clone` 會把計數弄壞。所以標準庫宣告
`Rc<T>: !Send`。試着把它丟進執行緒：

```rust
use std::rc::Rc;
use std::thread;

let data = Rc::new(vec![1, 2, 3]);
let data2 = Rc::clone(&data);

thread::spawn(move || {            // ❌ 編譯失敗
    println!("{data2:?}");
});
```

```text
error[E0277]: `Rc<Vec<i32>>` cannot be sent between threads safely
   = help: within `{closure}`, the trait `Send` is not implemented for `Rc<Vec<i32>>`
note: required by a bound in `spawn`
   |  pub fn spawn<F, T>(f: F) -> JoinHandle<T>
   |  where F: Send + 'static,
```

注意錯誤訊息的最後一段：`thread::spawn` 的簽名要求 `F: Send + 'static`——closure
必須 `Send`，而 closure 自動 `Send` 的條件是**它捕捉的每個變數都 `Send`**。
`Rc` 不是，整個 closure 就不是，編譯失敗。把 `Rc` 換成原子計數的 `Arc`
（**A**tomically **R**eference **C**ounted）就過了：

```rust
use std::sync::Arc;

let data = Arc::new(vec![1, 2, 3]);
let data2 = Arc::clone(&data);
thread::spawn(move || println!("{data2:?}"));   // ✅
```

這就是「Chromium 用文件規範、Rust 用編譯器拒絕」的具體畫面。Chromium 工程師
必須**知道** `scoped_refptr` 的計數是原子的所以可跨執行緒、而某某類別不行；
Rust 程式設計師寫錯直接編不過。

### 範例二：`Send` 但不 `Sync` ——`RefCell` 與 `Mutex` 的差別

`RefCell<T>`（[ch15-05](../../reference/book/src/ch15-05-interior-mutability.md)）
提供「對不可變引用做可變操作」的能力（內部可變性），但它的借用檢查是非原子的，
所以：`RefCell<T>: Send`（整顆搬去別的執行緒沒問題）但 `RefCell<T>: !Sync`
（兩條執行緒同時碰會壞）。

```rust
use std::cell::RefCell;
use std::sync::Arc;
use std::thread;

let shared = Arc::new(RefCell::new(0));
let s2 = Arc::clone(&shared);
thread::spawn(move || { *s2.borrow_mut() += 1; });   // ❌
```

```text
error[E0277]: `RefCell<i32>` cannot be shared between threads safely
   = help: the trait `Sync` is not implemented for `RefCell<i32>`
   = note: required for `Arc<RefCell<i32>>` to implement `Send`
```

最後一行很有教育意義：`Arc<T>` 要 `Send`，前提是 `T: Send + Sync`——因為 `Arc`
的本質就是讓多條執行緒同時持有 `&T`。修法是把 `RefCell` 換成 `Mutex`：

```rust
use std::sync::{Arc, Mutex};

let shared = Arc::new(Mutex::new(0));
let s2 = Arc::clone(&shared);
thread::spawn(move || { *s2.lock().unwrap() += 1; });   // ✅
```

`Mutex<T>` 的魔法在型別層面就是一行：**`T: Send` ⇒ `Mutex<T>: Sync`**。
鎖把「需要外部同步的東西」變成「自帶同步的東西」。

### 常見型別的 Send / Sync 一覽

| 型別 | `Send` | `Sync` | 直覺 |
|---|---|---|---|
| `i32`、`String`、`Vec<T: Send>` | ✅ | ✅ | 普通資料，誰拿都行 |
| `Rc<T>` | ❌ | ❌ | 計數非原子 |
| `Arc<T: Send + Sync>` | ✅ | ✅ | 計數原子 |
| `RefCell<T: Send>` | ✅ | ❌ | 借用檢查非原子 |
| `Mutex<T: Send>` | ✅ | ✅ | 鎖提供同步 |
| `AtomicBool` 等 | ✅ | ✅ | 硬體原子操作 |
| `*const T`（裸指標） | ❌ | ❌ | 編譯器無從擔保 |

### 和 Chromium Threading Lexicon 的漂亮對應

回頭看 Chromium 文件用文字定義的詞彙，會發現它們和 Rust 的 marker trait 幾乎
一一對應：

| Chromium 詞彙（文件定義） | Rust 對應 | 例子 |
|---|---|---|
| **Thread-safe**：可安全地平行存取 | `Sync` | `Mutex<T>`、`AtomicBool` |
| **Thread-unsafe**：存取必須外部同步 | `!Sync`（但可 `Send`） | `RefCell<T>` |
| **Thread-affine**：只能在建立它的執行緒上用 | `!Send` | `Rc<T>` |
| **Immutable**：建構後不可變，隨便讀 | `Sync`（無內部可變性） | `Arc<str>` |

Chromium 要靠 `THREAD_CHECKER` 在執行期抓 thread-affine 類別被亂用；Rust 的
`!Send` 型別**根本過不了 `thread::spawn` 的 bound**。這個對應表是理解「為什麼
這個 port 能比 C++ 原版砍掉一堆檢查設施」的鑰匙。

## 1.7 訊息傳遞 vs 共享狀態：守則 2 的 Rust 版

Chromium 守則 2 說「優先傳訊息，不要共享記憶體」。Rust Book 用兩章講同一件事：
[ch16-02 用 channel 傳訊息](../../reference/book/src/ch16-02-message-passing.md)、
[ch16-03 共享狀態（Mutex）](../../reference/book/src/ch16-03-shared-state.md)，
並引用 Go 的口號：

> Do not communicate by sharing memory; instead, share memory by communicating.

標準庫的 channel 長這樣：

```rust
use std::sync::mpsc;
use std::thread;

let (tx, rx) = mpsc::channel();
thread::spawn(move || {
    tx.send(String::from("hi")).unwrap();   // 值的「所有權」隨訊息轉移！
});
println!("{}", rx.recv().unwrap());
```

注意註解那行：`send` 拿走值的所有權。送出去之後你**用不到它了**（編譯器擋），
所以「兩邊同時碰同一塊資料」根本寫不出來——訊息傳遞的安全性同樣是所有權系統
的推論。

而本系列要講的 **post task 模式，本質上就是訊息傳遞的一種**：你送出去的訊息
不是資料、而是「一段帶着資料的程式碼」（closure），收件方是某個 sequence。
Chromium 文件 "Memory ordering guarantees" 一節保證 post 與執行之間有
happens-before 關係——post 前的所有寫入，task 執行時保證可見。`rust_task` 透過
`Mutex` 與 atomics 提供同樣的保證（第 4、5 章會看到具體機制）。

## 本章小結

- Chromium 的並行架構為「回應性」服務：工作切成 task，丟到 thread pool 或
  sequence 上執行，main / IO thread 永遠保持輕快。
- 三條守則：重活外包、訊息傳遞取代共享記憶體、小心跨執行緒的物件生命週期。
- Rust 把 Chromium 用文件與 DCHECK 維護的執行緒紀律，編進了型別系統：
  - 所有權與借用規則 → 杜絕懸空引用；
  - `Send` / `Sync` → 杜絕資料競爭；
  - Chromium 的 thread-safe / thread-unsafe / thread-affine 詞彙 ≈
    `Sync` / `!Sync` / `!Send`。

## 動手做

1. 把 1.6 的兩個編譯錯誤範例親手打進一個 `cargo new` 專案，讀完整的編譯器輸出
   ——Rust 錯誤訊息的 `help:` 段落常常直接給出修法。
2. 猜猜 `Box<dyn FnOnce()>`（沒有 `+ Send`）能不能丟進 `thread::spawn`？
   寫個五行程式驗證。答案會在第 2 章揭曉原理。

## 延伸閱讀

- Chromium 對「為什麼偏好 sequence 而非 thread」的完整論述：
  `reference/threading_and_tasks.md` 的 *Prefer Sequences to Physical Threads* 一節。
- Rust 如何看待「可組合的並行抽象」：
  [ch16-04](../../reference/book/src/ch16-04-extensible-concurrency-sync-and-send.md)
  結尾——`Send` / `Sync` 是少數「不安全程式碼的正確性由人擔保」的地方，標準庫
  把這層擔保包好，使用者就能安全組合。
