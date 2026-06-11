# 第 2 章 Task：一個可被攜帶的工作單元

> 本章 Chromium 素材：[`threading_and_tasks.md`](https://chromium.googlesource.com/chromium/src/+/main/docs/threading_and_tasks.md) 的 Tasks 一節、
> [`base/functional/callback.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/functional/callback.h)、
> [`base/functional/bind.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/functional/bind.h)、
> [`base/task/thread_pool/task.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task.h)。
> 本章 Rust 素材：Rust Book [ch13-01 closure](https://rust-lang.tw/book-tw/ch13-01-closures.html)、
> [ch15-01 Box](https://rust-lang.tw/book-tw/ch15-01-box.html)、
> [ch17-02 trait object](https://rust-lang.tw/book-tw/ch17-02-trait-objects.html)、
> [ch10-03 生命週期](https://rust-lang.tw/book-tw/ch10-03-lifetime-syntax.html)。
> 本章主角程式碼：[`rust_task/task.rs`](../../rust_task/task.rs)（全檔 30 行）。

## 2.1 Chromium：`base::OnceClosure` 與 `base::BindOnce`

Chromium 文件對 task 的定義：

> A task is a `base::OnceClosure` added to a queue for asynchronous execution.
> A `base::OnceClosure` stores a function pointer and arguments. It has a
> `Run()` method that invokes the function pointer using the bound arguments.

```cpp
void TaskA() {}
void TaskB(int v) {}
auto task_a = base::BindOnce(&TaskA);
auto task_b = base::BindOnce(&TaskB, 42);   // 42 被存進 closure
```

`BindOnce` 把「函式」和「部分引數」綁成一個可攜帶的物件——函數式語言叫
partial application。「Once」的意思是這個 callback **最多被呼叫一次**，呼叫時
消耗掉綁定的引數（所以可以綁 move-only 型別，例如 `std::unique_ptr`）。

C++ 需要 `base/functional/bind.h` 裡上千行的模板機械來實作這件事。Rust 不需要
——因為語言原生就有「帶狀態、可表達只能呼叫一次」的 closure。

## 2.2 〔Rust 基礎〕closure 與它的三種捕捉方式

closure 是能**捕捉所在環境變數**的匿名函式
（[ch13-01](https://rust-lang.tw/book-tw/ch13-01-closures.html)）：

```rust
let x = 4;
let equal_to_x = |z| z == x;   // 捕捉了 x
assert!(equal_to_x(4));
```

closure 捕捉變數有三種方式，編譯器會自動選**最寬鬆夠用**的那種：

```rust
let s = String::from("hi");

// 1. 不可變借用（&s）——只是讀
let print_it = || println!("{s}");

// 2. 可變借用（&mut s）——要改
let mut s = String::from("hi");
let mut append = || s.push('!');

// 3. 取得所有權（move）——要把值帶走，或 closure 要活得比 s 久
let s = String::from("hi");
let own_it = move || println!("{s}");
println!("{s}");   // ❌ s 已被 move 進 closure
```

第 1 章說過：**跨執行緒的 closure 必須用 `move`**，因為借用 stack 上的變數會
變懸空引用。本系列所有 post 出去的 task 都是 move closure。

### `FnOnce` / `FnMut` / `Fn`：closure 的三個 trait

每個 closure 都是編譯器生成的**獨一無二的匿名型別**，它們透過實作這三個 trait
之一（或多個）來「可被呼叫」：

| trait | 呼叫時拿 self 的方式 | 能呼叫幾次 | 典型場景 | Chromium 對應 |
|---|---|---|---|---|
| `FnOnce` | `self`（消耗自己） | **一次** | 把捕捉的值 move 出去 | `OnceCallback` |
| `FnMut` | `&mut self` | 多次 | 累加器、狀態機 | — |
| `Fn` | `&self` | 多次（可並發） | 純讀取 | `RepeatingCallback` |

三者是繼承關係：`Fn: FnMut: FnOnce`——能多次呼叫的當然也能呼叫一次。所以
「task 佇列」這種**只保證呼叫一次**的場合，用最寬的 `FnOnce` 當 bound，能收下
所有 closure。

對照看 Chromium 的兩種 callback，對應關係非常整齊：

```cpp
// C++                                     // Rust
base::OnceCallback<void()>                 Box<dyn FnOnce() + Send>
base::RepeatingCallback<void()>            Arc<dyn Fn() + Send + Sync>
std::move(once_cb).Run()                   (once_cb)()      // 呼叫即消耗
repeating_cb.Run(); repeating_cb.Run();    repeating_cb(); repeating_cb();
```

C++ 要靠 `std::move(...).Run()` 的**約定**表達「用過即丟」，忘了 move 只會
warning；Rust 的 `FnOnce` 呼叫第二次**直接編譯失敗**：

```rust
let s = String::from("hi");
let f = move || drop(s);   // 把 s move 出去 → 這個 closure 只實作 FnOnce
f();
f();   // ❌ error[E0382]: use of moved value: `f`
```

### `BindOnce(&TaskB, 42)` 的 Rust 寫法

不需要任何函式庫，move closure 就是 partial application：

```rust
fn task_b(v: i32) { println!("{v}"); }

let v = 42;
let task = move || task_b(v);   // ≈ base::BindOnce(&TaskB, 42)
```

要綁定「物件＋方法」（C++ 的 `BindOnce(&A::Method, ptr)`）情況複雜一點——涉及
物件的生命週期管理，那是第 6 章 `bind_once` 的主題。本章先處理「自由函式＋引數」。

## 2.3 拆解 task 型別：`Box<dyn FnOnce() + Send + 'static>`

本 repo 的 task 佇列存的型別是 `Box<dyn FnOnce() + Send + 'static>`。四個成分
逐一拆解——每個都對應一個 Rust 核心概念。

### `dyn FnOnce()`：trait object，存「任意」closure 的唯一辦法

佇列要能裝下**各式各樣**的 closure，但每個 closure 型別都不同、大小也不同
（捕捉了 `i32` 的 closure 是 4 bytes，捕捉了三個 `String` 的是 72 bytes）。
泛型（`Vec<F: FnOnce()>`）行不通——一個 `Vec` 只能放一種 `F`。

解法是 **trait object**（[ch17-02](https://rust-lang.tw/book-tw/ch17-02-trait-objects.html)）：
`dyn FnOnce()` 的意思是「某個實作了 `FnOnce()` 的型別，具體是誰我不管」。代價
是動態分派——呼叫透過 vtable 間接跳轉，和 C++ 虛擬函式一樣。Chromium 的
`OnceCallback` 內部也是同樣的 type-erasure 技術（見 `callback.h` 的
`BindStateBase`），只是 C++ 要手工搭建，Rust 是語言內建。

### `Box<...>`：把大小不定的東西放上 heap

trait object 在編譯期大小未知（unsized），不能直接當區域變數或塞進 `VecDeque`
——容器的每個元素必須等大。`Box<T>`（[ch15-01](https://rust-lang.tw/book-tw/ch15-01-box.html)）
把值放到 heap、自己只是一個指標（trait object 的 `Box` 是兩個字寬：資料指標＋
vtable 指標），大小固定，問題解決。

`Box` 同時表達**所有權**：佇列擁有這個 task；task 被執行（消耗）或被丟棄時，
`Box` 連同捕捉的一切自動釋放。沒有 GC、沒有手動 free。

### `+ Send`：這個 closure 會被送去別的執行緒

第 1 章的主角。task 會被**任意 worker thread** 撿去執行，所以 closure 必須
`Send`——而 closure 自動 `Send` 的條件是捕捉的每個變數都 `Send`。於是：

```rust
let rc = std::rc::Rc::new(5);
pool.post_task(TaskTraits::default(), Box::new(move || {
    println!("{rc}");      // ❌ Rc 不是 Send → 整個 closure 不是 Send → 編譯失敗
}));
```

Chromium 要靠 code review 抓「這個 callback 捕捉了執行緒不安全的東西」；
這裡是型別簽名自動把關。

### `+ 'static`：不准借用任何活不夠久的東西

`'static` 是生命週期（lifetime，[ch10-03](https://rust-lang.tw/book-tw/ch10-03-lifetime-syntax.html)）
的一種：「活到程式結束都有效」。對 closure 而言，`F: 'static` 的意思是
**closure 不持有任何短於 `'static` 的借用**——捕捉的東西要嘛是擁有的值（move
進來的），要嘛是 `'static` 引用（如字串字面量）。

為什麼必須？task 進佇列後**何時執行、是否執行都不知道**。如果它借用了呼叫端
stack 上的變數，等它執行時那個 stack frame 早就沒了：

```rust
fn post_bad(pool: &ThreadPool) {
    let local = String::from("short-lived");
    pool.post_task(TaskTraits::default(), Box::new(|| {
        println!("{local}");   // ❌ closure may outlive the current function,
    }));                       //    but it borrows `local`
}   // local 在這裡就死了，task 可能還沒跑
```

加 `move` 之後，`local` 的所有權搬進 closure，活多久由 closure 決定——編譯通過，
且絕對安全。

**重要的觀念對比**：C++ 世界 post callback 的頭號殺手是「callback 執行時，它
引用的物件已經死了」（use-after-free）。`'static` bound 把這一**整類** bug 在
編譯期消滅。剩下的是反過來的問題——「物件被 closure 拖着，活得比你想要的久」
——那是第 6 章 `bind_once` 用 `Weak` 解決的事。先死 vs 不死，Rust 把危險的那半
砍掉，把無害但惱人的那半留給你設計。

## 2.4 `Task` struct：加上元資料

`rust_task/task.rs` 全文如下：

```rust
use std::time::Instant;

pub struct Task {
    pub callback: Box<dyn FnOnce() + Send + 'static>,
    pub posted_from: &'static std::panic::Location<'static>,
    pub delayed_run_time: Option<Instant>,
    pub sequence_num: u64,
}

impl Task {
    #[track_caller]
    pub fn new(callback: Box<dyn FnOnce() + Send + 'static>) -> Self {
        Self {
            callback,
            posted_from: std::panic::Location::caller(),
            delayed_run_time: None,
            sequence_num: 0,
        }
    }
    // new_delayed(...) 同理，多填 delayed_run_time
}
```

對應 Chromium 的
[`base/task/thread_pool/task.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/task/thread_pool/task.h)
——那邊的欄位是 `posted_from`、`delayed_run_time`、`sequence_num`，一模一樣。
逐欄看：

- **`posted_from`** — 「這個 task 是從哪一行 post 的」，除錯時救命。Chromium
  要求每個 `PostTask` 呼叫端手寫 `FROM_HERE` 巨集；Rust 用 **`#[track_caller]`**
  屬性：標了它的函式裡呼叫 `std::panic::Location::caller()`，拿到的是
  **呼叫端**的檔案、行號——呼叫者一個字都不用寫。
- **`delayed_run_time: Option<Instant>`** —— 立即任務是 `None`，延遲任務是
  `Some(到期時刻)`。

  〔Rust 基礎〕`Option<T>`（[ch6-01](https://rust-lang.tw/book-tw/ch06-01-defining-an-enum.html)）
  是 Rust 沒有 null 的替身：值要嘛是 `Some(t)` 要嘛是 `None`，**用之前必須
  模式匹配解開**，編譯器保證你處理過「沒有值」的情況。C++ 慣用的
  `base::TimeTicks` 零值或哨兵值，在 Rust 一律是 `Option`。
- **`sequence_num`** —— 同一個 sequence 內遞增的序號，讓「同時到期」的延遲任務
  有決定性的先後（第 4 章用到）。

也注意 `Instant` 對應 Chromium 的 `base::TimeTicks`（單調時鐘，不受系統調時間
影響）；`Duration` 對應 `base::TimeDelta`。

## 2.5 回收第 1 章的伏筆

第 1 章動手做第 2 題問：`Box<dyn FnOnce()>`（**沒有** `+ Send`）能不能丟進
`thread::spawn`？答案是不能：

```text
error[E0277]: `dyn FnOnce()` cannot be sent between threads safely
```

trait object 的 auto trait **不會自動穿透**——`dyn FnOnce()` 這個型別本身沒
承諾 `Send`，就算裡面包的具體 closure 其實是 `Send` 也一樣，因為編譯器在
trait object 上只看得到你寫出來的 bound。所以要在型別上**顯式寫出**
`dyn FnOnce() + Send`。這就是為什麼 task 型別的 `+ Send + 'static` 一個字都
省不得。

## 本章小結

| Chromium | Rust | 章節 |
|---|---|---|
| `base::OnceClosure` | `Box<dyn FnOnce() + Send + 'static>` | 2.3 |
| `base::BindOnce(&F, args...)` | `move` closure | 2.2 |
| `base::RepeatingCallback` | `Arc<dyn Fn() + Send + Sync>` | 2.2 |
| 「用過即丟」靠 `std::move` 約定 | `FnOnce` 編譯期強制 | 2.2 |
| `FROM_HERE` | `#[track_caller]` + `Location::caller()` | 2.4 |
| use-after-free 風險 | 被 `'static` bound 編譯期消滅 | 2.3 |

## 動手做

1. 寫三個 closure 分別只滿足 `Fn` / `FnMut` / `FnOnce`，用一個泛型函式
   `fn call_twice<F: FnMut()>(mut f: F) { f(); f(); }` 測試哪些能傳入、
   錯誤訊息各說什麼。
2. 把 `Task::new` 的 `#[track_caller]` 拿掉再跑 `cargo test -p rust_task`，
   觀察 `posted_from` 變成哪一行（提示：會變成 `task.rs` 自己——`caller()` 退化
   為「`new` 函式內部」）。

## 延伸閱讀

- Chromium callback 系統的完整文件：[`base/functional/callback.h`](https://source.chromium.org/chromium/chromium/src/+/main:base/functional/callback.h)
  開頭的大段註解（C++ 那邊為了達到 Rust closure 的效果做了多少工程，值得一看）。
- closure 捕捉的底層展開（編譯器把 closure 變成匿名 struct）：
  [ch13-01](https://rust-lang.tw/book-tw/ch13-01-closures.html) 後半。
