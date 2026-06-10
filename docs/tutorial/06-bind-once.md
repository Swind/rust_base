# 第 6 章 bind_once：物件生命週期與回呼

> 本章對應 Chromium 守則三：「跨 sequence 操作物件時，小心生命週期」。
> Chromium 素材：`reference/threading_and_tasks.md` 的 Cancelling a Task 一節、
> [`reference/base/memory/weak_ptr.h`](../../reference/base/memory/weak_ptr.h)、
> [`reference/base/functional/bind.h`](../../reference/base/functional/bind.h)
> （搜尋 `Unretained` 的註解）。
> Rust 素材：Rust Book [ch15-04 Rc](../../reference/book/src/ch15-04-rc.md)、
> [ch15-06 Weak 與引用循環](../../reference/book/src/ch15-06-reference-cycles.md)、
> [ch10-01 泛型](../../reference/book/src/ch10-01-syntax.md)、
> [ch10-02 trait bound](../../reference/book/src/ch10-02-traits.md)。
> 主角程式碼：[`rust_task/bind.rs`](../../rust_task/bind.rs)（全檔含測試 178 行）。

## 6.1 問題：佇列會把你的物件留多久？

第 2 章說過：`'static` bound 已在編譯期消滅「callback 跑的時候物件已經死了」
（use-after-free）。但它的代價是反方向的問題——

task 被 post 之後、執行之前，closure 捕捉的一切都被佇列**持有**。如果 closure
捕捉了 `Arc<Handler>`，那麼即使整個程式都已不需要這個 handler，它也得活到
task 執行完——**佇列成了隱形的生命週期延長器**。後果輕則資源晚放（檔案、
socket 押在一個不知道何時才跑的 task 手裡），重則配合循環引用直接洩漏。

C++ / Chromium 的對應問題更兇險。binding 物件指標有三種選擇
（`threading_and_tasks.md` Quick start 與 `bind.h` 註解）：

| Chromium 寫法 | 語意 | 風險 |
|---|---|---|
| `base::Unretained(ptr)` | 裸指標，不管生死 | use-after-free，文件明言「avoid」 |
| `scoped_refptr` | 強引用，延長生命 | 延長到你不想要的程度；文件說這「usually the wrong design pattern」 |
| `base::WeakPtr` | 弱引用，死了變 no-op | 推薦預設，但**非執行緒安全** |

`WeakPtr` 的機制：物件持有 `WeakPtrFactory` 成員，callback 綁定
`weak_ptr_factory_.GetWeakPtr()`；物件解構時 factory 失效所有已發出的
weak pointer，**已在佇列裡的 callback 執行時自動變 no-op**。文件範例：

```cpp
class A {
 public:
  void ComputeAndStore() {
    // Compute 在 thread pool 跑；Store 回到本 sequence。
    // 若 A 先死，Store 那一步被自動取消（保證不 use-after-free）。
    base::ThreadPool::PostTaskAndReplyWithResult(
        FROM_HERE, base::BindOnce(&Compute),
        base::BindOnce(&A::Store, weak_ptr_factory_.GetWeakPtr()));
  }
 private:
  base::WeakPtrFactory<A> weak_ptr_factory_{this};
};
```

## 6.2 Rust 的原料：`Arc` 與 `Weak` 深入

Rust 標準庫直接內建了這對概念
（[ch15-06](../../reference/book/src/ch15-06-reference-cycles.md)）。`Arc<T>`
維護兩個計數：

```rust
use std::sync::{Arc, Weak};

let strong: Arc<String> = Arc::new("hi".into());   // 強計數 1，弱計數 0
let weak: Weak<String> = Arc::downgrade(&strong);  // 強 1，弱 1

// 用 Weak 之前必須 upgrade —— 回傳 Option<Arc<T>>
match weak.upgrade() {
    Some(arc) => println!("還活着：{arc}"),         // 暫時強計數 +1
    None      => println!("已經死了"),
}

drop(strong);                       // 強計數歸 0 → String 立刻釋放
assert!(weak.upgrade().is_none());  // Weak 還在，但升級失敗
```

關鍵規則：

- **值的生死只看強計數**。強計數歸零，值立刻解構；弱引用再多也留不住它
  （弱計數只是讓計數器那塊管理記憶體本身晚點回收）。
- `upgrade()` 是唯一的取用路：成功拿到臨時的 `Arc`（保證用的期間活着），
  失敗拿到 `None`。**不存在「拿到引用但物件已死」的狀態**——這是 dangling
  pointer 在 API 形狀上被排除。
- 跟 Chromium `WeakPtr` 的決定性差異：`Weak::upgrade` 是**原子操作、執行緒
  安全**。Chromium 文件警告 `WeakPtr` 的解參考和解構必須在同一 sequence；
  Rust 把這條規範也編譯掉了。

## 6.3 `bind_once`：一個 API 收兩種語意

回到 post task 的場景，選擇題變成二選一：

- closure 捕捉 `Arc<T>` → callback **保證執行**，T 被延命到執行完；
- closure 捕捉 `Weak<T>` → **不延命**；執行時升級失敗就靜默跳過。

[`rust_task/bind.rs`](../../rust_task/bind.rs) 用一個 trait 把兩者統一成
同一個函式：

```rust
/// 把 Arc<T> 和 Weak<T> 抽象成「能解析出 Option<Arc<T>> 的東西」
pub trait IntoArc<T>: Send + 'static {
    fn into_arc(self) -> Option<Arc<T>>;
}

impl<T: Send + Sync + 'static> IntoArc<T> for Arc<T> {
    fn into_arc(self) -> Option<Arc<T>> { Some(self) }        // 永遠成功
}

impl<T: Send + Sync + 'static> IntoArc<T> for Weak<T> {
    fn into_arc(self) -> Option<Arc<T>> { self.upgrade() }    // 可能 None
}

pub fn bind_once<P, T, F>(ptr: P, f: F) -> Box<dyn FnOnce() + Send + 'static>
where
    P: IntoArc<T>,
    T: Send + Sync + 'static,
    F: FnOnce(Arc<T>) + Send + 'static,
{
    Box::new(move || {
        if let Some(arc) = ptr.into_arc() {
            f(arc);
        }
    })
}
```

用法直接對照 Chromium：

```rust
// ≈ BindOnce(&Handler::OnEvent, handler_refptr)：強引用，保證執行
pool.post_task(traits, bind_once(Arc::clone(&handler), |h| h.on_event()));

// ≈ BindOnce(&Handler::OnEvent, weak_factory_.GetWeakPtr())：物件死了就跳過
pool.post_task(traits, bind_once(Arc::downgrade(&handler), |h| h.on_event()));

// 額外引數照常用 closure 捕捉（≈ BindOnce 的後續引數）
let msg = "hello".to_string();
pool.post_task(traits, bind_once(Arc::downgrade(&handler), move |h| h.on_message(msg)));
```

### 〔Rust 教學〕泛型＋trait bound：這 12 行值得逐字讀

`bind_once` 的簽名是一堂濃縮的泛型課
（[ch10-01](../../reference/book/src/ch10-01-syntax.md)）：

```rust
pub fn bind_once<P, T, F>(ptr: P, f: F) -> Box<dyn FnOnce() + Send + 'static>
where
    P: IntoArc<T>,                       // ptr 可以是 Arc<T> 或 Weak<T>
    T: Send + Sync + 'static,            // 被指物件必須能跨執行緒共享
    F: FnOnce(Arc<T>) + Send + 'static,  // callback 收一個保活的 Arc<T>
```

- **三個型別參數**：`P`（指標型別）、`T`（物件型別）、`F`（callback 型別）。
  呼叫時全部由編譯器**推導**，使用者一個尖括號都不用寫。
- **`where` 子句**列出每個參數要滿足的 trait bound——這是 Rust 泛型與 C++
  模板的本質差異：bound 在**定義處**檢查（簽名就是完整契約），不像 C++ 模板
  錯誤在展開處爆出八層 traceback。
- **「用 trait 統一多種輸入」**是標準庫到處在用的 API 手法（`AsRef`、`Into`、
  `IntoIterator`…）。`IntoArc` 是你自己也能寫的迷你版：為 `Arc<T>` 和
  `Weak<T>` 各寫一個 `impl`，語意差異（永遠成功 vs 可能失敗）封裝在各自的
  `into_arc` 裡，`bind_once` 本體完全不用分支。
- **單態化（monomorphization）**：編譯器為每組實際型別生成專屬機器碼，泛型
  零執行期開銷。動態的部分只有回傳值——裝箱成 `Box<dyn FnOnce()>`，因為佇列
  需要統一型別（第 2 章）。「泛型進、trait object 出」是這類 API 的典型剖面。
- 對照 Chromium：`base::BindOnce` 為了同樣的效果需要
  `reference/base/functional/bind_internal.h` 約三千行模板機械，其中專門有
  特化來識別 `WeakPtr` 接收者並插入「死了就跳過」邏輯。Rust 版 12 行。

### `f` 收到的是 `Arc<T>`：執行期間保證活着

細節但重要：callback 的參數是 `Arc<T>` 而非 `&T`。`upgrade` 成功後，這個臨時
`Arc` 把強計數撐高，**整個 callback 執行期間物件保證不死**——就算別的執行緒
此刻 drop 掉最後一個外部強引用也一樣。「檢查活着」與「使用」之間沒有縫隙。

## 6.4 經典陷阱：在「完成信號」上用 `Weak`

強弱選錯是這套 API 的頭號 bug 來源，兩個方向：

- **該弱卻強**：物件被佇列拖着不死。輕則資源晚放，重則洩漏。
- **該強卻弱**：callback 是個「done 信號」，有人**阻塞等待**它 fire——物件
  先死，callback 靜默跳過，等待者**永遠卡死**。死鎖比洩漏難查得多，因為現場
  什麼錯誤都沒有，只有一條不動的執行緒。

第二種的真實案例就在 event_bus 範例裡，原始碼直接把警告寫成註解
（`rust_task/examples/event_bus.rs:95`）：

```rust
// flush 故意「不」用 bind_once：done callback 必須永遠 fire，
// 否則呼叫端的 Barrier 會永遠等不到。
fn flush(&self, done: impl FnOnce() + Send + 'static) {
    self.runner.post_task(Box::new(done));
}
```

呼叫端長這樣（`wait_flush`）：post 一個「敲 barrier」的 task 到 sequence 尾端、
然後 `barrier.wait()` 等它——這是「等 sequence 排空」的標準技巧。若 `flush` 用
`bind_once(Arc::downgrade(self), ...)`，bus 在 flush 前被 drop，task 變 no-op，
`barrier.wait()` 永遠等不到第二個人。

**判斷準則一句話：「這個 callback 不執行的話，有沒有人會等到天荒地老？」**
有 → `Arc` 或普通 closure；沒有、且 callback 只是操作物件自身 → `Weak`。

## 6.5 `bind_repeating` 與 `self: &Arc<Self>`

多次呼叫版（給第 7 章的計時器用）：

```rust
pub fn bind_repeating<T, F>(weak: Weak<T>, f: F) -> Arc<dyn Fn() + Send + Sync + 'static>
where
    T: Send + Sync + 'static,
    F: Fn(Arc<T>) + Send + Sync + 'static,
{
    Arc::new(move || {
        if let Some(arc) = weak.upgrade() {
            f(arc);
        }
    })
}
```

兩個刻意的不同：trait 從 `FnOnce` 換成 `Fn`（要反覆呼叫）；**只收 `Weak`**，
不像 `bind_once` 強弱皆可——一個會被無限次呼叫又持強引用的 closure 幾乎必然
是生命週期 bug，API 直接不給這個選項。「**把錯誤用法做成型別上不可表達**」
（make invalid states unrepresentable）是 Rust API 設計的核心信條，這對函式
是極小而完整的示範。

最後補一個 event_bus 用到的進階語法。`subscribe` 的接收者寫作：

```rust
fn subscribe(self: &Arc<Self>, cb: impl Fn(&E) + Send + Sync + 'static) -> u64 {
    let id = self.next_id.fetch_add(1, Ordering::Relaxed);
    self.runner.post_task(bind_once(Arc::downgrade(self), move |bus| {
        bus.state.lock().unwrap().subscribers.push((id, Arc::new(cb)));
    }));
    id
}
```

〔Rust 教學〕`self: &Arc<Self>` 是顯式指定接收者型別：方法內部需要
`Arc::downgrade(self)`（從 `&Arc<Self>` 造 `Weak`），普通的 `&self` 給不出
`Arc` 本體。呼叫端毫無感覺——只要 bus 存在於 `Arc` 裡，照常寫
`bus.subscribe(...)`。對應 Chromium 裡「類別內部呼叫
`weak_ptr_factory_.GetWeakPtr()`」的慣用法，差別是 Rust 不需要在物件裡藏一個
factory 成員。

## 本章小結

| 情境 | Chromium | 本 repo |
|---|---|---|
| callback 必須執行（完成信號） | 強引用 / 同步機制 | `bind_once(Arc::clone(&x), f)` 或普通 closure |
| callback 跟着物件生死 | `WeakPtr` | `bind_once(Arc::downgrade(&x), f)` |
| 重複 callback（timer 等） | `BindRepeating` + `WeakPtr` | `bind_repeating(weak, f)`（只收 Weak） |
| 裸指標賭物件活着 | `base::Unretained`（勸退） | **寫不出來**（`'static` bound） |

- Rust 概念入帳：`Arc`/`Weak` 雙計數模型、`upgrade()` 的 `Option` 形狀、
  泛型三參數＋`where` 子句、用 trait 統一輸入型別、單態化、
  `self: &Arc<Self>`、make-invalid-states-unrepresentable。

## 動手做

1. 跑 `cargo test -p rust_task bind`——`bind.rs` 的單元測試本身就是規格書：
  `arc_always_runs`、`weak_skips_after_drop`、`weak_does_not_extend_lifetime`，
  一個測試一條語意。
2. 親手製造 6.4 的 hang：把 event_bus 的 `flush` 改成
   `bind_once(Arc::downgrade(self), ...)`（需要把簽名改成 `self: &Arc<Self>`），
   在 `wait_flush` 之前 `drop(bus)`，跑起來感受「沒有錯誤訊息的卡死」。
3. 給 `IntoArc` 加第三個實作：`impl IntoArc<T> for Option<Arc<T>>`（`None` 視
   同升級失敗）。想想這會不會破壞既有呼叫端（提示：不會——新增 impl 是
   非破壞性擴充，這正是 trait 開放性的好處）。

## 延伸閱讀

- `reference/base/memory/weak_ptr.h` 開頭 60 行的註解：`WeakPtr` 的完整契約，
  特別是「must be dereferenced and invalidated on the same SequencedTaskRunner」
  ——然後回想 Rust 的 `Weak` 為什麼不需要這條。
- 第 4.5(c) 節（`Weak` 斷循環引用）＋本章＝`Weak` 的兩大用途：打破所有權環、
  「不延命」的回呼。Rust Book [ch15-06](../../reference/book/src/ch15-06-reference-cycles.md)
  的 tree 例子是第三個經典場景（父子互指）。
