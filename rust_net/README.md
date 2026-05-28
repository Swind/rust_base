# rust_net

Async TCP networking built on top of `rust_io`. Requires Linux (epoll).

## SocketPosix

Mirrors Chromium's `net::SocketPosix`: wraps a non-blocking fd and exposes callback-based operations. All methods that touch epoll **must be called from the IO thread**.

`IoTaskRunner` holds only `Weak` references to watchers — keep the `SocketPosix` alive externally (e.g. in a struct or outside the `post_task` closure) until all callbacks fire.

### Client

```rust
use rust_net::SocketPosix;
use rust_io::IoTaskRunner;

let socket = SocketPosix::new();     // keep alive outside the closure
let srv    = Arc::clone(&socket);

io.post_task(Box::new(move || {
    srv.open(&addr).unwrap();

    let s = Arc::clone(&srv);
    srv.connect(addr, move |result| {
        result.unwrap();
        let s2 = Arc::clone(&s);
        s.read(4096, move |result| {
            println!("received: {:?}", result.unwrap());
        });
        s2.write(b"hello".to_vec(), |_| {});
    });
}));
```

### Server

```rust
let server = SocketPosix::new();     // keep alive outside the closure
let srv    = Arc::clone(&server);

io.post_task(Box::new(move || {
    srv.open(&addr).unwrap();
    srv.bind(addr).unwrap();
    srv.listen(128).unwrap();

    srv.accept(move |result| {
        let client = result.unwrap();
        client.write(b"hello".to_vec(), |_| {});
        // call srv.accept() again here to keep accepting
    });
}));
```

### Operations

| Method | Description |
|--------|-------------|
| `open(addr)` | Create non-blocking socket fd |
| `connect(addr, cb)` | Async connect; callback fires on completion |
| `read(len, cb)` | Read up to `len` bytes; immediate or epoll-backed |
| `read_if_ready(cb)` | Notify when readable; caller does the actual read |
| `write(buf, cb)` | Write buf; immediate or epoll-backed |
| `bind(addr)` | Bind to address (sets `SO_REUSEADDR`) |
| `listen(backlog)` | Start accepting connections |
| `accept(cb)` | Accept one connection; one-shot, call again to continue |

## Examples

```bash
cargo run --example socket_posix    # connect+write+read, ReadIfReady, streaming
```
