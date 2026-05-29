# rust_net

Async TCP networking built on top of `rust_io`. Requires Linux (epoll).

Mirrors the TCP slice of Chromium's `net/` socket stack, layered the same way.
Application code normally uses the top two; the lower layers are there when you
need finer control.

```
SocketPosix        raw non-blocking fd + epoll-driven connect/read/write/accept
   ↑
TcpSocket          adds TCP socket options (SO_REUSEADDR, TCP_NODELAY)
   ↑
TcpClientSocket    open + client defaults + connect — the connected-stream handle
TcpServerSocket    open + server defaults + bind + listen — accept → TcpClientSocket
```

| Type | Chromium | Use it when |
|------|----------|-------------|
| `TcpClientSocket` | `net::TCPClientSocket` | Connect out and read/write a stream |
| `TcpServerSocket` | `net::TCPServerSocket` | Listen + accept connections |
| `TcpSocket` | `net::TCPSocket` | Set TCP options yourself / build a custom flow |
| `SocketPosix` | `net::SocketPosix` | The raw fd primitive (rarely needed) |

All methods that touch epoll **must be called from the IO thread**, and the
socket must be kept alive until its callbacks fire — `IoTaskRunner` holds only
`Weak` references to watchers.

> Naming follows Rust convention (`Tcp`, not `TCP`): acronyms are treated as one
> word, which clippy's `upper_case_acronyms` lint enforces.

## Client

`connect` opens the fd, applies client defaults (`TCP_NODELAY`), and connects.

```rust
use rust_net::TcpClientSocket;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;
use std::sync::Arc;

let io     = IoTaskRunner::new();
let client = Arc::new(TcpClientSocket::new());   // keep alive outside the closure
let c      = Arc::clone(&client);

io.post_task(Box::new(move || {
    let c2 = Arc::clone(&c);
    c.connect(addr, move |result| {
        result.unwrap();
        c2.write(b"hello".to_vec(), |_| {});
        c2.read(4096, |r| println!("received: {:?}", r.unwrap()));
    });
}));
```

## Server

`listen` opens the fd, sets `SO_REUSEADDR`, binds, and listens. Bind to `addr:0`
and read `local_addr()` for the kernel-assigned port. `accept` is one-shot; each
peer arrives as a connected `TcpClientSocket`.

```rust
use rust_net::TcpServerSocket;
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;
use std::sync::Arc;

let io     = IoTaskRunner::new();
let server = Arc::new(TcpServerSocket::new());   // keep alive outside the closure
let s      = Arc::clone(&server);

io.post_task(Box::new(move || {
    s.listen("127.0.0.1:0".parse().unwrap(), 128).unwrap();
    let addr = s.local_addr().unwrap();

    s.accept(move |result| {
        let peer = result.unwrap();              // TcpClientSocket
        peer.write(b"hello".to_vec(), |_| {});
        // call accept() again here to keep accepting
    });
}));
```

## Operations

`TcpClientSocket`: `connect(addr, cb)`, `read(len, cb)`, `read_if_ready(cb)`,
`write(buf, cb)`, `local_addr()`, `disconnect()`,
`from_connected(TcpSocket)`.

`TcpServerSocket`: `listen(addr, backlog)`, `accept(cb)`, `local_addr()`.

`TcpSocket`: the above plus the option setters `set_default_options_for_server()`,
`set_default_options_for_client()`, `set_reuse_addr(bool)`, `set_no_delay(bool)`.

`SocketPosix` (low-level): `open` / `connect` / `read` / `read_if_ready` /
`write` / `bind` / `listen` / `accept` / `local_addr` / `close`. `bind` here is
the bare `bind(2)` — TCP options live in `TcpSocket`.

### read vs. read_if_ready

- `read(len, cb)` — the socket owns the buffer and delivers the bytes.
- `read_if_ready(cb)` — the socket signals readability; the caller does the read.
  The per-operation (non-persistent fd watch) pattern from `rust_io`.

## Examples

```bash
cargo run --example tcp_echo        # TcpServerSocket + TcpClientSocket echo, one IO thread
cargo run --example socket_posix    # low-level SocketPosix: connect+write+read, ReadIfReady, streaming
```
