//! End-to-end proof of the spike: `block_on(connect().await + write().await +
//! read().await)` against a tiny blocking echo server, plus a `spawn` demo.
//!
//! Run with: `cargo run -p rust_async --example async_tcp_echo`

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use rust_async::{Async, block_on, spawn};

fn main() {
    // A throwaway blocking echo server on an OS-assigned port.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 64];
        let n = sock.read(&mut buf).unwrap();
        sock.write_all(&buf[..n]).unwrap();
    });

    // Everything below runs on the async runtime built on rust_task + rust_io.
    let echoed = block_on(async move {
        let conn = Async::connect(addr).await.unwrap();
        println!("connected to {addr}");

        conn.write_all(b"hello rust_async").await.unwrap();
        println!("sent request");

        let mut buf = [0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });
    println!("echo: {echoed}");
    assert_eq!(echoed, "hello rust_async");

    // The executor path: spawn a future, await its JoinHandle.
    let sum = block_on(async {
        let a = spawn(async { 20 });
        let b = spawn(async { 22 });
        a.await + b.await
    });
    println!("spawned sum: {sum}");
    assert_eq!(sum, 42);

    println!("OK");
}
