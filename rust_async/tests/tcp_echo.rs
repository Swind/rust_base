//! Integration test mirroring the `tcp_echo` example: proves the epoll→Waker
//! wiring drives a real `connect/write/read` to completion via `block_on`, and
//! that `spawn` runs futures on the thread pool.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use rust_async::{Async, TcpListener as AsyncTcpListener, UdpSocket, block_on, spawn};

#[test]
fn block_on_connect_write_read() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 64];
        let n = sock.read(&mut buf).unwrap();
        sock.write_all(&buf[..n]).unwrap();
    });

    let echoed = block_on(async move {
        let conn = Async::connect(addr).await.unwrap();
        conn.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });

    assert_eq!(echoed, "ping");
    server.join().unwrap();
}

#[test]
fn spawn_runs_futures_on_pool() {
    let sum = block_on(async {
        let a = spawn(async { 20 });
        let b = spawn(async { 22 });
        a.await + b.await
    });
    assert_eq!(sum, 42);
}

/// Proves `Async` works with the `futures` ecosystem via its `futures_io`
/// trait impls — driven here by futures-lite's `AsyncReadExt`/`AsyncWriteExt`
/// combinators rather than our inherent methods.
#[test]
fn futures_io_combinators_interop() {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 64];
        let n = sock.read(&mut buf).unwrap();
        sock.write_all(&buf[..n]).unwrap();
    });

    let echoed = block_on(async move {
        let mut conn = Async::connect(addr).await.unwrap();
        conn.write_all(b"via-futures-io").await.unwrap(); // AsyncWriteExt
        conn.close().await.unwrap(); // half-close so the read below hits EOF
        let mut out = Vec::new();
        conn.read_to_end(&mut out).await.unwrap(); // AsyncReadExt
        out
    });

    assert_eq!(echoed, b"via-futures-io");
    server.join().unwrap();
}

/// Both ends on the runtime: an `async` accept loop (spawned on the pool)
/// echoes for a client driven by `block_on`.
#[test]
fn tcp_listener_accept() {
    let echoed = block_on(async {
        let listener = AsyncTcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let server = spawn(async move {
            let (conn, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).await.unwrap();
            conn.write_all(&buf[..n]).await.unwrap();
        });

        let conn = Async::connect(addr).await.unwrap();
        conn.write_all(b"listener").await.unwrap();
        let mut buf = [0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        server.await;
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });
    assert_eq!(echoed, "listener");
}

/// IPv6 loopback connect + echo, proving the V6 path in `start_connect`.
#[test]
fn ipv6_connect_echo() {
    let listener = AsyncTcpListener::bind("[::1]:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();

    let echoed = block_on(async move {
        let server = spawn(async move {
            let (conn, _peer) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).await.unwrap();
            conn.write_all(&buf[..n]).await.unwrap();
        });
        let conn = Async::connect(addr).await.unwrap();
        conn.write_all(b"v6").await.unwrap();
        let mut buf = [0u8; 64];
        let n = conn.read(&mut buf).await.unwrap();
        server.await;
        String::from_utf8_lossy(&buf[..n]).into_owned()
    });
    assert_eq!(echoed, "v6");
}

/// A cloned stream used for reading while the original writes — exercises the
/// reactor's read+write-at-once path on a single fd.
#[test]
fn cloned_stream_concurrent_read_write() {
    let listener = AsyncTcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();

    let got = block_on(async move {
        let server = spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = conn.read(&mut buf).await.unwrap();
            conn.write_all(&buf[..n]).await.unwrap();
        });

        let writer = Async::connect(addr).await.unwrap();
        let reader = writer.clone(); // shares fd + reactor registration

        // Reader parks on readability while the writer sends on the same fd.
        let read_task = spawn(async move {
            let mut buf = [0u8; 64];
            let n = reader.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });

        writer.write_all(b"duplex").await.unwrap();
        let out = read_task.await;
        server.await;
        out
    });

    assert_eq!(got, b"duplex");
}

/// UDP datagram round-trip via `send_to`/`recv_from`.
#[test]
fn udp_send_recv() {
    let got = block_on(async {
        let server = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        client.send_to(b"datagram", server_addr).await.unwrap();

        let mut buf = [0u8; 64];
        let (n, from) = server.recv_from(&mut buf).await.unwrap();
        server.send_to(&buf[..n], from).await.unwrap();

        let mut rbuf = [0u8; 64];
        let (rn, _) = client.recv_from(&mut rbuf).await.unwrap();
        rbuf[..rn].to_vec()
    });

    assert_eq!(got, b"datagram");
}
