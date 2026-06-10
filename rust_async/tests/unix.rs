//! Async Unix-domain sockets over the same reactor readiness path as TCP/UDP.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use rust_async::block_on;
use rust_async::os::unix::net::{UnixDatagram, UnixListener, UnixStream};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_sock() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("rust_async_unix_{}_{}.sock", std::process::id(), n))
}

#[test]
fn stream_pair_round_trip() {
    block_on(async {
        let (a, b) = UnixStream::pair().unwrap();
        a.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        let n = b.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
    });
}

#[test]
fn listener_accept_and_incoming() {
    let path = temp_sock();
    std::fs::remove_file(&path).ok();

    let listener = UnixListener::bind(&path).unwrap();
    let client_path = path.clone();
    let client = thread::spawn(move || {
        let mut sock = std::os::unix::net::UnixStream::connect(&client_path).unwrap();
        use std::io::Write;
        sock.write_all(b"hello unix").unwrap();
    });

    let got = block_on(async move {
        let (conn, _addr) = listener.accept().await.unwrap();
        assert!(conn.local_addr().is_ok());
        let mut buf = [0u8; 16];
        let n = conn.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    });

    assert_eq!(got, b"hello unix");
    client.join().unwrap();
    std::fs::remove_file(&path).ok();
}

#[test]
fn datagram_send_recv() {
    block_on(async {
        let (a, b) = UnixDatagram::pair().unwrap();
        a.send(b"datagram").await.unwrap();
        let mut buf = [0u8; 16];
        let n = b.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"datagram");
    });
}

#[test]
fn connect_to_listener() {
    let path = temp_sock();
    std::fs::remove_file(&path).ok();

    let listener = UnixListener::bind(&path).unwrap();
    let server = thread::spawn(move || {
        block_on(async move {
            let (conn, _) = listener.accept().await.unwrap();
            conn.write_all(b"ok").await.unwrap();
        });
    });

    let connect_path = path.clone();
    let got = block_on(async move {
        let conn = UnixStream::connect(&connect_path).await.unwrap();
        let mut buf = [0u8; 2];
        let n = conn.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    });

    assert_eq!(got, b"ok");
    server.join().unwrap();
    std::fs::remove_file(&path).ok();
}
