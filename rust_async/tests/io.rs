//! Buffering / adapters re-exported from `futures-util` under `rust_async::io`,
//! exercised against our own `Async` stream and in-memory `Cursor`.

use std::io::Write;
use std::net::TcpListener;
use std::thread;

use rust_async::block_on;
use rust_async::io::{AsyncBufReadExt, AsyncReadExt, BufReader, Cursor, copy};
use rust_async::stream::StreamExt;

#[test]
fn buf_reader_reads_lines_from_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.write_all(b"first\nsecond\n").unwrap();
    });

    let lines = block_on(async move {
        let conn = rust_async::Async::connect(addr).await.unwrap();
        let reader = BufReader::new(conn);
        let mut out = Vec::new();
        let mut lines = reader.lines();
        while let Some(line) = lines.next().await {
            out.push(line.unwrap());
        }
        out
    });

    assert_eq!(lines, vec!["first".to_string(), "second".to_string()]);
    server.join().unwrap();
}

#[test]
fn copy_between_cursor_and_vec() {
    let copied = block_on(async {
        let mut src = Cursor::new(b"hello world".to_vec());
        let mut dst: Vec<u8> = Vec::new();
        let n = copy(&mut src, &mut dst).await.unwrap();
        (n, dst)
    });
    assert_eq!(copied.0, 11);
    assert_eq!(copied.1, b"hello world");
}

#[test]
fn read_to_end_via_ext_trait() {
    let data = block_on(async {
        let mut src = Cursor::new(b"abc".to_vec());
        let mut buf = Vec::new();
        src.read_to_end(&mut buf).await.unwrap();
        buf
    });
    assert_eq!(data, b"abc");
}
