//! End-to-end test of an HTTP server on a single fused lane: many concurrent
//! connections, one I/O thread.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use futures_lite::AsyncWriteExt; // for `.close()`; read/write are inherent on `Async`
use rust_async::net::{Async, TcpListener};
use rust_async::{Runtime, block_on, sleep, spawn};
use rust_io::IoTaskRunner;

const N_CLIENTS: usize = 64;
const HANDLER_DELAY: Duration = Duration::from_millis(20);

/// Read one request's headers, wait `HANDLER_DELAY` (an async sleep on the
/// lane), then reply. The delay is the crux of the test: if the lane could not
/// multiplex, the delays would serialize.
async fn handle_conn(mut stream: Async) -> io::Result<()> {
    let mut buf = vec![0u8; 4096];
    let mut len = 0;
    loop {
        if buf[..len].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        let n = stream.read(&mut buf[len..]).await?;
        if n == 0 {
            return Ok(());
        }
        len += n;
    }

    sleep(HANDLER_DELAY).await;

    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nHELLO";
    stream.write_all(resp.as_bytes()).await?;
    stream.close().await
}

#[test]
fn single_lane_serves_many_concurrent_connections() {
    // Bind on the test thread so the listening socket exists (and we know its
    // port) before the server loop starts.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    // The entire server runs on one fused lane (executor == reactor).
    thread::spawn(move || {
        let io = IoTaskRunner::new();
        let rt = Runtime::new(io.clone(), io);
        block_on(rt.spawn(async move {
            loop {
                let (stream, _peer) = listener.accept().await.unwrap();
                spawn(async move {
                    let _ = handle_conn(stream).await;
                });
            }
        }));
    });

    let start = Instant::now();
    let clients: Vec<_> = (0..N_CLIENTS)
        .map(|_| {
            thread::spawn(move || {
                let mut conn = TcpStream::connect(addr).unwrap();
                conn.write_all(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n").unwrap();
                let mut resp = String::new();
                conn.read_to_string(&mut resp).unwrap();
                assert!(resp.contains("200 OK"), "bad status line: {resp:?}");
                assert!(resp.ends_with("HELLO"), "bad body: {resp:?}");
            })
        })
        .collect();

    for c in clients {
        c.join().unwrap();
    }
    let elapsed = start.elapsed();

    // Serial handling would take ~N_CLIENTS * HANDLER_DELAY (~1.28s). Overlapping
    // on the single lane should finish far sooner — proof of multiplexing.
    let serial = HANDLER_DELAY * N_CLIENTS as u32;
    assert!(
        elapsed < serial / 2,
        "served {N_CLIENTS} connections in {elapsed:?}; serial would be ~{serial:?} — \
         expected heavy overlap"
    );
}
