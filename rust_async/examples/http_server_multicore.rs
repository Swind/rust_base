//! The HTTP server, sharded across N single-thread lanes (thread-per-core).
//!
//! Identical to `http_server`, except the executor is a [`Runtime`] with one
//! lane per core instead of a single `current_thread` lane. The accept loop
//! runs on lane 0 and round-robins each connection onto a lane; from then on
//! that connection's socket is watched and served entirely on its own lane (no
//! cross-lane hand-off), so the lanes scale across cores while each stays
//! ordered and lock-free internally.
//!
//! ```text
//! cargo run -p rust_async --example http_server_multicore
//! curl http://127.0.0.1:8080/
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_lite::AsyncWriteExt; // for `.close()`; read/write are inherent on `Async`
use rust_async::net::{Async, TcpListener};
use rust_async::runtime::Runtime;

fn main() -> io::Result<()> {
    let lanes = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    println!("listening on http://{addr} across {lanes} lanes");

    let rt = Runtime::new(lanes);
    let rt2 = Arc::clone(&rt);
    rt.run(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    // Round-robin the connection onto a lane.
                    rt2.spawn(async move {
                        let _ = handle(stream).await;
                    });
                }
                Err(e) => eprintln!("accept error: {e}"),
            }
        }
    });
    Ok(())
}

async fn handle(mut stream: Async) -> io::Result<()> {
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

    let body = "Hello from a thread-per-core lane!\n";
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.close().await
}
