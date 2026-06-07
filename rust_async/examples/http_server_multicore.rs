//! The HTTP server, sharded across N fused lanes (thread-per-core).
//!
//! Identical to `http_server`, except instead of one fused [`Runtime`] there
//! are `N` of them — one per core, each its own `IoTaskRunner` used as both
//! executor and reactor. The accept loop runs on lane 0 and round-robins each
//! connection onto a lane by spawning it on that lane's `Runtime`; from then on
//! the connection's socket is watched and served entirely on its own lane (no
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
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_lite::AsyncWriteExt; // for `.close()`; read/write are inherent on `Async`
use rust_async::net::{Async, TcpListener};
use rust_async::{Runnable, Runtime, block_on};
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;

/// One fused lane: an `IoTaskRunner` used as both executor and reactor.
fn lane() -> Runtime {
    let io = IoTaskRunner::new();
    let exec = io.clone();
    Runtime::new(
        move |r: Runnable| {
            exec.post_task(Box::new(move || {
                r.run();
            }));
        },
        io,
    )
}

fn main() -> io::Result<()> {
    let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    println!("listening on http://{addr} across {n} lanes");

    let lanes: Arc<Vec<Runtime>> = Arc::new((0..n).map(|_| lane()).collect());
    let next = Arc::new(AtomicUsize::new(0));

    // The accept loop itself runs on lane 0.
    let lanes2 = Arc::clone(&lanes);
    block_on(lanes[0].spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    // Round-robin the connection onto a lane's runtime.
                    let lane = next.fetch_add(1, Ordering::Relaxed) % lanes2.len();
                    lanes2[lane].spawn(async move {
                        let _ = handle(stream).await;
                    });
                }
                Err(e) => eprintln!("accept error: {e}"),
            }
        }
    }))
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
