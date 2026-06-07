//! A minimal HTTP/1.1 server on a **single fused lane**.
//!
//! The runtime is one `IoTaskRunner` used as *both* the executor and the
//! reactor, so everything — the accept loop, every connection, the reactor's
//! epoll, and the response writing — runs on **one** thread. Concurrency comes
//! from `async`/`.await`, not from threads: when a connection blocks on a
//! socket read, the lane moves on to another ready connection. The only other
//! threads in the process are the parallel pool that `offload` uses for heavy
//! work.
//!
//! Run it:
//! ```text
//! cargo run -p rust_async --example http_server
//! curl http://127.0.0.1:8080/
//! curl http://127.0.0.1:8080/compute     # exercises offload (CPU work off-lane)
//! ```

use std::io;
use std::net::SocketAddr;

use futures_lite::AsyncWriteExt; // for `.close()`; read/write are inherent on `Async`
use rust_async::net::{Async, TcpListener};
use rust_async::{Runnable, Runtime, block_on, offload, spawn};
use rust_io::IoTaskRunner;
use rust_task::TaskRunner;

fn main() -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    println!("listening on http://{addr}  (try / and /compute)");

    // One fused lane: the same IoTaskRunner is the executor and the reactor.
    let io = IoTaskRunner::new();
    let exec = io.clone();
    let rt = Runtime::new(
        move |r: Runnable| {
            exec.post_task(Box::new(move || {
                r.run();
            }));
        },
        io,
    );

    // `block_on` waits on the calling thread; the server runs on the lane. Each
    // connection task is spawned with the free `spawn`, inheriting `rt`, so it
    // stays on the same single lane.
    block_on(rt.spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("accept error: {e}");
                    continue;
                }
            };
            spawn(async move {
                if let Err(e) = handle(stream).await {
                    eprintln!("connection {peer} error: {e}");
                }
            });
        }
    }))
}

/// Read one request, route it, write one response, half-close.
async fn handle(mut stream: Async) -> io::Result<()> {
    let mut buf = vec![0u8; 8 * 1024];
    let mut len = 0;

    // Read until we have the full header block (CRLF CRLF) or the client closes.
    let header_end = loop {
        if let Some(end) = find_header_end(&buf[..len]) {
            break end;
        }
        if len == buf.len() {
            return write_simple(&mut stream, 431, "Request Header Fields Too Large").await;
        }
        let n = stream.read(&mut buf[len..]).await?;
        if n == 0 {
            return Ok(()); // client went away before sending a full request
        }
        len += n;
    };

    // Parse just enough to route on the path.
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    let path = match req.parse(&buf[..header_end]) {
        Ok(_) => req.path.unwrap_or("/").to_string(),
        Err(_) => return write_simple(&mut stream, 400, "Bad Request").await,
    };

    let body = match path.as_str() {
        // Heavy CPU work: pushed onto the parallel offload pool so it never
        // stalls the reactor lane; execution resumes back here when it's done.
        "/compute" => {
            let sum = offload(|| (0u64..50_000_000).sum::<u64>()).await;
            format!("sum(0..50_000_000) = {sum}\n")
        }
        other => format!("Hello from rust_async! path = {other}\n"),
    };

    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.close().await
}

async fn write_simple(stream: &mut Async, code: u16, reason: &str) -> io::Result<()> {
    let resp =
        format!("HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    stream.write_all(resp.as_bytes()).await?;
    stream.close().await
}

/// Index just past the `\r\n\r\n` that ends the request headers, if present.
fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}
