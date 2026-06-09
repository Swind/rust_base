//! A minimal HTTP/1.1 server where **`main` itself joins the runtime's pool**.
//!
//! There is no `block_on` here. Instead the pattern is the one the runtime is
//! built for:
//!
//! 1. Build a [`Runtime`] = a `ThreadPool` executor + a reactor
//!    (`IoTaskRunner`).
//! 2. `rt.spawn(...)` the entrypoint (the accept loop) onto that runtime.
//! 3. Have `main` *join the pool* via [`ThreadPool::attach_current_thread`], so
//!    the main thread becomes one more worker instead of parking idle.
//!
//! The executor is a pool of worker threads (plus `main`); the reactor runs on
//! its own thread. Connections are spawned with the free [`spawn`], inheriting
//! the runtime, so they are processed by any worker. A server's accept loop
//! never returns, so the process runs until killed (or until someone calls
//! `pool.shutdown()`, which is what releases `attach_current_thread`).
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
use rust_async::{Runtime, offload, spawn};
use rust_io::IoTaskRunner;
use rust_task::{TaskTraits, ThreadPool};

fn main() -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    println!("listening on http://{addr}  (try / and /compute)");

    // Executor: a pool of worker threads. Reactor: its own IoTaskRunner thread.
    let pool = ThreadPool::new(4);
    let rt = Runtime::new(pool.create_task_runner(TaskTraits::default()), IoTaskRunner::new());

    // Post the entrypoint onto the runtime; each connection is spawned with the
    // free `spawn`, inheriting `rt`, so it runs on the same pool.
    rt.spawn(async move {
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
    });

    // `main` joins the pool as one more worker and runs until shutdown. The
    // accept loop never completes, so for a server this blocks for the process
    // lifetime — no idle parked thread, `main` does real work.
    pool.attach_current_thread();
    Ok(())
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
