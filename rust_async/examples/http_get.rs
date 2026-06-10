//! A plain-HTTP client that resolves a host name and fetches a URL's headers.
//!
//! The interesting bit is `Async::connect("example.com:80")`: it accepts
//! anything `std::net::ToSocketAddrs` does and resolves the name on the
//! blocking pool (DNS is a blocking syscall), then connects without blocking
//! the executor. For HTTPS see `rust_net`'s `https_get` example.
//!
//! Run with:
//! ```text
//! cargo run -p rust_async --example http_get            # defaults to example.com
//! cargo run -p rust_async --example http_get neverssl.com
//! ```
//! Requires outbound network access.

use rust_async::Async;
use rust_async::block_on;

fn main() {
    let host = std::env::args().nth(1).unwrap_or_else(|| "example.com".to_string());

    let result = block_on(async {
        let conn = Async::connect(format!("{host}:80")).await?;
        println!("connected to {}", conn.peer_addr()?);

        let request = format!("GET / HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
        conn.write_all(request.as_bytes()).await?;

        // Read until EOF (the server closes after the response with Connection: close).
        let mut response = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = conn.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            response.extend_from_slice(&buf[..n]);
        }
        Ok::<_, std::io::Error>(response)
    });

    match result {
        Ok(response) => {
            let text = String::from_utf8_lossy(&response);
            // Print just the status line + headers (up to the blank line).
            let head = text.split("\r\n\r\n").next().unwrap_or("");
            println!("--- response head ---\n{head}");
            println!("\n({} bytes total)", response.len());
        }
        Err(e) => eprintln!("request to {host} failed: {e}"),
    }
}
