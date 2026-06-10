//! Unix-domain sockets end to end, within one process: a `UnixListener` whose
//! `incoming()` stream feeds an echo task, a `UnixStream` client that does a
//! round-trip, and a `UnixDatagram` pair.
//!
//! This is the `async_tcp_echo` story over `AF_UNIX` — the exact same reactor
//! readiness path, just a different address family.
//!
//! Run with: `cargo run -p rust_async --example unix_echo`

use rust_async::os::unix::net::{UnixDatagram, UnixListener, UnixStream};
use rust_async::stream::StreamExt;
use rust_async::{block_on, spawn};

fn main() -> std::io::Result<()> {
    let path =
        std::env::temp_dir().join(format!("rust_async_unix_echo_{}.sock", std::process::id()));
    std::fs::remove_file(&path).ok();

    let result = block_on(async {
        let listener = UnixListener::bind(&path)?;

        // Echo server: serve two connections off the incoming() stream, then stop.
        let server = spawn(async move {
            let mut incoming = listener.incoming();
            for _ in 0..2 {
                if let Some(conn) = incoming.next().await {
                    let conn = conn?;
                    let mut buf = [0u8; 256];
                    let n = conn.read(&mut buf).await?;
                    conn.write_all(&buf[..n]).await?;
                }
            }
            Ok::<_, std::io::Error>(())
        });

        // Two clients, each doing a request/response round-trip.
        for msg in [b"hello".as_slice(), b"again".as_slice()] {
            let conn = UnixStream::connect(&path).await?;
            conn.write_all(msg).await?;
            let mut buf = [0u8; 256];
            let n = conn.read(&mut buf).await?;
            println!("stream echo: {:?}", String::from_utf8_lossy(&buf[..n]));
        }
        server.await?;

        // Connected datagram pair.
        let (a, b) = UnixDatagram::pair()?;
        a.send(b"datagram ping").await?;
        let mut buf = [0u8; 256];
        let n = b.recv(&mut buf).await?;
        println!("datagram echo: {:?}", String::from_utf8_lossy(&buf[..n]));

        Ok::<_, std::io::Error>(())
    });

    std::fs::remove_file(&path).ok();
    result
}
