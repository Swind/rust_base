//! A tiny `cat -n`: read standard input line by line and echo it to standard
//! output with line numbers. The only example that exercises the async
//! standard streams.
//!
//! `io::stdin`/`io::stdout` are not epoll-driven (the fds may be files, pipes,
//! or TTYs); each read/write is offloaded to the blocking pool. A `BufReader`
//! turns the byte stream into lines.
//!
//! Run with:
//! ```text
//! echo -e "alpha\nbeta\ngamma" | cargo run -p rust_async --example async_cat
//! cargo run -p rust_async --example async_cat < src/lib.rs
//! ```

use rust_async::block_on;
use rust_async::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout};
use rust_async::stream::StreamExt;

fn main() -> std::io::Result<()> {
    block_on(async {
        let mut out = stdout();
        let reader = BufReader::new(stdin());
        let mut lines = reader.lines();

        let mut n = 1u64;
        while let Some(line) = lines.next().await {
            let line = line?;
            out.write_all(format!("{n:>6}\t{line}\n").as_bytes()).await?;
            n += 1;
        }
        out.flush().await?;
        Ok(())
    })
}
