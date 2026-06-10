//! Standard streams over the blocking pool. stdin needs a real pipe to test
//! meaningfully, so here we drive the stdout/stderr writer state machine
//! (poll_write → poll_flush → poll_close) to completion via the futures-util
//! AsyncWriteExt combinators.

use rust_async::block_on;
use rust_async::io::{AsyncWriteExt, stderr, stdout};

#[test]
fn stdout_write_flush_close() {
    block_on(async {
        let mut out = stdout();
        out.write_all(b"rust_async stdout test\n").await.unwrap();
        out.flush().await.unwrap();
        out.close().await.unwrap();
    });
}

#[test]
fn stderr_write_flush() {
    block_on(async {
        let mut err = stderr();
        err.write_all(b"rust_async stderr test\n").await.unwrap();
        err.flush().await.unwrap();
    });
}

#[test]
fn stdout_many_small_writes() {
    block_on(async {
        let mut out = stdout();
        for _ in 0..16 {
            out.write_all(b".").await.unwrap();
        }
        out.write_all(b"\n").await.unwrap();
        out.flush().await.unwrap();
    });
}
