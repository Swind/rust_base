//! Stage 4: async filesystem access over `rust_io::FileProxy`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use rust_async::block_on;
use rust_async::fs::{self, File};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn temp_path() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("rust_async_fs_{}_{}", std::process::id(), n))
}

#[test]
fn write_then_read_round_trip() {
    let path = temp_path();
    block_on(async {
        fs::write(&path, b"hello async fs".to_vec()).await.unwrap();
        let data = fs::read(&path).await.unwrap();
        assert_eq!(data, b"hello async fs");
    });
    std::fs::remove_file(&path).ok();
}

#[test]
fn positional_and_append() {
    let path = temp_path();
    block_on(async {
        let f = File::open(&path);
        f.write_all(b"0000000000".to_vec()).await.unwrap();
        f.write_at(3, b"XYZ".to_vec()).await.unwrap();
        let part = f.read_at(3, 3).await.unwrap();
        assert_eq!(part, b"XYZ");
        f.append(b"!!".to_vec()).await.unwrap();
        let all = f.read_all().await.unwrap();
        assert_eq!(all, b"000XYZ0000!!");
    });
    std::fs::remove_file(&path).ok();
}

#[test]
fn read_missing_file_errors() {
    let path = temp_path();
    let r = block_on(fs::read(&path));
    assert!(r.is_err());
}
