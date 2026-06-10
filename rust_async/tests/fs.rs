//! Async filesystem access: positional helpers, directory/metadata ops, and
//! the cursor-based `File` (AsyncRead/AsyncWrite/AsyncSeek).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use rust_async::block_on;
use rust_async::fs::{self, File, OpenOptions};
use rust_async::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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
        let f = OpenOptions::new().read(true).write(true).create(true).open(&path).await.unwrap();
        f.write_at(0, b"0000000000".to_vec()).await.unwrap();
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
fn cursor_read_write_seek_via_traits() {
    let path = temp_path();
    block_on(async {
        // Sequential cursor writes through the AsyncWrite trait.
        let mut f = File::create(&path).await.unwrap();
        f.write_all(b"hello ").await.unwrap();
        f.write_all(b"world").await.unwrap();
        f.flush().await.unwrap();

        // Re-open and read sequentially via AsyncRead.
        let mut r = File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello world");

        // Seek then read from the new cursor position.
        r.seek(std::io::SeekFrom::Start(6)).await.unwrap();
        let mut rest = [0u8; 5];
        r.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"world");

        // SeekFrom::End resolves the length off the executor.
        let end = r.seek(std::io::SeekFrom::End(0)).await.unwrap();
        assert_eq!(end, 11);
    });
    std::fs::remove_file(&path).ok();
}

#[test]
fn file_via_buf_reader_lines() {
    use rust_async::io::{AsyncBufReadExt, BufReader};
    use rust_async::stream::StreamExt;

    let path = temp_path();
    block_on(async {
        File::create(&path).await.unwrap().write_all(b"a\nb\nc\n").await.unwrap();
        let reader = BufReader::new(File::open(&path).await.unwrap());
        let mut got = Vec::new();
        let mut lines = reader.lines();
        while let Some(line) = lines.next().await {
            got.push(line.unwrap());
        }
        assert_eq!(got, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    });
    std::fs::remove_file(&path).ok();
}

#[test]
fn read_missing_file_errors() {
    let path = temp_path();
    let r = block_on(fs::read(&path));
    assert!(r.is_err());
}

#[test]
fn dir_create_list_remove() {
    use rust_async::stream::StreamExt;

    let dir = temp_path();
    block_on(async {
        fs::create_dir_all(&dir).await.unwrap();
        fs::write(dir.join("a.txt"), b"a".to_vec()).await.unwrap();
        fs::write(dir.join("b.txt"), b"bb".to_vec()).await.unwrap();

        let mut names = Vec::new();
        let mut entries = fs::read_dir(&dir).await.unwrap();
        while let Some(entry) = entries.next().await {
            let entry = entry.unwrap();
            assert!(entry.file_type().await.unwrap().is_file());
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        assert_eq!(names, vec!["a.txt".to_string(), "b.txt".to_string()]);

        fs::remove_dir_all(&dir).await.unwrap();
        assert!(fs::metadata(&dir).await.is_err());
    });
}

#[test]
fn metadata_reports_len_and_type() {
    let path = temp_path();
    block_on(async {
        fs::write(&path, b"12345".to_vec()).await.unwrap();
        let md = fs::metadata(&path).await.unwrap();
        assert!(md.is_file());
        assert_eq!(md.len(), 5);
    });
    std::fs::remove_file(&path).ok();
}

#[test]
fn rename_and_copy() {
    let a = temp_path();
    let b = temp_path();
    let c = temp_path();
    block_on(async {
        fs::write(&a, b"data".to_vec()).await.unwrap();
        fs::rename(&a, &b).await.unwrap();
        assert!(fs::metadata(&a).await.is_err());
        let n = fs::copy(&b, &c).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(fs::read(&c).await.unwrap(), b"data");
    });
    for p in [&a, &b, &c] {
        std::fs::remove_file(p).ok();
    }
}
