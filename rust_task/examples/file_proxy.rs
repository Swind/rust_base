//! FileProxy demo — async file I/O backed by a blocking thread pool.
//!
//! Three demos:
//!
//!  1. **Write then read** — write a file asynchronously, then read it back
//!     by chaining a read call from inside the write callback.
//!  2. **Append** — build a file incrementally with three append operations,
//!     each chained from the previous callback.
//!  3. **Concurrent reads** — issue multiple reads against the same file at
//!     the same time; all callbacks fire on the IO thread even though the
//!     actual reads run in parallel on the thread pool.
//!
//! Run with:
//!   cargo run --example file_proxy

fn main() {
    #[cfg(target_os = "linux")]
    linux::run();
    #[cfg(not(target_os = "linux"))]
    eprintln!("This example requires Linux.");
}

#[cfg(target_os = "linux")]
mod linux {
    use rust_task::{FileProxy, IoTaskRunner, TaskRunner, ThreadPool};
    use std::sync::{Arc, Barrier, Mutex};

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir()
            .join(format!("rust_task_fp_example_{}_{}", std::process::id(), tag))
    }

    pub fn run() {
        demo_write_then_read();
        demo_append();
        demo_concurrent_reads();
    }

    // ── Demo 1: write then read ───────────────────────────────────────────────

    fn demo_write_then_read() {
        println!("=== Demo 1: write then read ===");

        let pool = ThreadPool::new(2);
        let io = IoTaskRunner::new();
        let path = temp_path("write_read");

        let received = Arc::new(Mutex::new(Vec::<u8>::new()));
        let barrier = Arc::new(Barrier::new(2));

        let r = Arc::clone(&received);
        let b = Arc::clone(&barrier);
        let p = path.clone();
        let pool2 = Arc::clone(&pool);
        io.post_task(Box::new(move || {
            // Both proxies share the same pool — no need to manage runners manually.
            let file_w = FileProxy::new(&p, Arc::clone(&pool2));
            let file_r = FileProxy::new(&p, pool2);

            file_w.write_all(b"hello from FileProxy".to_vec(), move |result| {
                result.expect("write failed");
                println!("  write complete, now reading back...");

                // Chain: read the file from inside the write callback.
                // Safe because the callback runs on the IO thread.
                file_r.read_all(move |result| {
                    let data = result.expect("read failed");
                    println!("  read back: {:?}", std::str::from_utf8(&data).unwrap());
                    *r.lock().unwrap() = data;
                    b.wait();
                });
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();
        std::fs::remove_file(&path).ok();

        assert_eq!(*received.lock().unwrap(), b"hello from FileProxy");
        println!();
    }

    // ── Demo 2: chained appends ───────────────────────────────────────────────

    fn demo_append() {
        println!("=== Demo 2: chained appends ===");

        let pool = ThreadPool::new(2);
        let io = IoTaskRunner::new();
        let path = temp_path("append");

        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        let p = path.clone();
        let pool2 = Arc::clone(&pool);

        io.post_task(Box::new(move || {
            let f1 = FileProxy::new(&p, Arc::clone(&pool2));
            let f2 = FileProxy::new(&p, Arc::clone(&pool2));
            let f3 = FileProxy::new(&p, pool2);

            f1.append(b"one ".to_vec(), move |r| {
                r.unwrap();
                println!("  appended \"one \"");
                f2.append(b"two ".to_vec(), move |r| {
                    r.unwrap();
                    println!("  appended \"two \"");
                    f3.append(b"three".to_vec(), move |r| {
                        r.unwrap();
                        println!("  appended \"three\"");
                        b.wait();
                    });
                });
            });
        }));

        barrier.wait();
        io.shutdown();
        pool.shutdown();

        let contents = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).ok();

        println!("  final file contents: {:?}", std::str::from_utf8(&contents).unwrap());
        assert_eq!(contents, b"one two three");
        println!();
    }

    // ── Demo 3: concurrent reads ──────────────────────────────────────────────

    fn demo_concurrent_reads() {
        println!("=== Demo 3: concurrent reads ===");

        let path = temp_path("concurrent");
        std::fs::write(&path, b"abcdefghij").unwrap();

        let pool = ThreadPool::new(4);
        let io = IoTaskRunner::new();

        let results: Arc<Mutex<Vec<(u64, Vec<u8>)>>> = Arc::new(Mutex::new(Vec::new()));
        // Only main + the last arriving callback meet at the barrier.
        // All callbacks run on the single IO thread sequentially, so blocking
        // each one would deadlock — only the last one (when all results are in)
        // signals the main thread.
        let barrier = Arc::new(Barrier::new(2));
        const NUM_READS: usize = 3;

        for &offset in &[0u64, 3, 7] {
            let file = FileProxy::new(&path, Arc::clone(&pool));
            let r = Arc::clone(&results);
            let b = Arc::clone(&barrier);

            io.post_task(Box::new(move || {
                file.read(offset, 3, move |result| {
                    let data = result.unwrap();
                    println!(
                        "  read @ offset {}: {:?}",
                        offset,
                        std::str::from_utf8(&data).unwrap()
                    );
                    let mut got = r.lock().unwrap();
                    got.push((offset, data));
                    if got.len() == NUM_READS {
                        b.wait();
                    }
                });
            }));
        }

        barrier.wait();
        io.shutdown();
        pool.shutdown();
        std::fs::remove_file(&path).ok();

        let mut got = results.lock().unwrap().clone();
        got.sort_by_key(|(offset, _)| *offset);
        assert_eq!(got[0].1, b"abc"); // offset 0
        assert_eq!(got[1].1, b"def"); // offset 3
        assert_eq!(got[2].1, b"hij"); // offset 7
        println!("  all reads returned correct data\n");
    }
}
