//! A small filesystem pipeline tying together the `fs` and `io` layers:
//! `File::create`/`OpenOptions`, `read_dir` as a stream, `BufReader::lines`,
//! and `io::copy` — all on the blocking pool, never stalling the reactor.
//!
//! It writes a few log files, lists the directory, filters every `ERROR` line
//! out of each log via a buffered line reader, writes the result to one file,
//! copies that file with `io::copy`, and reports sizes from `fs::metadata`.
//!
//! Run with: `cargo run -p rust_async --example file_pipeline`

use std::path::PathBuf;

use rust_async::block_on;
use rust_async::fs::{self, File};
use rust_async::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, copy};
use rust_async::stream::StreamExt;

fn main() -> std::io::Result<()> {
    let dir: PathBuf =
        std::env::temp_dir().join(format!("rust_async_file_pipeline_{}", std::process::id()));

    block_on(async {
        // Fresh working directory.
        fs::create_dir_all(&dir).await?;

        // Write three logs, each a mix of INFO and ERROR lines.
        for (name, body) in [
            ("auth.log", "INFO login ok\nERROR bad password\nINFO logout\n"),
            ("net.log", "INFO connect\nERROR timeout\nERROR reset\n"),
            ("app.log", "INFO start\nINFO tick\nINFO stop\n"),
        ] {
            let mut f = File::create(dir.join(name)).await?;
            f.write_all(body.as_bytes()).await?;
            f.flush().await?;
        }

        // List the directory (read_dir yields a stream of entries).
        let mut names = Vec::new();
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next().await {
            names.push(entry?.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        println!("logs in {}: {:?}", dir.display(), names);

        // Filter ERROR lines out of every .log via a buffered line reader.
        let mut errors = String::new();
        for name in &names {
            let path = dir.join(name);
            let reader = BufReader::new(File::open(&path).await?);
            let mut lines = reader.lines();
            while let Some(line) = lines.next().await {
                let line = line?;
                if line.contains("ERROR") {
                    errors.push_str(&format!("{name}: {line}\n"));
                }
            }
        }

        // Persist the collected errors, then duplicate the file with io::copy.
        let errors_path = dir.join("errors.txt");
        let backup_path = dir.join("errors.bak");
        File::create(&errors_path).await?.write_all(errors.as_bytes()).await?;

        let mut src = File::open(&errors_path).await?;
        let mut dst = File::create(&backup_path).await?;
        let copied = copy(&mut src, &mut dst).await?;
        dst.flush().await?;

        println!("\n--- errors.txt ---\n{errors}");
        println!(
            "copied {copied} bytes; errors.txt is {} bytes, backup is {} bytes",
            fs::metadata(&errors_path).await?.len(),
            fs::metadata(&backup_path).await?.len(),
        );

        // Clean up.
        fs::remove_dir_all(&dir).await?;
        Ok(())
    })
}
