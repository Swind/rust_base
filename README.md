# rust_base

[![CI](https://github.com/Swind/rust_base/actions/workflows/ci.yml/badge.svg)](https://github.com/Swind/rust_base/actions/workflows/ci.yml)

A personal base library for Rust projects, porting core concepts from Chromium's `base/` and `net/` layers.

## Crates

| Crate | Description | Platform |
|-------|-------------|----------|
| [`rust_task`](rust_task/) | Thread pool, task runners, sequencing, monitoring | cross-platform |
| [`rust_io`](rust_io/) | epoll event loop (`IoTaskRunner`), async file I/O (`FileProxy`) | Linux |
| [`rust_net`](rust_net/) | Async TCP socket (`SocketPosix`) | Linux |

## Dependency graph

```
rust_task  ←── rust_io  ←── rust_net
```

## Usage

```toml
[dependencies]
rust_task = { git = "https://github.com/Swind/rust_base" }
rust_io   = { git = "https://github.com/Swind/rust_base" }
rust_net  = { git = "https://github.com/Swind/rust_base" }
```
