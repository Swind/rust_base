use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use crate::message_pump::{FdWatcher, MessagePumpDelegate, MessagePumpForIo, WatchMode};

// Internal per-registration state.
struct WatchEntry {
    watcher: Weak<dyn FdWatcher + Send + Sync>,
    persistent: bool,
    mode: WatchMode,
    generation: u64,
}

/// Linux `epoll`-based [`MessagePumpForIo`] backend.
///
/// Owns the `epoll` fd and an `eventfd` used to interrupt a blocked
/// `epoll_wait` when new work is posted.  Created by `IoTaskRunner::new`; you
/// rarely construct one directly.
///
/// Mirrors `base::MessagePumpEpoll` in Chromium.
pub struct EpollMessagePump {
    epoll_fd: RawFd,
    wake_fd: RawFd, // eventfd: written by schedule_work to interrupt epoll_wait
    watches: Mutex<HashMap<RawFd, WatchEntry>>,
    generation_counter: AtomicU64,
    quit: AtomicBool,
}

impl EpollMessagePump {
    /// Create the epoll loop's fds.  Does not start any thread — the caller
    /// (`IoTaskRunner`) spawns the IO thread and calls [`run`](Self::run).
    pub fn new() -> Arc<Self> {
        let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        assert!(epoll_fd >= 0, "epoll_create1 failed");

        let wake_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        assert!(wake_fd >= 0, "eventfd failed");

        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: wake_fd as u64 };
        let rc = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, wake_fd, &mut ev) };
        assert_eq!(rc, 0, "epoll_ctl(ADD wake_fd) failed");

        Arc::new(Self {
            epoll_fd,
            wake_fd,
            watches: Mutex::new(HashMap::new()),
            generation_counter: AtomicU64::new(0),
            quit: AtomicBool::new(false),
        })
    }
}

impl Drop for EpollMessagePump {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.epoll_fd);
            libc::close(self.wake_fd);
        }
    }
}

impl MessagePumpForIo for EpollMessagePump {
    fn schedule_work(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(self.wake_fd, &raw const val as *const libc::c_void, 8);
        }
    }

    fn quit(&self) {
        self.quit.store(true, Ordering::Release);
        self.schedule_work();
    }

    fn register_fd(
        &self,
        fd: RawFd,
        persistent: bool,
        mode: WatchMode,
        watcher: Weak<dyn FdWatcher + Send + Sync>,
    ) -> Option<u64> {
        let generation = self.generation_counter.fetch_add(1, Ordering::Relaxed);
        let epoll_events = mode_to_epoll_events(mode);
        let mut ev = libc::epoll_event { events: epoll_events, u64: fd as u64 };

        let rc = unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut ev) };
        if rc < 0 {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::EEXIST {
                let rc2 =
                    unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_MOD, fd, &mut ev) };
                if rc2 < 0 {
                    return None;
                }
            } else {
                return None;
            }
        }

        self.watches
            .lock()
            .unwrap()
            .insert(fd, WatchEntry { watcher, persistent, mode, generation });
        Some(generation)
    }

    fn unregister_fd(&self, fd: RawFd, generation: u64) -> bool {
        let mut watches = self.watches.lock().unwrap();
        match watches.get(&fd) {
            Some(e) if e.generation == generation => {
                watches.remove(&fd);
                let mut dummy = libc::epoll_event { events: 0, u64: 0 };
                unsafe { libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, &mut dummy) };
                true
            }
            _ => false,
        }
    }

    fn run(&self, delegate: Arc<dyn MessagePumpDelegate>) {
        delegate.on_run_start();

        loop {
            // Let the task layer run everything that's ready, and tell us when
            // the next delayed task is due.
            let next_deadline = delegate.do_work();

            if self.quit.load(Ordering::Acquire) {
                break;
            }

            let timeout_ms: i32 = match next_deadline {
                None => -1,
                Some(deadline) => {
                    let now = Instant::now();
                    if deadline <= now {
                        0
                    } else {
                        (deadline - now).as_millis().min(i32::MAX as u128) as i32
                    }
                }
            };

            let mut events = [libc::epoll_event { events: 0, u64: 0 }; 64];
            let n = unsafe { libc::epoll_wait(self.epoll_fd, events.as_mut_ptr(), 64, timeout_ms) };

            if n < 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                break;
            }

            for ev in &events[..n as usize] {
                let fd = ev.u64 as RawFd;
                let ev_flags = ev.events;

                if fd == self.wake_fd {
                    let mut buf = [0u8; 8];
                    unsafe {
                        libc::read(self.wake_fd, buf.as_mut_ptr() as *mut libc::c_void, 8);
                    };
                    continue;
                }

                // Hold the lock only long enough to extract the watcher and
                // decide whether to remove the entry.  Release before calling
                // callbacks so the callback can re-arm via watch_file_descriptor
                // without deadlocking.
                let (watcher_opt, can_read, can_write) = {
                    let mut watches = self.watches.lock().unwrap();
                    let Some(entry) = watches.get(&fd) else { continue };
                    let can_read = (ev_flags & libc::EPOLLIN as u32) != 0
                        && matches!(entry.mode, WatchMode::Read | WatchMode::ReadWrite);
                    let can_write = (ev_flags & libc::EPOLLOUT as u32) != 0
                        && matches!(entry.mode, WatchMode::Write | WatchMode::ReadWrite);
                    let watcher = entry.watcher.upgrade();
                    if !entry.persistent {
                        watches.remove(&fd);
                        let mut dummy = libc::epoll_event { events: 0, u64: 0 };
                        unsafe {
                            libc::epoll_ctl(self.epoll_fd, libc::EPOLL_CTL_DEL, fd, &mut dummy)
                        };
                    }
                    (watcher, can_read, can_write)
                };

                if let Some(w) = watcher_opt {
                    if can_read {
                        delegate.begin_work_item();
                        w.on_file_can_read_without_blocking(fd);
                        delegate.end_work_item();
                    }
                    if can_write {
                        delegate.begin_work_item();
                        w.on_file_can_write_without_blocking(fd);
                        delegate.end_work_item();
                    }
                }
            }
        }

        delegate.on_run_end();
    }
}

fn mode_to_epoll_events(mode: WatchMode) -> u32 {
    match mode {
        WatchMode::Read => libc::EPOLLIN as u32,
        WatchMode::Write => libc::EPOLLOUT as u32,
        WatchMode::ReadWrite => (libc::EPOLLIN | libc::EPOLLOUT) as u32,
    }
}
