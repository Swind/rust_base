use std::io;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use crate::io_task_runner::{FdWatchController, FdWatcher, IoTaskRunner, WatchMode};

const INVALID_FD: i32 = -1;

type ConnectCb = Box<dyn FnOnce(io::Result<()>) + Send>;
type AcceptCb = Box<dyn FnOnce(io::Result<Arc<SocketPosix>>) + Send>;

// ── Pending operations
// ────────────────────────────────────────────────────────

enum ReadOp {
    // ReadIfReady: just notify when fd is readable; caller does its own read.
    ReadIfReady(Box<dyn FnOnce(io::Result<()>) + Send>),
    // Read: IoTaskRunner does the read and delivers the data.
    Read { len: usize, cb: Box<dyn FnOnce(io::Result<Vec<u8>>) + Send> },
}

struct WriteOp {
    buf: Vec<u8>,
    cb: Box<dyn FnOnce(io::Result<usize>) + Send>,
}

// ── SocketPosix
// ───────────────────────────────────────────────────────────────

/// Async TCP socket backed by `IoTaskRunner`.
///
/// Mirrors Chromium's `net::SocketPosix`: wraps a non-blocking fd and exposes
/// callback-based connect / read / write / accept operations.  All methods
/// that touch epoll **must be called from the IO thread**.
///
/// # Client lifecycle
///
/// ```ignore
/// let socket = SocketPosix::new();
/// socket.open(&addr)?;
/// let s = Arc::clone(&socket);
/// socket.connect(addr, move |result| {
///     result.unwrap();
///     s.read(1024, move |result| { println!("{} bytes", result.unwrap().len()); });
/// });
/// ```
///
/// # Server lifecycle
///
/// ```ignore
/// let server = SocketPosix::new();
/// server.open(&addr)?;
/// server.bind(addr)?;
/// server.listen(128)?;
///
/// let srv = Arc::clone(&server);
/// server.accept(move |result| {
///     let client = result.unwrap();
///     client.read(1024, move |result| { /* handle data */ });
///     // call srv.accept() again to accept the next connection
/// });
/// ```
pub struct SocketPosix {
    fd: AtomicI32,
    read_watcher: Mutex<FdWatchController>,
    write_watcher: Mutex<FdWatchController>,
    pending_read: Mutex<Option<ReadOp>>,
    pending_write: Mutex<Option<WriteOp>>,
    pending_connect: Mutex<Option<ConnectCb>>,
    pending_accept: Mutex<Option<AcceptCb>>,
}

impl SocketPosix {
    /// Create a new, unopened socket.  Call `open()` before any other method.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            fd: AtomicI32::new(INVALID_FD),
            read_watcher: Mutex::new(FdWatchController::new()),
            write_watcher: Mutex::new(FdWatchController::new()),
            pending_read: Mutex::new(None),
            pending_write: Mutex::new(None),
            pending_connect: Mutex::new(None),
            pending_accept: Mutex::new(None),
        })
    }

    /// Wrap an already-connected fd (e.g. from `accept4(2)`).
    ///
    /// The socket takes ownership of `fd` and closes it on drop.
    pub fn from_fd(fd: RawFd) -> Arc<Self> {
        Arc::new(Self {
            fd: AtomicI32::new(fd),
            read_watcher: Mutex::new(FdWatchController::new()),
            write_watcher: Mutex::new(FdWatchController::new()),
            pending_read: Mutex::new(None),
            pending_write: Mutex::new(None),
            pending_connect: Mutex::new(None),
            pending_accept: Mutex::new(None),
        })
    }

    /// Open the socket fd.  The address family is inferred from `addr`.
    pub fn open(&self, addr: &SocketAddr) -> io::Result<()> {
        let family = if addr.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
        let fd = unsafe {
            libc::socket(family, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        self.fd.store(fd, Ordering::Relaxed);
        Ok(())
    }

    /// Async connect.  Callback fires with `Ok(())` on success.
    ///
    /// Must be called from the IO thread.
    pub fn connect(
        self: &Arc<Self>,
        addr: SocketAddr,
        cb: impl FnOnce(io::Result<()>) + Send + 'static,
    ) {
        let fd = self.fd.load(Ordering::Relaxed);
        assert!(fd >= 0, "socket not open");

        match syscall_connect(fd, &addr) {
            Ok(()) => cb(Ok(())),
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {
                *self.pending_connect.lock().unwrap() = Some(Box::new(cb));
                self.arm_write();
            }
            Err(e) => cb(Err(e)),
        }
    }

    /// Notify `cb` when the fd is readable without blocking.
    ///
    /// The callback does **not** receive data — it just signals readiness.
    /// The caller is responsible for calling `read()` (or its own `libc::read`)
    /// after receiving the notification.  This mirrors Chromium's
    /// `ReadIfReady`.
    ///
    /// Must be called from the IO thread.
    pub fn read_if_ready(self: &Arc<Self>, cb: impl FnOnce(io::Result<()>) + Send + 'static) {
        assert!(self.fd.load(Ordering::Relaxed) >= 0, "socket not open");
        *self.pending_read.lock().unwrap() = Some(ReadOp::ReadIfReady(Box::new(cb)));
        self.arm_read();
    }

    /// Read up to `len` bytes.  Callback receives the data.
    ///
    /// Tries an immediate `read(2)` first; only registers a watch on EAGAIN.
    /// The callback may receive fewer bytes than `len` (partial read).
    ///
    /// Must be called from the IO thread.
    pub fn read(
        self: &Arc<Self>,
        len: usize,
        cb: impl FnOnce(io::Result<Vec<u8>>) + Send + 'static,
    ) {
        let fd = self.fd.load(Ordering::Relaxed);
        assert!(fd >= 0, "socket not open");

        let mut buf = vec![0u8; len];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, len) };
        if n >= 0 {
            buf.truncate(n as usize);
            cb(Ok(buf));
            return;
        }
        let err = io::Error::last_os_error();
        if !is_would_block(&err) {
            cb(Err(err));
            return;
        }
        // EAGAIN: register watch, store callback
        *self.pending_read.lock().unwrap() = Some(ReadOp::Read { len, cb: Box::new(cb) });
        self.arm_read();
    }

    /// Write `buf`.  Callback receives bytes written (may be partial).
    ///
    /// Tries an immediate `write(2)` first; only registers a watch on EAGAIN.
    /// If a partial write occurs, the caller is responsible for writing the
    /// remaining bytes.
    ///
    /// Must be called from the IO thread.
    pub fn write(
        self: &Arc<Self>,
        buf: Vec<u8>,
        cb: impl FnOnce(io::Result<usize>) + Send + 'static,
    ) {
        let fd = self.fd.load(Ordering::Relaxed);
        assert!(fd >= 0, "socket not open");

        let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
        if n >= 0 {
            cb(Ok(n as usize));
            return;
        }
        let err = io::Error::last_os_error();
        if !is_would_block(&err) {
            cb(Err(err));
            return;
        }
        // EAGAIN: register watch, store callback
        *self.pending_write.lock().unwrap() = Some(WriteOp { buf, cb: Box::new(cb) });
        self.arm_write();
    }

    /// Bind the socket to `addr`.  Sets `SO_REUSEADDR` automatically.
    ///
    /// Call after `open()` and before `listen()`.
    pub fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        let fd = self.fd.load(Ordering::Relaxed);
        assert!(fd >= 0, "socket not open");
        // SO_REUSEADDR avoids "address already in use" after a quick restart.
        let one: libc::c_int = 1;
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        syscall_bind(fd, &addr)
    }

    /// Start listening for incoming connections.
    ///
    /// Call after `bind()` and before `accept()`.
    pub fn listen(&self, backlog: i32) -> io::Result<()> {
        let fd = self.fd.load(Ordering::Relaxed);
        assert!(fd >= 0, "socket not open");
        let rc = unsafe { libc::listen(fd, backlog) };
        if rc < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    /// Accept one incoming connection.  Callback receives a new `SocketPosix`
    /// for the client, or an error.
    ///
    /// The callback is one-shot: call `accept()` again inside the callback to
    /// keep accepting.  Must be called from the IO thread.
    pub fn accept(self: &Arc<Self>, cb: impl FnOnce(io::Result<Arc<SocketPosix>>) + Send + 'static) {
        let fd = self.fd.load(Ordering::Relaxed);
        assert!(fd >= 0, "socket not open");

        match do_accept(fd) {
            Ok(client_fd) => cb(Ok(SocketPosix::from_fd(client_fd))),
            Err(e) if e.raw_os_error() == Some(libc::EAGAIN) => {
                *self.pending_accept.lock().unwrap() = Some(Box::new(cb));
                self.arm_read();
            }
            Err(e) => cb(Err(e)),
        }
    }

    /// Cancel a pending `accept`.
    pub fn cancel_accept(&self) {
        *self.pending_accept.lock().unwrap() = None;
        self.read_watcher.lock().unwrap().stop_watching_file_descriptor();
    }

    /// Cancel a pending `read_if_ready` or `read`.
    pub fn cancel_read(&self) {
        *self.pending_read.lock().unwrap() = None;
        self.read_watcher.lock().unwrap().stop_watching_file_descriptor();
    }

    /// Cancel a pending `write`.
    pub fn cancel_write(&self) {
        *self.pending_write.lock().unwrap() = None;
        self.write_watcher.lock().unwrap().stop_watching_file_descriptor();
    }

    /// Close the socket and cancel all pending operations.
    pub fn close(&self) {
        self.read_watcher.lock().unwrap().stop_watching_file_descriptor();
        self.write_watcher.lock().unwrap().stop_watching_file_descriptor();
        *self.pending_read.lock().unwrap() = None;
        *self.pending_write.lock().unwrap() = None;
        *self.pending_connect.lock().unwrap() = None;
        *self.pending_accept.lock().unwrap() = None;
        let fd = self.fd.swap(INVALID_FD, Ordering::Relaxed);
        if fd >= 0 {
            unsafe { libc::close(fd) };
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    fn arm_read(self: &Arc<Self>) {
        let fd = self.fd.load(Ordering::Relaxed);
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        io.watch_file_descriptor(
            fd,
            false,
            WatchMode::Read,
            &mut self.read_watcher.lock().unwrap(),
            Arc::clone(self) as Arc<dyn FdWatcher + Send + Sync>,
        );
    }

    fn arm_write(self: &Arc<Self>) {
        let fd = self.fd.load(Ordering::Relaxed);
        let io = IoTaskRunner::current().expect("must be called from the IO thread");
        io.watch_file_descriptor(
            fd,
            false,
            WatchMode::Write,
            &mut self.write_watcher.lock().unwrap(),
            Arc::clone(self) as Arc<dyn FdWatcher + Send + Sync>,
        );
    }
}

impl Default for SocketPosix {
    fn default() -> Self {
        Self {
            fd: AtomicI32::new(INVALID_FD),
            read_watcher: Mutex::new(FdWatchController::new()),
            write_watcher: Mutex::new(FdWatchController::new()),
            pending_read: Mutex::new(None),
            pending_write: Mutex::new(None),
            pending_connect: Mutex::new(None),
            pending_accept: Mutex::new(None),
        }
    }
}

impl Drop for SocketPosix {
    fn drop(&mut self) {
        let fd = self.fd.swap(INVALID_FD, Ordering::Relaxed);
        if fd >= 0 {
            unsafe { libc::close(fd) };
        }
    }
}

impl FdWatcher for SocketPosix {
    fn on_file_can_read_without_blocking(&self, fd: RawFd) {
        // A listening socket becomes readable when a new connection arrives;
        // check pending_accept before the regular read path.
        let accept_cb = self.pending_accept.lock().unwrap().take();
        if let Some(cb) = accept_cb {
            cb(do_accept(fd).map(SocketPosix::from_fd));
            return;
        }

        // Extract the pending read op before calling any callback.  Rust extends
        // the lifetime of a temporary MutexGuard through an entire `match`
        // scrutinee, so using `match self.pending_read.lock()...take()` would
        // hold the lock while the callback runs — deadlocking if the callback
        // calls read() again.
        let op = self.pending_read.lock().unwrap().take();
        match op {
            Some(ReadOp::ReadIfReady(cb)) => cb(Ok(())),
            Some(ReadOp::Read { len, cb }) => {
                let mut buf = vec![0u8; len];
                let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, len) };
                if n >= 0 {
                    buf.truncate(n as usize);
                    cb(Ok(buf));
                } else {
                    cb(Err(io::Error::last_os_error()));
                }
            }
            None => {} // spurious wakeup
        }
    }

    fn on_file_can_write_without_blocking(&self, fd: RawFd) {
        // Same lock-before-callback pattern: extract then release before calling.
        let connect_cb = self.pending_connect.lock().unwrap().take();
        if let Some(cb) = connect_cb {
            cb(check_connect_error(fd));
            return;
        }

        let write_op = self.pending_write.lock().unwrap().take();
        if let Some(WriteOp { buf, cb }) = write_op {
            let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
            if n >= 0 {
                cb(Ok(n as usize));
            } else {
                cb(Err(io::Error::last_os_error()));
            }
        }
    }
}

// ── Syscall helpers
// ───────────────────────────────────────────────────────────

fn is_would_block(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::EAGAIN)
}

fn do_accept(fd: RawFd) -> io::Result<RawFd> {
    let new_fd = unsafe {
        libc::accept4(
            fd,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    };
    if new_fd >= 0 { Ok(new_fd) } else { Err(io::Error::last_os_error()) }
}

fn syscall_bind(fd: RawFd, addr: &SocketAddr) -> io::Result<()> {
    let rc = match addr {
        SocketAddr::V4(a) => {
            let sa = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: libc::in_addr { s_addr: u32::from(*a.ip()).to_be() },
                sin_zero: [0; 8],
            };
            unsafe {
                libc::bind(
                    fd,
                    &sa as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            }
        }
        SocketAddr::V6(a) => {
            let sa = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: 0,
                sin6_addr: libc::in6_addr { s6_addr: a.ip().octets() },
                sin6_scope_id: 0,
            };
            unsafe {
                libc::bind(
                    fd,
                    &sa as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            }
        }
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

fn syscall_connect(fd: RawFd, addr: &SocketAddr) -> io::Result<()> {
    let rc = match addr {
        SocketAddr::V4(a) => {
            let sa = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: a.port().to_be(),
                sin_addr: libc::in_addr { s_addr: u32::from(*a.ip()).to_be() },
                sin_zero: [0; 8],
            };
            unsafe {
                libc::connect(
                    fd,
                    &sa as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            }
        }
        SocketAddr::V6(a) => {
            let sa = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: a.port().to_be(),
                sin6_flowinfo: 0,
                sin6_addr: libc::in6_addr { s6_addr: a.ip().octets() },
                sin6_scope_id: 0,
            };
            unsafe {
                libc::connect(
                    fd,
                    &sa as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            }
        }
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

fn check_connect_error(fd: RawFd) -> io::Result<()> {
    let mut err: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &mut err as *mut _ as *mut libc::c_void,
            &mut len,
        );
    }
    if err == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(err)) }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_task_runner::IoTaskRunner;
    use crate::task_runner::TaskRunner;
    use std::sync::{Arc, Barrier};

    // Create a connected Unix socket pair; both ends are non-blocking.
    fn socket_pair() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        assert_eq!(rc, 0, "socketpair failed");
        (fds[0], fds[1])
    }

    fn write_raw(fd: RawFd, data: &[u8]) {
        unsafe { libc::write(fd, data.as_ptr() as *const libc::c_void, data.len()) };
    }

    fn close_fd(fd: RawFd) {
        unsafe { libc::close(fd) };
    }

    // ── read ─────────────────────────────────────────────────────────────────

    #[test]
    fn read_delivers_data() {
        let io = IoTaskRunner::new();
        let (fd0, fd1) = socket_pair();
        let socket = SocketPosix::from_fd(fd0);

        let barrier = Arc::new(Barrier::new(2));
        let received = Arc::new(Mutex::new(Vec::new()));

        let s = Arc::clone(&socket);
        let b = Arc::clone(&barrier);
        let r = Arc::clone(&received);
        io.post_task(Box::new(move || {
            s.read(64, move |result| {
                *r.lock().unwrap() = result.unwrap();
                b.wait();
            });
        }));

        write_raw(fd1, b"hello");
        barrier.wait();

        io.shutdown();
        assert_eq!(*received.lock().unwrap(), b"hello");
        close_fd(fd1);
    }

    #[test]
    fn read_immediate_when_data_already_present() {
        let io = IoTaskRunner::new();
        let (fd0, fd1) = socket_pair();
        write_raw(fd1, b"world"); // write before registering the read

        let socket = SocketPosix::from_fd(fd0);
        let barrier = Arc::new(Barrier::new(2));
        let received = Arc::new(Mutex::new(Vec::new()));

        let s = Arc::clone(&socket);
        let b = Arc::clone(&barrier);
        let r = Arc::clone(&received);
        io.post_task(Box::new(move || {
            // Data is already there: read() returns immediately without epoll.
            s.read(64, move |result| {
                *r.lock().unwrap() = result.unwrap();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        assert_eq!(*received.lock().unwrap(), b"world");
        close_fd(fd1);
    }

    // ── read_if_ready ─────────────────────────────────────────────────────────

    #[test]
    fn read_if_ready_notifies_then_caller_reads() {
        let io = IoTaskRunner::new();
        let (fd0, fd1) = socket_pair();
        let socket = SocketPosix::from_fd(fd0);

        let barrier = Arc::new(Barrier::new(2));
        let notified = Arc::new(Mutex::new(false));

        let s = Arc::clone(&socket);
        let b = Arc::clone(&barrier);
        let n = Arc::clone(&notified);
        io.post_task(Box::new(move || {
            s.read_if_ready(move |result| {
                result.unwrap();
                *n.lock().unwrap() = true;
                b.wait();
            });
        }));

        write_raw(fd1, b"ping");
        barrier.wait();

        io.shutdown();
        assert!(*notified.lock().unwrap());
        close_fd(fd1);
    }

    // ── write ────────────────────────────────────────────────────────────────

    #[test]
    fn write_delivers_data() {
        let io = IoTaskRunner::new();
        let (fd0, fd1) = socket_pair();
        let socket = SocketPosix::from_fd(fd0);

        let barrier = Arc::new(Barrier::new(2));
        let written = Arc::new(Mutex::new(0usize));

        let s = Arc::clone(&socket);
        let b = Arc::clone(&barrier);
        let w = Arc::clone(&written);
        io.post_task(Box::new(move || {
            s.write(b"hello world".to_vec(), move |result| {
                *w.lock().unwrap() = result.unwrap();
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();

        // Read from the other end to verify the data arrived.
        let mut buf = [0u8; 64];
        let n = unsafe { libc::read(fd1, buf.as_mut_ptr() as *mut libc::c_void, 64) };
        assert_eq!(&buf[..n as usize], b"hello world");
        assert_eq!(*written.lock().unwrap(), 11);
        close_fd(fd1);
    }

    // ── bind / listen / accept ────────────────────────────────────────────────

    // Helper: get the local address a bound fd is listening on.
    fn get_local_addr(fd: RawFd) -> std::net::SocketAddr {
        let mut sa = libc::sockaddr_in {
            sin_family: 0,
            sin_port: 0,
            sin_addr: libc::in_addr { s_addr: 0 },
            sin_zero: [0; 8],
        };
        let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        unsafe { libc::getsockname(fd, &mut sa as *mut _ as *mut libc::sockaddr, &mut len) };
        std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            u16::from_be(sa.sin_port),
        ))
    }

    /// bind + listen + accept delivers a new SocketPosix for the client.
    ///
    /// The server socket is created outside the post_task closure so the test
    /// body holds a strong Arc; IoTaskRunner only keeps Weak references to
    /// FdWatchers, so the object must be kept alive externally.
    #[test]
    fn accept_delivers_new_socket() {
        let io = IoTaskRunner::new();
        let server = SocketPosix::new();

        let accepted = Arc::new(Mutex::new(false));
        let barrier = Arc::new(Barrier::new(2));

        let a = Arc::clone(&accepted);
        let b = Arc::clone(&barrier);
        let srv = Arc::clone(&server);
        io.post_task(Box::new(move || {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            srv.open(&addr).unwrap();
            srv.bind(addr).unwrap();
            srv.listen(1).unwrap();

            let bound_addr = get_local_addr(srv.fd.load(Ordering::Relaxed));
            std::thread::spawn(move || {
                std::net::TcpStream::connect(bound_addr).unwrap();
            });

            srv.accept(move |result| {
                result.expect("accept failed");
                *a.lock().unwrap() = true;
                b.wait();
            });
        }));

        barrier.wait();
        io.shutdown();
        assert!(*accepted.lock().unwrap());
    }

    /// accept() is one-shot; calling it again inside the callback accepts the
    /// next connection.
    #[test]
    fn accept_can_repeat_for_multiple_connections() {
        let io = IoTaskRunner::new();
        let server = SocketPosix::new();

        let count = Arc::new(Mutex::new(0usize));
        let barrier = Arc::new(Barrier::new(2));

        let c = Arc::clone(&count);
        let b = Arc::clone(&barrier);
        let srv = Arc::clone(&server);
        io.post_task(Box::new(move || {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            srv.open(&addr).unwrap();
            srv.bind(addr).unwrap();
            srv.listen(2).unwrap();

            let bound_addr = get_local_addr(srv.fd.load(Ordering::Relaxed));

            // Two clients connect sequentially; keep them alive until both are accepted.
            std::thread::spawn(move || {
                let _c1 = std::net::TcpStream::connect(bound_addr).unwrap();
                let _c2 = std::net::TcpStream::connect(bound_addr).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(100));
            });

            let srv2 = Arc::clone(&srv);
            srv.accept(move |result| {
                result.expect("first accept failed");
                *c.lock().unwrap() += 1;

                let c2 = Arc::clone(&c);
                let b2 = Arc::clone(&b);
                srv2.accept(move |result| {
                    result.expect("second accept failed");
                    *c2.lock().unwrap() += 1;
                    b2.wait();
                });
            });
        }));

        barrier.wait();
        io.shutdown();
        assert_eq!(*count.lock().unwrap(), 2);
    }

    /// Data written by the server via the accepted SocketPosix arrives at the
    /// client (std::net::TcpStream).
    #[test]
    fn server_can_write_to_accepted_client() {
        use std::io::Read as _;

        let io = IoTaskRunner::new();
        let server = SocketPosix::new();

        let barrier = Arc::new(Barrier::new(2));

        let b = Arc::clone(&barrier);
        let srv = Arc::clone(&server);
        io.post_task(Box::new(move || {
            let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
            srv.open(&addr).unwrap();
            srv.bind(addr).unwrap();
            srv.listen(1).unwrap();

            let bound_addr = get_local_addr(srv.fd.load(Ordering::Relaxed));

            let b2 = Arc::clone(&b);
            std::thread::spawn(move || {
                let mut stream = std::net::TcpStream::connect(bound_addr).unwrap();
                let mut buf = [0u8; 64];
                let n = stream.read(&mut buf).unwrap();
                assert_eq!(&buf[..n], b"ping");
                b2.wait();
            });

            srv.accept(move |result| {
                let client = result.expect("accept failed");
                client.write(b"ping".to_vec(), |result| {
                    result.expect("write failed");
                });
            });
        }));

        barrier.wait();
        io.shutdown();
    }

    // ── connect ──────────────────────────────────────────────────────────────

    #[test]
    fn connect_to_loopback() {
        use std::net::{SocketAddr, TcpListener};

        // Bind a listener on an ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let io = IoTaskRunner::new();
        let socket = SocketPosix::new();
        let barrier = Arc::new(Barrier::new(2));
        let connected = Arc::new(Mutex::new(false));

        let s = Arc::clone(&socket);
        let b = Arc::clone(&barrier);
        let c = Arc::clone(&connected);
        io.post_task(Box::new(move || {
            s.open(&addr).unwrap();
            s.connect(addr, move |result| {
                *c.lock().unwrap() = result.is_ok();
                b.wait();
            });
        }));

        // Accept the incoming connection so the kernel completes the handshake.
        let _stream = listener.accept().unwrap();
        barrier.wait();

        io.shutdown();
        assert!(*connected.lock().unwrap());
    }

    // ── callback chaining (regression for pending_read lock held during cb) ───

    /// Calling read() from inside a read callback must not deadlock.
    ///
    /// Before the fix, `on_file_can_read_without_blocking` held the
    /// `pending_read` lock through the entire match block (Rust temporary
    /// lifetime extension).  A callback that called read() again would try to
    /// re-acquire the same lock on the same thread → deadlock.
    #[test]
    fn read_callback_can_chain_another_read() {
        let io = IoTaskRunner::new();
        let (fd0, fd1) = socket_pair();
        let socket = SocketPosix::from_fd(fd0);

        let done = Arc::new(Barrier::new(2));
        let results: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));

        let s = Arc::clone(&socket);
        let d = Arc::clone(&done);
        let r = Arc::clone(&results);
        io.post_task(Box::new(move || {
            let s2 = Arc::clone(&s);
            let r2 = Arc::clone(&r);
            s.read(64, move |result| {
                r2.lock().unwrap().push(result.unwrap());
                // Chain: register a second read from inside the first callback.
                // The pending_read lock must already be released at this point.
                s2.read(64, move |result| {
                    r2.lock().unwrap().push(result.unwrap());
                    d.wait();
                });
            });
        }));

        write_raw(fd1, b"one");
        // Let the first read fire and register the second before writing again.
        std::thread::sleep(std::time::Duration::from_millis(30));
        write_raw(fd1, b"two");
        done.wait();

        io.shutdown();
        let got = results.lock().unwrap();
        assert_eq!(got[0], b"one");
        assert_eq!(got[1], b"two");
        close_fd(fd1);
    }

    /// Calling read() from inside a read_if_ready callback must not deadlock.
    #[test]
    fn read_if_ready_callback_can_chain_read() {
        let io = IoTaskRunner::new();
        let (fd0, fd1) = socket_pair();
        let socket = SocketPosix::from_fd(fd0);

        let done = Arc::new(Barrier::new(2));
        let received = Arc::new(Mutex::new(Vec::new()));

        let s = Arc::clone(&socket);
        let d = Arc::clone(&done);
        let r = Arc::clone(&received);
        io.post_task(Box::new(move || {
            let s2 = Arc::clone(&s);
            s.read_if_ready(move |result| {
                result.unwrap();
                // Data is ready; call read() from inside the readiness callback.
                s2.read(64, move |result| {
                    *r.lock().unwrap() = result.unwrap();
                    d.wait();
                });
            });
        }));

        write_raw(fd1, b"ping");
        done.wait();

        io.shutdown();
        assert_eq!(*received.lock().unwrap(), b"ping");
        close_fd(fd1);
    }
}
