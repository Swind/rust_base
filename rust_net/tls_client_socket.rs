use std::io::{self, ErrorKind, Read, Write};
use std::sync::{Arc, Mutex, Weak};

use crate::{ReadCallback, StreamSocket, WriteCallback};
use rustls::ClientConnection;
use rustls::pki_types::ServerName;

/// Largest plausible TLS record, so one transport read usually yields a whole
/// record. rustls buffers partial records, so a smaller value would still be
/// correct — just chattier.
const READ_CHUNK: usize = 16 * 1024;

/// Callback for an operation that only signals success/failure.
type DoneCallback = Box<dyn FnOnce(io::Result<()>) + Send>;

/// A TLS connection layered over a transport `StreamSocket`.
///
/// `rustls` does no I/O itself — it's a byte-in/byte-out state machine. This
/// type pumps those bytes through the transport's callback-based `read`/`write`
/// on the IO thread:
///
/// - **plaintext write** → `conn.writer()` encrypts → `write_tls` → transport
///   write
/// - **plaintext read**  ← `conn.reader()` decrypts ← `read_tls` ← transport
///   read
///
/// `TlsClientSocket` itself implements [`StreamSocket`], so once the handshake
/// completes an HTTP layer can treat it exactly like a plaintext
/// `TcpClientSocket`.
///
/// Construct it on an already-*TCP-connected* transport, then call
/// [`TlsClientSocket::handshake`] before reading or writing application data.
/// Must be driven from the IO thread and kept alive until callbacks fire.
pub struct TlsClientSocket {
    transport: Arc<dyn StreamSocket>,
    conn: Mutex<ClientConnection>,
    // Lets the callback-driven pumps re-acquire an owning handle to `self`.
    this: Weak<Self>,
}

impl TlsClientSocket {
    /// Wrap a connected `transport`. `server_name` is used for SNI and
    /// certificate verification, so it must be the host name (not an IP).
    pub fn new(
        transport: Arc<dyn StreamSocket>,
        config: Arc<rustls::ClientConfig>,
        server_name: ServerName<'static>,
    ) -> io::Result<Arc<Self>> {
        let conn = ClientConnection::new(config, server_name).map_err(io::Error::other)?;
        Ok(Arc::new_cyclic(|w| Self { transport, conn: Mutex::new(conn), this: w.clone() }))
    }

    fn arc(&self) -> Arc<Self> {
        self.this.upgrade().expect("TlsClientSocket dropped mid-operation")
    }

    /// Run the TLS handshake to completion, then fire `cb`.
    ///
    /// The transport must already be TCP-connected.
    pub fn handshake(&self, cb: DoneCallback) {
        Self::drive_handshake(self.arc(), cb);
    }

    // ── Pumps ───────────────────────────────────────────────────────────────

    /// Flush every byte rustls currently wants to send, then call `done`.
    /// Writing into a `Vec` never short-writes, so each `write_tls` drains all
    /// queued records; we loop only because new records can appear in between.
    fn flush_pending(me: Arc<Self>, done: DoneCallback) {
        let outgoing = {
            let mut conn = me.conn.lock().unwrap();
            if conn.wants_write() {
                let mut buf = Vec::new();
                // Vec is an infallible Writer.
                conn.write_tls(&mut buf).expect("write_tls into Vec");
                buf
            } else {
                Vec::new()
            }
        };

        if outgoing.is_empty() {
            done(Ok(()));
            return;
        }

        let next = Arc::clone(&me);
        write_all(
            Arc::clone(&me.transport),
            outgoing,
            Box::new(move |res| match res {
                Ok(()) => Self::flush_pending(next, done),
                Err(e) => done(Err(e)),
            }),
        );
    }

    /// Read one transport chunk, feed it to rustls, and process it. On success
    /// `done(Ok(()))`; the caller decides what to do next (retry a read, or
    /// continue the handshake).
    fn feed_one(me: Arc<Self>, done: DoneCallback) {
        let next = Arc::clone(&me);
        me.transport.read(
            READ_CHUNK,
            Box::new(move |res| match res {
                Ok(data) if data.is_empty() => {
                    done(Err(io::Error::new(ErrorKind::UnexpectedEof, "transport closed")))
                }
                Ok(data) => {
                    let r = {
                        let mut conn = next.conn.lock().unwrap();
                        let mut slice = data.as_slice();
                        conn.read_tls(&mut slice)
                            .and_then(|_| conn.process_new_packets().map_err(io::Error::other))
                    };
                    match r {
                        Ok(_) => done(Ok(())),
                        Err(e) => done(Err(e)),
                    }
                }
                Err(e) => done(Err(e)),
            }),
        );
    }

    fn drive_handshake(me: Arc<Self>, cb: DoneCallback) {
        // 1. Send anything rustls has queued (ClientHello, etc.).
        let after_flush = Arc::clone(&me);
        Self::flush_pending(
            Arc::clone(&me),
            Box::new(move |res| {
                if let Err(e) = res {
                    cb(Err(e));
                    return;
                }
                // 2. Finished?
                if !after_flush.conn.lock().unwrap().is_handshaking() {
                    cb(Ok(()));
                    return;
                }
                // 3. Need more from the peer; feed one chunk, then loop.
                let loop_back = Arc::clone(&after_flush);
                Self::feed_one(
                    after_flush,
                    Box::new(move |res| match res {
                        Ok(()) => Self::drive_handshake(loop_back, cb),
                        Err(e) => cb(Err(e)),
                    }),
                );
            }),
        );
    }
}

impl StreamSocket for TlsClientSocket {
    fn read(&self, len: usize, cb: ReadCallback) {
        // Serve already-decrypted plaintext if rustls has any.
        let mut buf = vec![0u8; len];
        let pulled = {
            let mut conn = self.conn.lock().unwrap();
            match conn.reader().read(&mut buf) {
                Ok(n) => Ok(Some(n)), // n == 0 means clean EOF
                Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(None),
                Err(e) => Err(e),
            }
        };

        match pulled {
            Ok(Some(n)) => {
                buf.truncate(n);
                cb(Ok(buf));
            }
            Err(e) => cb(Err(e)),
            Ok(None) => {
                // No plaintext buffered: pull more ciphertext, flush any
                // post-handshake response rustls produces, then retry.
                let me = self.arc();
                Self::feed_one(
                    self.arc(),
                    Box::new(move |res| match res {
                        Ok(()) => {
                            let retry = Arc::clone(&me);
                            Self::flush_pending(
                                me,
                                Box::new(move |res| match res {
                                    Ok(()) => retry.read(len, cb),
                                    Err(e) => cb(Err(e)),
                                }),
                            );
                        }
                        Err(e) => cb(Err(e)),
                    }),
                );
            }
        }
    }

    fn write(&self, buf: Vec<u8>, cb: WriteCallback) {
        let len = buf.len();
        if let Err(e) = self.conn.lock().unwrap().writer().write_all(&buf) {
            cb(Err(e));
            return;
        }
        // Encrypted form is queued inside rustls; flush it to the transport.
        Self::flush_pending(
            self.arc(),
            Box::new(move |res| match res {
                Ok(()) => cb(Ok(len)),
                Err(e) => cb(Err(e)),
            }),
        );
    }

    fn disconnect(&self) {
        self.conn.lock().unwrap().send_close_notify();
        let me = self.arc();
        // Best-effort: flush the close_notify, then drop the transport.
        Self::flush_pending(self.arc(), Box::new(move |_| me.transport.disconnect()));
    }
}

/// Write the whole buffer to `transport`, following partial writes until done.
fn write_all(transport: Arc<dyn StreamSocket>, buf: Vec<u8>, done: DoneCallback) {
    let len = buf.len();
    let chunk = buf.clone();
    let t = Arc::clone(&transport);
    transport.write(
        chunk,
        Box::new(move |res| match res {
            Ok(n) if n >= len => done(Ok(())),
            Ok(n) => write_all(t, buf[n..].to_vec(), done),
            Err(e) => done(Err(e)),
        }),
    );
}
