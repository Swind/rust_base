#[cfg(target_os = "linux")]
pub mod socket_posix;

#[cfg(target_os = "linux")]
pub use self::socket_posix::SocketPosix;
