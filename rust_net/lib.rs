#[cfg(target_os = "linux")]
pub mod socket_posix;
#[cfg(target_os = "linux")]
pub mod tcp_client_socket;
#[cfg(target_os = "linux")]
pub mod tcp_server_socket;
#[cfg(target_os = "linux")]
pub mod tcp_socket;

#[cfg(target_os = "linux")]
pub use self::socket_posix::SocketPosix;
#[cfg(target_os = "linux")]
pub use self::tcp_client_socket::TcpClientSocket;
#[cfg(target_os = "linux")]
pub use self::tcp_server_socket::TcpServerSocket;
#[cfg(target_os = "linux")]
pub use self::tcp_socket::TcpSocket;
