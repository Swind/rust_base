//! Two UDP sockets exchanging datagrams within one process, showing both the
//! addressed (`send_to`/`recv_from`) and connected (`connect` + `send`/`recv`)
//! styles of `net::UdpSocket`.
//!
//! Run with: `cargo run -p rust_async --example udp_chat`

use rust_async::block_on;
use rust_async::net::UdpSocket;

fn main() -> std::io::Result<()> {
    block_on(async {
        // Two sockets on OS-assigned loopback ports.
        let alice = UdpSocket::bind("127.0.0.1:0")?;
        let bob = UdpSocket::bind("127.0.0.1:0")?;
        let alice_addr = alice.local_addr()?;
        let bob_addr = bob.local_addr()?;

        // Addressed style: alice -> bob, bob replies to the sender it learned.
        alice.send_to(b"hi bob", bob_addr).await?;
        let mut buf = [0u8; 256];
        let (n, from) = bob.recv_from(&mut buf).await?;
        println!("bob received {:?} from {from}", String::from_utf8_lossy(&buf[..n]));
        bob.send_to(b"hi alice", from).await?;

        let (n, _) = alice.recv_from(&mut buf).await?;
        println!("alice received {:?}", String::from_utf8_lossy(&buf[..n]));

        // Connected style: pin each socket to its peer, then use send/recv.
        alice.connect(bob_addr)?;
        bob.connect(alice_addr)?;
        for i in 0..3 {
            alice.send(format!("ping {i}").as_bytes()).await?;
            let n = bob.recv(&mut buf).await?;
            println!("round {i}: bob got {:?}", String::from_utf8_lossy(&buf[..n]));
            bob.send(b"pong").await?;
            let n = alice.recv(&mut buf).await?;
            println!("round {i}: alice got {:?}", String::from_utf8_lossy(&buf[..n]));
        }
        Ok(())
    })
}
