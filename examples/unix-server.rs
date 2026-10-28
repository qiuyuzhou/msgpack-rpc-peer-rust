// Server fixture for tests: serves `sum` over a Unix domain socket until killed.
// Start it with `cargo run --locked --example unix-server -- <socket-path>`,
// then connect any MessagePack-RPC client and call `sum` with two `u32`
// parameters to receive their sum. Readiness is signaled on stderr and by the
// socket file existing.
#[cfg(unix)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use msgpack_rpc_peer::RpcPeer;
    use tokio::net::UnixListener;

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| usage_exit("usage: unix-server <socket-path>"));

    let listener = UnixListener::bind(&path)?;
    eprintln!("listening on {path}");

    // The registry is built once; every accepted connection shares it.
    let server = RpcPeer::builder().method("sum", |_, (a, b): (u32, u32)| async move {
        Ok(u64::from(a) + u64::from(b))
    })?;

    loop {
        let (stream, _peer_address) = listener.accept().await?;
        let (_peer, owner) = server.start(stream)?;
        tokio::spawn(async move {
            if let Err(error) = owner.wait().await {
                eprintln!("connection failed: {error}");
            }
        });
    }
}

#[cfg(unix)]
fn usage_exit(message: &'static str) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

#[cfg(not(unix))]
fn main() {
    eprintln!("Unix domain sockets require a Unix platform");
}
