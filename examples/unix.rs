#[cfg(unix)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use msgpack_rpc_peer::RpcPeer;
    // The application establishes the socket; both ends have the same RPC API.
    let (left, right) = tokio::net::UnixStream::pair()?;
    let (a, owner_a) = RpcPeer::builder()
        .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })?
        .start(left)?;
    let (b, owner_b) = RpcPeer::builder()
        .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })?
        .start(right)?;
    let (sum, doubled) = tokio::join!(
        a.request::<_, u32>("sum", (2, 3)),
        b.request::<_, u32>("double", (7,))
    );
    assert_eq!(sum?, 5);
    assert_eq!(doubled?, 14);
    owner_a.terminate();
    owner_a.wait().await?;
    owner_b.wait().await?;
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("Unix domain sockets require a Unix platform");
}
