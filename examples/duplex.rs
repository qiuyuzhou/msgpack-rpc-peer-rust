use msgpack_rpc_peer::RpcPeer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The application supplies its runtime and an already-established stream.
    let (left, right) = tokio::io::duplex(4096);
    let (a, a_connection) = RpcPeer::builder()
        .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })?
        .start(left)?;
    let (b, b_connection) = RpcPeer::builder()
        .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })?
        .start(right)?;

    let (sum, doubled) = tokio::join!(
        a.request::<_, u32>("sum", (2, 3)),
        b.request::<_, u32>("double", (7,)),
    );
    println!("sum = {}, double = {}", sum?, doubled?);

    a_connection.terminate();
    a_connection.wait().await?;
    b_connection.terminate();
    b_connection.wait().await?;
    Ok(())
}
