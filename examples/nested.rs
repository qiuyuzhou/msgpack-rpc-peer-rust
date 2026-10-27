use msgpack_rpc_peer::{RemoteError, RpcPeer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (left, right) = tokio::io::duplex(4096);
    let (a, a_connection) = RpcPeer::builder()
        .method("double", |context, (n,): (u32,)| async move {
            // A reverse call can itself await another reverse call.
            let offset: u32 = context
                .request("offset", ())
                .await
                .map_err(|_| RemoteError::new("offset call failed".into()).unwrap())?;
            Ok(n * 2 + offset)
        })?
        .start(left)?;
    let (_b, b_connection) = RpcPeer::builder()
        .method("compute", |context, (n,): (u32,)| async move {
            let result: u32 = context
                .request("double", (n,))
                .await
                .map_err(|_| RemoteError::new("double call failed".into()).unwrap())?;
            Ok(result)
        })?
        .method("offset", |_, (): ()| async { Ok(1u32) })?
        .start(right)?;

    // a -> b.compute -> a.double -> b.offset, then results unwind to a.
    let result: u32 = a.request("compute", (21,)).await?;
    assert_eq!(result, 43);
    println!("nested result = {result}");

    a_connection.terminate();
    a_connection.wait().await?;
    b_connection.terminate();
    b_connection.wait().await?;
    Ok(())
}
