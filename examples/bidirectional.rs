use msgpack_rpc_peer::RpcPeer;
use tokio::sync::mpsc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (left, right) = tokio::io::duplex(4096);
    let (events, mut received) = mpsc::channel(2);
    let a_events = events.clone();
    let (a, a_connection) = RpcPeer::builder()
        .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })?
        .notification("event", move |_, (text,): (String,)| {
            let events = a_events.clone();
            async move {
                events.send(text).await.unwrap();
                Ok(())
            }
        })?
        .start(left)?;
    let (b, b_connection) = RpcPeer::builder()
        .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })?
        .notification("event", move |_, (text,): (String,)| {
            let events = events.clone();
            async move {
                events.send(text).await.unwrap();
                Ok(())
            }
        })?
        .start(right)?;

    let (sum, doubled, a_sent, b_sent) = tokio::join!(
        a.request::<_, u32>("sum", (2, 3)),
        b.request::<_, u32>("double", (7,)),
        a.notify("event", ("hello from a",)),
        b.notify("event", ("hello from b",)),
    );
    println!("sum = {}, double = {}", sum?, doubled?);
    a_sent?;
    b_sent?;
    // Local enqueue success does not acknowledge execution. This application waits
    // on its own handler signals; the notifications can complete in either order.
    for _ in 0..2 {
        println!("{}", received.recv().await.unwrap());
    }

    a_connection.terminate();
    a_connection.wait().await?;
    b_connection.terminate();
    b_connection.wait().await?;
    Ok(())
}
