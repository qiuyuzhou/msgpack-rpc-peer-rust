use std::sync::Arc;

use msgpack_rpc_peer::{Error, PeerBuilder, RemoteError, RpcPeer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Semaphore, mpsc},
    time::{Duration, timeout},
};

#[tokio::test]
async fn responses_complete_in_reverse_order_while_first_handler_waits() {
    timeout(Duration::from_secs(3), async {
        let (started, mut starts) = mpsc::channel(2);
        let release = Arc::new(Semaphore::new(0));
        let gate = release.clone();
        let (left, right) = tokio::io::duplex(128);
        let (a, a_connection) = RpcPeer::builder().start(left).unwrap();
        let (_b, b_connection) = RpcPeer::builder()
            .method("work", move |_, (n,): (u32,)| {
                let started = started.clone();
                let gate = gate.clone();
                async move {
                    started.send(n).await.unwrap();
                    if n == 1 {
                        gate.acquire().await.unwrap().forget();
                    }
                    Ok(n * 10)
                }
            })
            .unwrap()
            .start(right)
            .unwrap();
        let calling = async {
            let first = a.request::<_, u32>("work", (1,));
            let second = async {
                assert_eq!(starts.recv().await, Some(1));
                assert_eq!(a.request::<_, u32>("work", (2,)).await.unwrap(), 20);
                assert_eq!(starts.recv().await, Some(2));
                // The second response has arrived before the first can be produced.
                release.add_permits(1);
            };
            let (first, ()) = tokio::join!(first, second);
            assert_eq!(first.unwrap(), 10);
        };
        calling.await;
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn opposite_originating_id_zero_does_not_collide_with_pending_call() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(128);
        let (peer, connection) = RpcPeer::builder()
            .method("inc", |_, (n,): (u32,)| async move { Ok(n + 1) })
            .unwrap()
            .start(stream)
            .unwrap();
        let calling = peer.request::<_, u32>("inc", (7,));
        let remote = async {
            let mut request = [0; 9];
            raw.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"\x94\x00\x00\xa3inc\x91\x07");
            // Request 0 from the opposite namespace and response 0 concatenated.
            raw.write_all(b"\x94\x00\x00\xa3inc\x91\x29\x94\x01\x00\xc0\x08")
                .await
                .unwrap();
            let mut response = [0; 5];
            raw.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"\x94\x01\x00\xc0\x2a");
        };
        let (result, ()) = tokio::join!(calling, remote);
        assert_eq!(result.unwrap(), 8);
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

fn nested_peer() -> PeerBuilder {
    RpcPeer::builder()
        .method("outer", |context, (n,): (u32,)| async move {
            context
                .request::<_, u32>("middle", (n,))
                .await
                .map_err(remote_error)
        })
        .unwrap()
        .method("middle", |context, (n,): (u32,)| async move {
            context
                .request::<_, u32>("leaf", (n,))
                .await
                .map_err(remote_error)
        })
        .unwrap()
        .method("leaf", |_, (n,): (u32,)| async move { Ok(n + 1) })
        .unwrap()
}

fn remote_error(_: Error) -> RemoteError {
    RemoteError::new("reverse call failed".into()).unwrap()
}

#[tokio::test]
async fn both_peers_await_nested_bidirectional_calls() {
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(64);
        let (a, a_connection) = nested_peer().start(left).unwrap();
        let (b, b_connection) = nested_peer().start(right).unwrap();
        let (a_result, b_result) = tokio::join!(
            a.request::<_, u32>("outer", (10,)),
            b.request::<_, u32>("outer", (20,)),
        );
        assert_eq!(a_result.unwrap(), 11);
        assert_eq!(b_result.unwrap(), 21);
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn notifications_can_await_reverse_calls_and_send_notifications_concurrently() {
    timeout(Duration::from_secs(3), async {
        let (received, mut events) = mpsc::channel(4);
        let gate = Arc::new(Semaphore::new(0));
        let builder = || {
            let received = received.clone();
            let completed = received.clone();
            let gate = gate.clone();
            nested_peer()
                .notification("outer", move |context, (n,): (u32,)| {
                    let received = received.clone();
                    let gate = gate.clone();
                    async move {
                        received.send(n).await.unwrap();
                        gate.acquire().await.unwrap().forget();
                        let result: u32 =
                            context.request("outer", (n,)).await.map_err(remote_error)?;
                        context
                            .notify("done", (result,))
                            .await
                            .map_err(remote_error)
                    }
                })
                .unwrap()
                .notification("done", {
                    let received = completed;
                    move |_, (n,): (u32,)| {
                        let received = received.clone();
                        async move {
                            received.send(n).await.unwrap();
                            Ok(())
                        }
                    }
                })
                .unwrap()
        };
        let (left, right) = tokio::io::duplex(64);
        let (a, a_connection) = builder().start(left).unwrap();
        let (b, b_connection) = builder().start(right).unwrap();
        let (a_sent, b_sent) = tokio::join!(a.notify("outer", (10,)), b.notify("outer", (20,)));
        a_sent.unwrap();
        b_sent.unwrap();
        let mut started = [events.recv().await.unwrap(), events.recv().await.unwrap()];
        started.sort();
        assert_eq!(started, [10, 20]);
        // Both notification handlers are blocked, but incoming calls still progress.
        let (a_result, b_result) = tokio::join!(
            a.request::<_, u32>("outer", (30,)),
            b.request::<_, u32>("outer", (40,)),
        );
        assert_eq!(a_result.unwrap(), 31);
        assert_eq!(b_result.unwrap(), 41);
        gate.add_permits(2);
        let mut results = [events.recv().await.unwrap(), events.recv().await.unwrap()];
        results.sort();
        assert_eq!(results, [11, 21]);
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn full_notification_handler_budget_does_not_block_responses() {
    timeout(Duration::from_secs(3), async {
        let (started, mut starts) = mpsc::channel(64);
        let (stream, mut raw) = tokio::io::duplex(128);
        let (peer, connection) = RpcPeer::builder()
            .notification("wait", move |_, (): ()| {
                let started = started.clone();
                async move {
                    started.send(()).await.unwrap();
                    std::future::pending::<Result<(), RemoteError>>().await
                }
            })
            .unwrap()
            .method("ok", |_, (): ()| async { Ok(()) })
            .unwrap()
            .start(stream)
            .unwrap();
        for _ in 0..64 {
            raw.write_all(b"\x93\x02\xa4wait\x90").await.unwrap();
        }
        for _ in 0..64 {
            starts.recv().await.unwrap();
        }
        // Requests and notifications share the same bounded handler budget.
        raw.write_all(b"\x94\x00\x00\xa2ok\x90").await.unwrap();
        let expected =
            b"\x94\x01\x00\x82\xa4code\xd1\x83\x00\xa7message\xbahandler capacity exhausted\xc0";
        let mut actual = vec![0; expected.len()];
        raw.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        let calling = peer.request::<_, u32>("ok", ());
        let remote = async {
            let mut request = [0; 7];
            raw.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"\x94\x00\x00\xa2ok\x90");
            raw.write_all(b"\x94\x01\x00\xc0\x2a").await.unwrap();
        };
        let (result, ()) = tokio::join!(calling, remote);
        assert_eq!(result.unwrap(), 42);
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}
