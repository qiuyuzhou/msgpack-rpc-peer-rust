use msgpack_rpc_peer::{Config, Error, RpcPeer, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, timeout},
};

async fn empty_call(peer: &RpcPeer, dynamic: bool) -> Result<Value, Error> {
    if dynamic {
        peer.request_value("x", Value::Array(vec![])).await
    } else {
        peer.request::<_, u32>("x", ()).await.map(Value::from)
    }
}

#[tokio::test]
async fn invalid_arguments_release_pending_capacity_without_reusing_the_id() {
    for dynamic in [false, true] {
        let scenario = async {
            let (stream, mut raw) = tokio::io::duplex(64);
            let (peer, connection) = RpcPeer::builder()
                .config(Config {
                    max_pending_calls: 1,
                    ..Config::default()
                })
                .start(stream)
                .unwrap();
            // Passes the compile-time params contract but serializes a bare scalar.
            #[derive(serde::Serialize)]
            struct ScalarParams(u32);
            impl msgpack_rpc_peer::RpcParams for ScalarParams {}
            assert_eq!(
                if dynamic {
                    peer.request_value("x", 7.into()).await.map(|_| ())
                } else {
                    peer.request::<_, ()>("x", ScalarParams(7)).await
                },
                Err(Error::InvalidParams)
            );
            let (result, ()) = tokio::join!(empty_call(&peer, dynamic), async {
                let mut bytes = [0; 6];
                raw.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"\x94\x00\x01\xa1x\x90");
                raw.write_all(b"\x94\x01\x01\xc0\x2a").await.unwrap();
            });
            assert_eq!(result, Ok(Value::from(42)));
            connection.terminate();
            connection.wait().await.unwrap();
        };
        timeout(Duration::from_secs(3), scenario).await.unwrap();
    }
}

#[tokio::test]
async fn timed_out_calls_ignore_late_responses_and_never_reuse_ids() {
    for dynamic in [false, true] {
        let scenario = async {
            let (stream, mut raw) = tokio::io::duplex(64);
            let (peer, connection) = RpcPeer::builder()
                .config(Config {
                    default_call_timeout: Duration::from_millis(100),
                    ..Config::default()
                })
                .start(stream)
                .unwrap();
            let (result, ()) = tokio::join!(empty_call(&peer, dynamic), async {
                let mut bytes = [0; 6];
                raw.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"\x94\x00\x00\xa1x\x90");
            });
            assert_eq!(result, Err(Error::Timeout));
            raw.write_all(b"\x94\x01\x00\xc0\x63").await.unwrap();
            let (result, ()) = tokio::join!(empty_call(&peer, dynamic), async {
                let mut bytes = [0; 6];
                raw.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"\x94\x00\x01\xa1x\x90");
                raw.write_all(b"\x94\x01\x01\xc0\x2a").await.unwrap();
            });
            assert_eq!(result, Ok(Value::from(42)));
            connection.terminate();
            connection.wait().await.unwrap();
        };
        timeout(Duration::from_secs(3), scenario).await.unwrap();
    }
}

#[tokio::test]
async fn canceling_a_started_frame_finishes_it_but_skips_a_queued_call() {
    for dynamic in [false, true] {
        let scenario = async {
            let (stream, mut raw) = tokio::io::duplex(1);
            let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
            let first_peer = peer.clone();
            let first = tokio::spawn(async move {
                if dynamic {
                    first_peer
                        .request_value("x", Value::Array(vec!["a".repeat(128).into()]))
                        .await
                        .map(|_| ())
                } else {
                    first_peer.request::<_, ()>("x", ("a".repeat(128),)).await
                }
            });
            assert_eq!(raw.read_u8().await.unwrap(), 0x94);
            let mut second = Box::pin(empty_call(&peer, dynamic));
            tokio::select! { biased; result = &mut second => panic!("unexpected completion: {result:?}"), _ = std::future::ready(()) => {} }
            drop(second);
            first.abort();
            let _ = first.await;
            let (result, ()) = tokio::join!(empty_call(&peer, dynamic), async {
                let mut remaining = vec![0; 135];
                raw.read_exact(&mut remaining).await.unwrap();
                assert_eq!(&remaining[..7], b"\x00\x00\xa1x\x91\xd9\x80");
                assert!(remaining[7..].iter().all(|byte| *byte == b'a'));
                let mut next = [0; 6];
                raw.read_exact(&mut next).await.unwrap();
                assert_eq!(&next, b"\x94\x00\x02\xa1x\x90");
                raw.write_all(b"\x94\x01\x02\xc0\x2a").await.unwrap();
            });
            assert_eq!(result, Ok(Value::from(42)));
            connection.terminate();
            connection.wait().await.unwrap();
        };
        timeout(Duration::from_secs(3), scenario).await.unwrap();
    }
}
