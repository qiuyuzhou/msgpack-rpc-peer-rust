use msgpack_rpc_peer::{Config, Error, RpcPeer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, timeout},
};

#[tokio::test]
async fn invalid_arguments_release_pending_capacity_without_reusing_the_id() {
    timeout(Duration::from_secs(3), async {
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
            peer.request::<_, ()>("x", ScalarParams(7)).await,
            Err(Error::InvalidParams)
        );
        let (result, ()) = tokio::join!(peer.request::<_, u32>("x", ()), async {
            let mut bytes = [0; 6];
            raw.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"\x94\x00\x01\xa1x\x90");
            raw.write_all(b"\x94\x01\x01\xc0\x2a").await.unwrap();
        });
        assert_eq!(result, Ok(42));
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn timed_out_calls_ignore_late_responses_and_never_reuse_ids() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder()
            .config(Config {
                default_call_timeout: Duration::from_millis(10),
                ..Config::default()
            })
            .start(stream)
            .unwrap();
        let (result, ()) = tokio::join!(peer.request::<_, ()>("x", ()), async {
            let mut bytes = [0; 6];
            raw.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"\x94\x00\x00\xa1x\x90");
        });
        assert_eq!(result, Err(Error::Timeout));
        raw.write_all(b"\x94\x01\x00\xc0\x63").await.unwrap();
        let (result, ()) = tokio::join!(
            peer.request_with_timeout::<_, u32>("x", (), Duration::from_secs(1)),
            async {
                let mut bytes = [0; 6];
                raw.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"\x94\x00\x01\xa1x\x90");
                raw.write_all(b"\x94\x01\x01\xc0\x2a").await.unwrap();
            }
        );
        assert_eq!(result, Ok(42));
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn canceling_a_started_frame_finishes_it_but_skips_a_queued_call() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(1);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        let first_peer = peer.clone();
        let first = tokio::spawn(async move { first_peer.request::<_, ()>("x", ("a".repeat(128),)).await });
        assert_eq!(raw.read_u8().await.unwrap(), 0x94);
        let mut second = Box::pin(peer.request::<_, ()>("x", ()));
        tokio::select! { biased; result = &mut second => panic!("unexpected completion: {result:?}"), _ = std::future::ready(()) => {} }
        drop(second);
        first.abort();
        let _ = first.await;
        let (result, ()) = tokio::join!(peer.request::<_, u32>("x", ()), async {
            let mut remaining = vec![0; 135];
            raw.read_exact(&mut remaining).await.unwrap();
            assert_eq!(&remaining[..7], b"\x00\x00\xa1x\x91\xd9\x80");
            assert!(remaining[7..].iter().all(|byte| *byte == b'a'));
            let mut next = [0; 6];
            raw.read_exact(&mut next).await.unwrap();
            assert_eq!(&next, b"\x94\x00\x02\xa1x\x90");
            raw.write_all(b"\x94\x01\x02\xc0\x2a").await.unwrap();
        });
        assert_eq!(result, Ok(42));
        connection.terminate(); connection.wait().await.unwrap();
    }).await.unwrap();
}
