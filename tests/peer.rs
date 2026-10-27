use msgpack_rpc_peer::{Error, RemoteError, RpcParams, RpcPeer};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn typed_requests_work_in_both_directions() {
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(128);
        let (a, a_connection) = RpcPeer::builder()
            .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })
            .unwrap()
            .start(left)
            .unwrap();
        let (b, b_connection) = RpcPeer::builder()
            .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })
            .unwrap()
            .start(right)
            .unwrap();
        let b_clone = b.clone();
        let (sum, doubled) = tokio::join!(
            a.request::<_, u32>("sum", (2, 3)),
            b_clone.request::<_, u32>("double", (7,)),
        );
        assert_eq!(sum.unwrap(), 5);
        assert_eq!(doubled.unwrap(), 14);
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn result_type_mismatch_leaves_the_connection_usable() {
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(128);
        let (peer, owner) = RpcPeer::builder().start(left).unwrap();
        let (_, remote) = RpcPeer::builder()
            .method("answer", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(right)
            .unwrap();
        assert_eq!(
            peer.request::<_, String>("answer", ()).await,
            Err(Error::InvalidResult)
        );
        assert_eq!(peer.request::<_, u32>("answer", ()).await, Ok(42));
        owner.terminate();
        owner.wait().await.unwrap();
        remote.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn custom_params_with_invalid_shapes_are_rejected_at_runtime() {
    #[derive(serde::Serialize)]
    struct ScalarParams(u32);
    impl RpcParams for ScalarParams {}

    #[derive(serde::Serialize)]
    struct MapParams {
        n: u32,
    }
    impl RpcParams for MapParams {}

    let (stream, _raw) = tokio::io::duplex(128);
    let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
    assert_eq!(
        peer.request::<_, ()>("method", ScalarParams(7)).await,
        Err(Error::InvalidParams)
    );
    assert_eq!(
        peer.request::<_, ()>("method", MapParams { n: 7 }).await,
        Err(Error::InvalidParams)
    );
    assert_eq!(
        peer.notify("method", ScalarParams(7)).await,
        Err(Error::InvalidParams)
    );
    assert_eq!(
        peer.notify("method", MapParams { n: 7 }).await,
        Err(Error::InvalidParams)
    );
    connection.terminate();
    connection.wait().await.unwrap();
}

#[tokio::test]
async fn handler_can_await_a_reverse_request() {
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(64);
        let (a, a_connection) = RpcPeer::builder()
            .method("answer", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(left)
            .unwrap();
        let (_b, b_connection) = RpcPeer::builder()
            .method("reverse", |context, (): ()| async move {
                let answer: u32 = context
                    .request("answer", ())
                    .await
                    .map_err(|_| RemoteError::new("reverse failed".into()).unwrap())?;
                Ok(answer + 1)
            })
            .unwrap()
            .start(right)
            .unwrap();
        assert_eq!(a.request::<_, u32>("reverse", ()).await.unwrap(), 43);
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn handler_panics_and_application_errors_do_not_strand_the_connection() {
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(128);
        let (a, a_connection) = RpcPeer::builder().start(left).unwrap();
        let (_b, b_connection) = RpcPeer::builder()
            .method::<(), (), _, _>("panic", |_, ()| async {
                panic!("private application panic payload");
            })
            .unwrap()
            .method("error", |_, (): ()| async {
                Err::<(), _>(RemoteError::new("application failure".into()).unwrap())
            })
            .unwrap()
            .method("ok", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(right)
            .unwrap();
        let Err(Error::Remote(error)) = a.request::<_, ()>("panic", ()).await else {
            panic!("expected a remote failure");
        };
        assert_eq!(
            error,
            msgpack_rpc_peer::Value::Map(vec![
                ("code".into(), (-32603).into()),
                ("message".into(), "handler failed".into()),
            ])
        );
        assert_eq!(
            a.request::<_, ()>("error", ()).await,
            Err(Error::Remote("application failure".into()))
        );
        assert_eq!(a.request::<_, u32>("ok", ()).await.unwrap(), 42);
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn handlers_require_explicit_argument_lists() {
    #[derive(serde::Deserialize)]
    struct Bare {
        n: u32,
    }
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(128);
        let (a, a_connection) = RpcPeer::builder().start(left).unwrap();
        let (_b, b_connection) = RpcPeer::builder()
            .method("struct", |_, (bare,): (Bare,)| async move { Ok(bare.n) })
            .unwrap()
            .method(
                "option",
                |_, (value,): (Option<u32>,)| async move { Ok(value) },
            )
            .unwrap()
            .method("vec", |_, params: Vec<u32>| async move { Ok(params) })
            .unwrap()
            .method("array", |_, params: [u32; 2]| async move { Ok(params) })
            .unwrap()
            .start(right)
            .unwrap();
        let invalid = Error::Remote(msgpack_rpc_peer::Value::Map(vec![
            ("code".into(), (-32602).into()),
            ("message".into(), "invalid parameters".into()),
        ]));
        assert_eq!(
            a.request::<_, u32>("struct", (7,)).await,
            Err(invalid.clone())
        );
        assert_eq!(
            a.request::<_, Option<u32>>("option", ()).await,
            Err(invalid.clone())
        );
        assert_eq!(
            a.request::<_, u32>("struct", (std::collections::BTreeMap::from([("n", 7)]),))
                .await
                .unwrap(),
            7
        );
        assert_eq!(
            a.request::<_, Option<u32>>("option", (Some(7),))
                .await
                .unwrap(),
            Some(7)
        );
        assert_eq!(
            a.request::<_, Vec<u32>>("vec", (1, 2, 3)).await.unwrap(),
            vec![1, 2, 3]
        );
        assert_eq!(
            a.request::<_, [u32; 2]>("array", (1, 2)).await.unwrap(),
            [1, 2]
        );
        assert_eq!(a.request::<_, [u32; 2]>("array", (1,)).await, Err(invalid));
        a_connection.terminate();
        a_connection.wait().await.unwrap();
        b_connection.terminate();
        b_connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_notification_names_fail_and_closed_peers_reject_notifications() {
    assert!(matches!(
        RpcPeer::builder()
            .notification("event", |_, (): ()| async { Ok(()) })
            .unwrap()
            .notification("event", |_, (): ()| async { Ok(()) }),
        Err(Error::DuplicateMethod)
    ));
    let (stream, _raw) = tokio::io::duplex(128);
    let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
    connection.terminate();
    connection.wait().await.unwrap();
    assert_eq!(peer.notify("event", ()).await, Err(Error::Closed));
}

#[tokio::test]
async fn typed_enums_decode_inside_argument_lists_and_results() {
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    enum Choice {
        Unit,
        Newtype(u32),
        Tuple(u32, bool),
        Struct { n: u32 },
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(256);
        let (peer, owner) = RpcPeer::builder().start(left).unwrap();
        let (_, remote) = RpcPeer::builder()
            .method("echo", |_, (value,): (Choice,)| async move { Ok(value) })
            .unwrap()
            .start(right)
            .unwrap();
        for value in [
            Choice::Unit,
            Choice::Newtype(4),
            Choice::Tuple(2, true),
            Choice::Struct { n: 9 },
        ] {
            let result: Choice = peer.request("echo", (value.clone(),)).await.unwrap();
            assert_eq!(result, value);
        }
        owner.terminate();
        owner.wait().await.unwrap();
        remote.wait().await.unwrap();
    })
    .await
    .unwrap();
}
