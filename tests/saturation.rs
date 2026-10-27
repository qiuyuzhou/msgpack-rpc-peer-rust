use msgpack_rpc_peer::{Config, Error, RpcPeer};
use tokio::{
    io::AsyncReadExt,
    time::{Duration, timeout},
};

#[tokio::test]
async fn timeout_and_abort_release_pending_capacity_and_termination_wakes_waiters() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder().config(Config { max_pending_calls: 1, ..Config::default() }).start(stream).unwrap();
        let first_peer = peer.clone();
        let first = tokio::spawn(async move { first_peer.request::<_, ()>("x", ()).await });
        let mut bytes = [0; 6]; raw.read_exact(&mut bytes).await.unwrap();
        assert_eq!(peer.request_with_timeout::<_, ()>("x", (), Duration::from_millis(10)).await, Err(Error::Timeout));
        first.abort(); let _ = first.await;
        let second_peer = peer.clone();
        let second = tokio::spawn(async move { second_peer.request::<_, ()>("x", ()).await });
        raw.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"\x94\x00\x01\xa1x\x90");
        let mut waiting = Box::pin(peer.request::<_, ()>("x", ()));
        tokio::select! { biased; result = &mut waiting => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        connection.terminate(); connection.wait().await.unwrap();
        assert_eq!(waiting.await, Err(Error::Closed));
        assert_eq!(second.await.unwrap(), Err(Error::Closed));
    }).await.unwrap();
}

async fn read_value(raw: &mut tokio::io::DuplexStream) -> msgpack_rpc_peer::Value {
    let mut bytes = Vec::new();
    loop {
        bytes.push(raw.read_u8().await.unwrap());
        if let Ok(value) = rmpv::decode::read_value(&mut bytes.as_slice()) {
            return value;
        }
        assert!(bytes.len() < 1024);
    }
}

#[tokio::test]
async fn one_shared_handler_slot_rejects_requests_and_counts_dropped_notifications() {
    use std::sync::Arc;
    use tokio::{io::AsyncWriteExt, sync::Notify};
    timeout(Duration::from_secs(3), async {
        let entered = Arc::new(Notify::new());
        let signal = entered.clone();
        let notification_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran = notification_ran.clone();
        let (stream, mut raw) = tokio::io::duplex(1024);
        let (peer, connection) = RpcPeer::builder()
            .config(Config {
                max_handlers: 1,
                ..Config::default()
            })
            .method("x", move |_, (): ()| {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    std::future::pending::<Result<(), msgpack_rpc_peer::RemoteError>>().await
                }
            })
            .unwrap()
            .notification("n", move |_, (): ()| {
                let ran = ran.clone();
                async move {
                    ran.store(true, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                }
            })
            .unwrap()
            .start(stream)
            .unwrap();
        raw.write_all(b"\x94\x00\x00\xa1x\x90").await.unwrap();
        entered.notified().await;
        raw.write_all(b"\x93\x02\xa1n\x90\x94\x00\x01\xa1x\x90")
            .await
            .unwrap();
        let response = read_value(&mut raw).await;
        let fields = response.as_array().unwrap();
        assert_eq!(fields[0], 1.into());
        assert_eq!(fields[1], 1.into());
        assert!(fields[3].is_nil());
        assert_eq!(fields[2].as_map().unwrap()[0].1, (-32000).into());
        assert_eq!(peer.dropped_notifications(), 1);
        assert!(!notification_ran.load(std::sync::atomic::Ordering::Relaxed));
        let (result, ()) = tokio::join!(peer.request::<_, u32>("remote", ()), async {
            let request = read_value(&mut raw).await;
            let id = request.as_array().unwrap()[1].as_u64().unwrap() as u8;
            raw.write_all(&[0x94, 1, id, 0xc0, 42]).await.unwrap();
        });
        assert_eq!(result, Ok(42));
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn whole_call_timeout_covers_a_full_application_queue() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(1);
        let (peer, connection) = RpcPeer::builder().config(Config { application_queue_capacity: 1, ..Config::default() }).start(stream).unwrap();
        peer.notify("n", ("a".repeat(128),)).await.unwrap();
        assert_eq!(raw.read_u8().await.unwrap(), 0x93);
        peer.notify("n", ()).await.unwrap();
        assert_eq!(peer.request_with_timeout::<_, ()>("x", (), Duration::from_millis(10)).await, Err(Error::Timeout));
        let mut waiting = Box::pin(peer.notify("n", ()));
        tokio::select! { biased; result = &mut waiting => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        connection.terminate(); connection.wait().await.unwrap();
        assert_eq!(waiting.await, Err(Error::Closed));
    }).await.unwrap();
}

#[tokio::test]
async fn a_full_response_queue_terminates_instead_of_blocking_the_reader() {
    use tokio::io::AsyncWriteExt;
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(1024);
        let (peer, connection) = RpcPeer::builder().config(Config { response_queue_capacity: 1, ..Config::default() }).start(stream).unwrap();
        let mut pending = Box::pin(peer.request::<_, ()>("x", ()));
        tokio::select! { biased; result = &mut pending => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        // Responses are larger than this unread transport's capacity in aggregate.
        for id in 0..100u8 {
            if raw.write_all(&[0x94,0,id,0xa1,b'x',0x90]).await.is_err() { break; }
        }
        let completion = connection.wait().await;
        assert_eq!(completion, Err(Error::ResourceLimit("response queue")));
        assert_eq!(pending.await, completion);
    }).await.unwrap();
}

#[tokio::test]
async fn saturated_peers_reverse_calls_fail_with_overload_without_deadlock() {
    use std::sync::Arc;
    use tokio::sync::Barrier;
    timeout(Duration::from_secs(3), async {
        let barrier = Arc::new(Barrier::new(2));
        let builder = |barrier: Arc<Barrier>| {
            RpcPeer::builder()
                .config(Config {
                    max_handlers: 1,
                    ..Config::default()
                })
                .method("outer", move |context, (): ()| {
                    let barrier = barrier.clone();
                    async move {
                        barrier.wait().await;
                        context
                            .request::<_, ()>("leaf", ())
                            .await
                            .map_err(|error| match error {
                                Error::Remote(value) => {
                                    msgpack_rpc_peer::RemoteError::new(value).unwrap()
                                }
                                other => panic!("unexpected {other:?}"),
                            })
                    }
                })
                .unwrap()
                .method("leaf", |_, (): ()| async { Ok(()) })
                .unwrap()
        };
        let (left, right) = tokio::io::duplex(1024);
        let (a, ca) = builder(barrier.clone()).start(left).unwrap();
        let (b, cb) = builder(barrier).start(right).unwrap();
        let (a_result, b_result) = tokio::join!(
            a.request::<_, ()>("outer", ()),
            b.request::<_, ()>("outer", ())
        );
        for result in [a_result, b_result] {
            let Err(Error::Remote(value)) = result else {
                panic!("expected overload")
            };
            assert_eq!(value.as_map().unwrap()[0].1, (-32000).into());
        }
        ca.terminate();
        cb.terminate();
        ca.wait().await.unwrap();
        cb.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_capacity_configuration_is_rejected_at_startup() {
    for field in 0..4 {
        for bad in [0, usize::MAX] {
            let mut config = Config::default();
            match field {
                0 => config.max_pending_calls = bad,
                1 => config.max_handlers = bad,
                2 => config.application_queue_capacity = bad,
                _ => config.response_queue_capacity = bad,
            }
            let (stream, _raw) = tokio::io::duplex(64);
            assert!(matches!(
                RpcPeer::builder().config(config).start(stream),
                Err(Error::InvalidConfig(_))
            ));
        }
    }
    let (stream, _raw) = tokio::io::duplex(64);
    assert!(matches!(
        RpcPeer::builder()
            .config(Config {
                default_call_timeout: Duration::ZERO,
                ..Config::default()
            })
            .start(stream),
        Err(Error::InvalidConfig(_))
    ));
}
