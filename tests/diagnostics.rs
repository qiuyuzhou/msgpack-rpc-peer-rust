use msgpack_rpc_peer::{RemoteError, RpcPeer, Value};
use tokio::{
    io::AsyncWriteExt,
    time::{Duration, timeout},
};

#[tokio::test]
async fn notification_failure_is_counted_without_observation_or_sensitive_output() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(4096);
        let (peer, mut owner) = RpcPeer::builder()
            .notification("secret_method", |_, (_secret,): (String,)| async {
                Err(RemoteError::new(Value::from("secret_payload")).unwrap())
            })
            .unwrap()
            .start(stream)
            .unwrap();
        assert!(owner.take_events().is_none());
        raw.write_all(b"\x93\x02\xadsecret_method\x91\xaesecret_payload")
            .await
            .unwrap();
        while peer.diagnostics().notification_failures == 0 {
            tokio::task::yield_now().await;
        }
        let snapshot = peer.diagnostics();
        assert_eq!(snapshot.notification_failures, 1);
        assert_eq!(snapshot.lost_events, 0);
        let output = format!("{snapshot:?}");
        assert!(!output.contains("secret_method"));
        assert!(!output.contains("secret_payload"));
        owner.terminate();
        owner.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(panic = "unwind")]
async fn panic_later() -> Result<(), RemoteError> {
    tokio::task::yield_now().await;
    panic!("private panic payload");
}

#[cfg(panic = "unwind")]
#[tokio::test]
async fn invocation_and_future_panics_are_isolated_for_both_handler_kinds() {
    use msgpack_rpc_peer::{DiagnosticEvent, Error};
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(4096);
        let (caller, caller_owner) = RpcPeer::builder().start(left).unwrap();
        let (observed, mut owner) = RpcPeer::builder()
            .diagnostic_events(true)
            .method(
                "private_request_invoke",
                |_, (): ()| -> std::future::Ready<Result<(), RemoteError>> {
                    panic!("private panic payload")
                },
            )
            .unwrap()
            .method("private_request_future", |_, (): ()| panic_later())
            .unwrap()
            .notification(
                "private_notification_invoke",
                |_, (): ()| -> std::future::Ready<Result<(), RemoteError>> {
                    panic!("private panic payload")
                },
            )
            .unwrap()
            .notification("private_notification_future", |_, (): ()| panic_later())
            .unwrap()
            .method("ok", |_, (): ()| async { Ok(7u32) })
            .unwrap()
            .start(right)
            .unwrap();
        let mut events = owner.take_events().unwrap();
        assert!(owner.take_events().is_none());
        for method in ["private_request_invoke", "private_request_future"] {
            let error = caller.request::<_, ()>(method, ()).await.unwrap_err();
            let Error::Remote(value) = error else {
                panic!("expected remote error")
            };
            let expected = Value::Map(vec![
                ("code".into(), (-32603).into()),
                ("message".into(), "handler failed".into()),
            ]);
            assert_eq!(value, expected);
            assert_eq!(events.recv().await, Some(DiagnosticEvent::HandlerPanicked));
            assert_eq!(caller.request::<_, u32>("ok", ()).await.unwrap(), 7);
        }
        for method in ["private_notification_invoke", "private_notification_future"] {
            caller.notify(method, ()).await.unwrap();
            assert_eq!(events.recv().await, Some(DiagnosticEvent::HandlerPanicked));
            assert_eq!(caller.request::<_, u32>("ok", ()).await.unwrap(), 7);
        }
        assert_eq!(observed.diagnostics().handler_panics, 4);
        assert_eq!(observed.diagnostics().notification_failures, 0);
        owner.terminate();
        owner.wait().await.unwrap();
        assert_eq!(events.recv().await, None);
        caller_owner.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn event_backlog_is_bounded_and_lost_events_are_counted() {
    use msgpack_rpc_peer::{DIAGNOSTIC_EVENT_CAPACITY, DiagnosticEvent};
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(4096);
        let (peer, mut owner) = RpcPeer::builder()
            .diagnostic_events(true)
            .notification("n", |_, (): ()| async {
                Err(RemoteError::new("private error".into()).unwrap())
            })
            .unwrap()
            .start(stream)
            .unwrap();
        let mut events = owner.take_events().unwrap();
        for n in 1..=65 {
            raw.write_all(b"\x93\x02\xa1n\x90").await.unwrap();
            while peer.diagnostics().notification_failures < n {
                tokio::task::yield_now().await;
            }
        }
        while peer.diagnostics().lost_events < 1 {
            tokio::task::yield_now().await;
        }
        assert_eq!(peer.diagnostics().lost_events, 1);
        for _ in 0..DIAGNOSTIC_EVENT_CAPACITY {
            assert_eq!(
                events.recv().await,
                Some(DiagnosticEvent::NotificationFailed)
            );
        }
        drop(events);
        raw.write_all(b"\x93\x02\xa1n\x90").await.unwrap();
        while peer.diagnostics().lost_events < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(peer.diagnostics().notification_failures, 66);
        owner.terminate();
        owner.wait().await.unwrap();
    })
    .await
    .unwrap();
}
