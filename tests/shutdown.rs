use msgpack_rpc_peer::{Error, RpcPeer};
use tokio::{
    io::AsyncReadExt,
    time::{Duration, timeout},
};

#[tokio::test]
async fn graceful_shutdown_flushes_an_inflight_notification_and_refuses_new_application_work() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(1);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        peer.notify("event", ()).await.unwrap();
        assert_eq!(raw.read_u8().await.unwrap(), 0x93);
        peer.notify("queued", ()).await.unwrap();
        let mut shutting_down = Box::pin(connection.shutdown());
        tokio::select! { biased; result = &mut shutting_down => panic!("premature shutdown {result:?}"), _ = std::future::ready(()) => {} }
        assert_eq!(peer.notify("new", ()).await, Err(Error::Draining));
        assert_eq!(peer.request::<_, ()>("new", ()).await, Err(Error::Draining));
        let mut rest = [0;8]; raw.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"\x02\xa5event\x90");
        let mut queued = [0;10]; raw.read_exact(&mut queued).await.unwrap();
        assert_eq!(&queued,b"\x93\x02\xa6queued\x90");
        assert_eq!(shutting_down.await, Ok(()));
        assert!(raw.read_u8().await.is_err());
    }).await.unwrap();
}

#[tokio::test]
async fn deadline_cancels_handlers_and_pending_calls_and_waits_for_cleanup() {
    use std::sync::{Arc, Mutex};
    use tokio::{io::AsyncWriteExt, sync::oneshot};
    timeout(Duration::from_secs(3), async {
        struct Dropped(Option<oneshot::Sender<()>>);
        impl Drop for Dropped { fn drop(&mut self) { let _ = self.0.take().unwrap().send(()); } }
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx,dropped_rx) = oneshot::channel();
        let signals = Arc::new(Mutex::new(Some((started_tx,dropped_tx))));
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer,connection) = RpcPeer::builder().method("wait", move |_, (): ()| {
            let (started,dropped) = signals.lock().unwrap().take().unwrap();
            async move { let _dropped = Dropped(Some(dropped)); started.send(()).unwrap(); std::future::pending::<Result<(),msgpack_rpc_peer::RemoteError>>().await }
        }).unwrap().start(stream).unwrap();
        raw.write_all(b"\x94\x00\x00\xa4wait\x90").await.unwrap(); started_rx.await.unwrap();
        let mut call = Box::pin(peer.request::<_, ()>("remote", ()));
        tokio::select! { biased; result = &mut call => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        assert_eq!(connection.shutdown_with_timeout(Duration::from_millis(10)).await, Err(Error::ShutdownTimeout));
        assert_eq!(call.await, Err(Error::ShutdownTimeout));
        dropped_rx.await.unwrap();
        assert_eq!(peer.notify("x", ()).await, Err(Error::ShutdownTimeout));
    }).await.unwrap();
}

#[tokio::test]
async fn notifications_wait_for_a_queue_reservation_before_serializing() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Observed(Arc<AtomicUsize>);
    impl serde::Serialize for Observed {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            serializer.serialize_unit()
        }
    }
    timeout(Duration::from_secs(3), async {
        let (stream,mut raw) = tokio::io::duplex(1);
        let (peer,connection) = RpcPeer::builder().config(msgpack_rpc_peer::Config { application_queue_capacity:1, ..Default::default() }).start(stream).unwrap();
        peer.notify("first", ()).await.unwrap(); assert_eq!(raw.read_u8().await.unwrap(),0x93);
        peer.notify("second", ()).await.unwrap();
        let serialized = Arc::new(AtomicUsize::new(0));
        let mut waiting = Box::pin(peer.notify("third", Observed(serialized.clone())));
        tokio::select! { biased; result = &mut waiting => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        assert_eq!(serialized.load(Ordering::Relaxed),0);
        connection.terminate(); connection.wait().await.unwrap();
        assert_eq!(waiting.await,Err(Error::Closed));
        assert_eq!(serialized.load(Ordering::Relaxed),0);
    }).await.unwrap();
}

#[tokio::test]
async fn admitted_handlers_can_reverse_call_while_draining() {
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    timeout(Duration::from_secs(3), async {
        let (started_tx,started_rx) = oneshot::channel();
        let (release_tx,release_rx) = oneshot::channel();
        let signals = Arc::new(Mutex::new(Some((started_tx,release_rx))));
        let (left,right) = tokio::io::duplex(1024);
        let (a,ca) = RpcPeer::builder().method("leaf", |_, (): ()| async { Ok(42u32) }).unwrap().start(left).unwrap();
        let (b,cb) = RpcPeer::builder().method("outer", move |context, (): ()| {
            let (started,release) = signals.lock().unwrap().take().unwrap();
            async move { started.send(()).unwrap(); release.await.unwrap(); Ok(context.request::<_,u32>("leaf",()).await.unwrap()) }
        }).unwrap().start(right).unwrap();
        let calling = tokio::spawn(async move { a.request::<_,u32>("outer",()).await });
        started_rx.await.unwrap();
        let mut draining = Box::pin(cb.shutdown());
        tokio::select! { biased; result = &mut draining => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        assert_eq!(b.request::<_, ()>("leaf",()).await,Err(Error::Draining));
        release_tx.send(()).unwrap();
        assert_eq!(calling.await.unwrap(),Ok(42));
        assert_eq!(draining.await,Ok(()));
        ca.wait().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_a_call_admitted_before_pending_capacity_was_available() {
    use tokio::io::AsyncWriteExt;
    timeout(Duration::from_secs(3), async {
        let (stream,mut raw) = tokio::io::duplex(64);
        let (peer,connection) = RpcPeer::builder().config(msgpack_rpc_peer::Config {max_pending_calls:1,..Default::default()}).start(stream).unwrap();
        let mut first = Box::pin(peer.request::<_,u32>("x",()));
        tokio::select! { biased; result = &mut first => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        let mut bytes = [0;6]; raw.read_exact(&mut bytes).await.unwrap();
        let mut second = Box::pin(peer.request::<_,u32>("x",()));
        tokio::select! { biased; result = &mut second => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        let mut draining = Box::pin(connection.shutdown());
        tokio::select! { biased; result = &mut draining => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        let (first,second,completion,()) = tokio::join!(first,second,draining, async {
            raw.write_all(b"\x94\x01\x00\xc0\x14").await.unwrap();
            raw.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes,b"\x94\x00\x01\xa1x\x90");
            raw.write_all(b"\x94\x01\x01\xc0\x16").await.unwrap();
        });
        assert_eq!((first,second,completion),(Ok(20),Ok(22),Ok(())));
    }).await.unwrap();
}

#[tokio::test]
async fn escaped_context_loses_drain_admission_when_its_handler_completes() {
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;
    timeout(Duration::from_secs(3), async {
        let (context_tx,context_rx) = oneshot::channel();
        let context_tx = Arc::new(Mutex::new(Some(context_tx)));
        let (started_tx,started_rx) = oneshot::channel();
        let (release_tx,release_rx) = oneshot::channel();
        let signals = Arc::new(Mutex::new(Some((started_tx,release_rx))));
        let (left,right) = tokio::io::duplex(1024);
        let (a,ca) = RpcPeer::builder().start(left).unwrap();
        let (_b,cb) = RpcPeer::builder()
            .method("save", move |context, (): ()| { assert!(context_tx.lock().unwrap().take().unwrap().send(context).is_ok()); async { Ok(()) } }).unwrap()
            .method("hold", move |_, (): ()| { let (started,release) = signals.lock().unwrap().take().unwrap(); async move { started.send(()).unwrap(); release.await.unwrap(); Ok(()) } }).unwrap()
            .start(right).unwrap();
        a.request::<_, ()>("save",()).await.unwrap(); let escaped = context_rx.await.unwrap();
        let calling = tokio::spawn(async move { a.request::<_, ()>("hold",()).await });
        started_rx.await.unwrap();
        let mut draining = Box::pin(cb.shutdown());
        tokio::select! { biased; result = &mut draining => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        assert_eq!(escaped.request::<_, ()>("leaf",()).await,Err(Error::Draining));
        assert_eq!(escaped.notify("event",()).await,Err(Error::Draining));
        release_tx.send(()).unwrap();
        assert_eq!(calling.await.unwrap(),Ok(())); assert_eq!(draining.await,Ok(()));
        ca.wait().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn sustained_incoming_requests_do_not_extend_the_shutdown_deadline() {
    use std::sync::{Arc, Mutex};
    use tokio::{io::AsyncWriteExt, sync::oneshot};
    timeout(Duration::from_secs(3), async {
        let (started_tx,started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let (stream,mut raw) = tokio::io::duplex(1024);
        let (_peer,connection) = RpcPeer::builder()
            .method("hold",move |_, (): ()| { started_tx.lock().unwrap().take().unwrap().send(()).unwrap(); async { std::future::pending::<Result<(),msgpack_rpc_peer::RemoteError>>().await } }).unwrap()
            .method("ping",|_, (): ()| async { Ok(()) }).unwrap()
            .start(stream).unwrap();
        raw.write_all(b"\x94\x00\x00\xa4hold\x90").await.unwrap(); started_rx.await.unwrap();
        let mut draining = Box::pin(connection.shutdown_with_timeout(Duration::from_millis(10)));
        tokio::select! { biased; result = &mut draining => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        let (completion,served) = tokio::join!(draining,async {
            let mut served = 0;
            loop {
                if raw.write_all(b"\x94\x00\x01\xa4ping\x90").await.is_err() { break; }
                let mut reply = [0;5];
                if raw.read_exact(&mut reply).await.is_err() { break; }
                assert_eq!(&reply,b"\x94\x01\x01\xc0\xc0"); served += 1;
            }
            served
        });
        assert_eq!(completion,Err(Error::ShutdownTimeout));
        assert!(served > 0);
    }).await.unwrap();
}

#[tokio::test]
async fn eof_during_drain_fails_pending_work_and_completion_observes_transport_release() {
    timeout(Duration::from_secs(3), async {
        let (stream,mut raw) = tokio::io::duplex(64);
        let (peer,connection) = RpcPeer::builder().start(stream).unwrap();
        let mut calling = Box::pin(peer.request::<_, ()>("x",()));
        tokio::select! { biased; result = &mut calling => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        let mut request = [0;6]; raw.read_exact(&mut request).await.unwrap();
        let mut draining = Box::pin(connection.shutdown());
        tokio::select! { biased; result = &mut draining => panic!("unexpected {result:?}"), _ = std::future::ready(()) => {} }
        drop(raw);
        assert_eq!(draining.await,Ok(()));
        assert_eq!(calling.await,Err(Error::Closed));
    }).await.unwrap();
}

#[tokio::test]
async fn invalid_shutdown_deadlines_fail_without_panicking() {
    for deadline in [Duration::ZERO, Duration::MAX] {
        let config = msgpack_rpc_peer::Config {
            graceful_shutdown_timeout: deadline,
            ..Default::default()
        };
        let (stream, _raw) = tokio::io::duplex(64);
        assert!(matches!(
            RpcPeer::builder().config(config).start(stream),
            Err(Error::InvalidConfig(_))
        ));
    }
    let (stream, _raw) = tokio::io::duplex(64);
    let (_peer, connection) = RpcPeer::builder().start(stream).unwrap();
    assert!(matches!(
        connection.shutdown_with_timeout(Duration::MAX).await,
        Err(Error::InvalidConfig(_))
    ));
}

async fn termination_during_serialization(request: bool) {
    use std::sync::Mutex;
    struct PausedSerialization {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl serde::Serialize for PausedSerialization {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.started
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.release.recv().unwrap();
            serializer.serialize_unit()
        }
    }
    timeout(Duration::from_secs(3), async {
        let (stream, _raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let sending = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let args = PausedSerialization {
                        started: Mutex::new(Some(started_tx)),
                        release: release_rx,
                    };
                    if request {
                        peer.request::<_, ()>("x", (args,)).await
                    } else {
                        peer.notify("x", args).await
                    }
                })
        });
        started_rx.await.unwrap();
        connection.terminate();
        connection.wait().await.unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(sending.join().unwrap(), Err(Error::Closed));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_notification_does_not_commit_after_terminal_cleanup() {
    termination_during_serialization(false).await;
}

#[tokio::test]
async fn termination_during_request_serialization_preserves_terminal_error() {
    termination_during_serialization(true).await;
}

#[tokio::test]
async fn responses_progress_during_continuous_application_notifications() {
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(1024 * 1024);
        let (peer, owner) = RpcPeer::builder().start(left).unwrap();
        let (_, remote_owner) = RpcPeer::builder()
            .method("x", |_, (): ()| async { Ok(1u32) })
            .unwrap()
            .start(right)
            .unwrap();
        let producer_peer = peer.clone();
        let producer =
            tokio::spawn(async move { while producer_peer.notify("n", ()).await.is_ok() {} });
        let result = peer
            .request_with_timeout::<_, u32>("x", (), Duration::from_millis(100))
            .await;
        producer.abort();
        let _ = producer.await;
        owner.terminate();
        owner.wait().await.unwrap();
        remote_owner.terminate();
        remote_owner.wait().await.unwrap();
        assert_eq!(result, Ok(1));
    })
    .await
    .unwrap();
}
