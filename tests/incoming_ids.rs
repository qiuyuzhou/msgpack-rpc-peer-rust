use std::sync::Arc;

use msgpack_rpc_peer::{Config, Connection, Error, RpcPeer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::Notify,
    task::JoinHandle,
    time::{Duration, timeout},
};

async fn pending_call(peer: &RpcPeer, raw: &mut DuplexStream) -> JoinHandle<Result<(), Error>> {
    let peer = peer.clone();
    let pending = tokio::spawn(async move { peer.request("p", ()).await });
    let mut request = [0; 6];
    raw.read_exact(&mut request).await.unwrap();
    assert_eq!(&request, b"\x94\x00\x00\xa1p\x90");
    pending
}

async fn duplicate_failure(connection: Connection, pending: JoinHandle<Result<(), Error>>) {
    let expected = Err(Error::Protocol("duplicate active request ID"));
    assert_eq!(connection.wait().await, expected);
    assert_eq!(pending.await.unwrap(), expected);
}

#[tokio::test]
async fn duplicate_ids_are_fatal_while_a_handler_is_running() {
    timeout(Duration::from_secs(3), async {
        let started = Arc::new(Notify::new());
        let signal = started.clone();
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder()
            .method("x", move |_, (): ()| {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    std::future::pending::<Result<(), msgpack_rpc_peer::RemoteError>>().await
                }
            })
            .unwrap()
            .start(stream)
            .unwrap();
        let pending = pending_call(&peer, &mut raw).await;
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        started.notified().await;
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        duplicate_failure(connection, pending).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_unknown_ids_are_fatal_while_the_response_is_queued() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(1);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        let pending = pending_call(&peer, &mut raw).await;
        peer.notify("block", ()).await.unwrap();
        assert_eq!(raw.read_u8().await.unwrap(), 0x93);
        // The writer cannot finish its notification, so neither response can start.
        raw.write_all(b"\x94\x00\x2a\xa1x\x90\x94\x00\x2a\xa1x\x90")
            .await
            .unwrap();
        duplicate_failure(connection, pending).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_ids_are_fatal_after_the_handler_finishes_but_before_the_response_finishes() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(1);
        let (peer, connection) = RpcPeer::builder()
            .method("x", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(stream)
            .unwrap();
        let pending = pending_call(&peer, &mut raw).await;
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        assert_eq!(raw.read_u8().await.unwrap(), 0x94);
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        duplicate_failure(connection, pending).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_overloaded_ids_are_fatal_before_the_error_response_finishes() {
    timeout(Duration::from_secs(3), async {
        let started = Arc::new(Notify::new());
        let signal = started.clone();
        let (stream, mut raw) = tokio::io::duplex(1);
        let (peer, connection) = RpcPeer::builder()
            .config(Config {
                max_handlers: 1,
                ..Config::default()
            })
            .method("hold", move |_, (): ()| {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    std::future::pending::<Result<(), msgpack_rpc_peer::RemoteError>>().await
                }
            })
            .unwrap()
            .method("x", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(stream)
            .unwrap();
        let pending = pending_call(&peer, &mut raw).await;
        raw.write_all(b"\x94\x00\x01\xa4hold\x90").await.unwrap();
        started.notified().await;
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        assert_eq!(raw.read_u8().await.unwrap(), 0x94);
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        duplicate_failure(connection, pending).await;
    })
    .await
    .unwrap();
}

struct FlushControl {
    held: std::sync::atomic::AtomicBool,
    entered: Notify,
    completed: Notify,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}
impl FlushControl {
    fn new(held: bool) -> Arc<Self> {
        Arc::new(Self {
            held: std::sync::atomic::AtomicBool::new(held),
            entered: Notify::new(),
            completed: Notify::new(),
            waker: std::sync::Mutex::new(None),
        })
    }
    fn release(&self) {
        self.held.store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}
struct ControlledFlush {
    stream: DuplexStream,
    control: Arc<FlushControl>,
}
impl tokio::io::AsyncRead for ControlledFlush {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_read(cx, buffer)
    }
}
impl tokio::io::AsyncWrite for ControlledFlush {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_write(cx, bytes)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.control.held.load(std::sync::atomic::Ordering::SeqCst) {
            *this.control.waker.lock().unwrap() = Some(cx.waker().clone());
            this.control.entered.notify_one();
            return std::task::Poll::Pending;
        }
        let result = std::pin::Pin::new(&mut this.stream).poll_flush(cx);
        if result.is_ready() {
            this.control.completed.notify_one();
        }
        result
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn incoming_ids_remain_active_until_response_flush_completes() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let control = FlushControl::new(false);
        let (peer, connection) = RpcPeer::builder()
            .method("x", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(ControlledFlush {
                stream,
                control: control.clone(),
            })
            .unwrap();
        let pending = pending_call(&peer, &mut raw).await;
        control.completed.notified().await;
        control
            .held
            .store(true, std::sync::atomic::Ordering::SeqCst);
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        let mut response = [0; 5];
        raw.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"\x94\x01\x2a\xc0\x2a");
        control.entered.notified().await;
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        duplicate_failure(connection, pending).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn a_remote_peer_can_reuse_an_id_after_its_response_is_written_and_flushed() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let control = FlushControl::new(true);
        let (_peer, connection) = RpcPeer::builder()
            .method("x", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(ControlledFlush {
                stream,
                control: control.clone(),
            })
            .unwrap();
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        let mut response = [0; 5];
        raw.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"\x94\x01\x2a\xc0\x2a");
        control.entered.notified().await;
        control.release();
        control.completed.notified().await;
        for _ in 0..3 {
            raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
            raw.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"\x94\x01\x2a\xc0\x2a");
            control.completed.notified().await;
        }
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[cfg(panic = "unwind")]
#[tokio::test]
async fn panicked_request_ids_remain_active_until_error_response_flush_completes() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(256);
        let control = FlushControl::new(false);
        let (peer, connection) = RpcPeer::builder()
            .method(
                "x",
                |_, (): ()| -> std::future::Ready<Result<(), msgpack_rpc_peer::RemoteError>> {
                    panic!("handler failed");
                },
            )
            .unwrap()
            .start(ControlledFlush {
                stream,
                control: control.clone(),
            })
            .unwrap();
        let pending = pending_call(&peer, &mut raw).await;
        control.completed.notified().await;
        control
            .held
            .store(true, std::sync::atomic::Ordering::SeqCst);
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        control.entered.notified().await;
        raw.write_all(b"\x94\x00\x2a\xa1x\x90").await.unwrap();
        duplicate_failure(connection, pending).await;
    })
    .await
    .unwrap();
}
