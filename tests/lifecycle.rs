use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use msgpack_rpc_peer::{Error, RpcPeer};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::{Notify, oneshot},
    time::{Duration, timeout},
};

#[tokio::test]
async fn owner_drop_cancels_handlers_and_surviving_handles_observe_closure() {
    timeout(Duration::from_secs(3), async {
        struct OnDrop(Option<oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let dropped = Arc::new(std::sync::Mutex::new(Some(dropped_tx)));
        let started = Arc::new(Notify::new());
        let signal = started.clone();
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder()
            .method("wait", move |_, (): ()| {
                let signal = signal.clone();
                let guard = OnDrop(dropped.lock().unwrap().take());
                async move {
                    let _guard = guard;
                    signal.notify_one();
                    std::future::pending::<Result<(), msgpack_rpc_peer::RemoteError>>().await
                }
            })
            .unwrap()
            .start(stream)
            .unwrap();
        raw.write_all(b"\x94\x00\x00\xa4wait\x90").await.unwrap();
        started.notified().await;
        drop(connection);
        dropped_rx.await.unwrap();
        assert_eq!(
            raw.read_u8().await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(
            peer.request::<_, ()>("anything", ()).await,
            Err(Error::Closed)
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn termination_completes_a_pending_request_and_closes_the_stream() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        let calling = peer.request::<_, ()>("wait", ());
        let terminating = async {
            let mut request = [0; 9];
            raw.read_exact(&mut request).await.unwrap();
            connection.terminate();
            connection.wait().await.unwrap();
            assert_eq!(
                raw.read_u8().await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        };
        let (result, ()) = tokio::join!(calling, terminating);
        assert_eq!(result, Err(Error::Closed));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_response_fails_the_owner_and_pending_call_with_the_same_cause() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        let calling = peer.request::<_, ()>("wait", ());
        let remote = async {
            let mut request = [0; 9];
            raw.read_exact(&mut request).await.unwrap();
            // A response ID outside the u32 range.
            raw.write_all(b"\x94\x01\xcf\x00\x00\x00\x01\x00\x00\x00\x00\xc0\xc0")
                .await
                .unwrap();
            connection.wait().await
        };
        let (result, completion) = tokio::join!(calling, remote);
        assert_eq!(result, Err(Error::Protocol("ID must be a u32")));
        assert_eq!(completion, result);
    })
    .await
    .unwrap();
}

struct BrokenWriter;
impl AsyncRead for BrokenWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for BrokenWriter {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "fixture write failed",
        )))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn io_failure_reaches_the_owner_and_pending_call() {
    timeout(Duration::from_secs(3), async {
        let (peer, connection) = RpcPeer::builder().start(BrokenWriter).unwrap();
        let (result, completion) =
            tokio::join!(peer.request::<_, ()>("wait", ()), connection.wait());
        let expected = Err(Error::Transport("fixture write failed".into()));
        assert_eq!(result, expected);
        assert_eq!(completion, expected);
    })
    .await
    .unwrap();
}

#[test]
fn startup_requires_the_callers_runtime() {
    let (stream, _raw) = tokio::io::duplex(64);
    assert!(matches!(
        RpcPeer::builder().start(stream),
        Err(Error::NoRuntime)
    ));
}
