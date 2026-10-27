use msgpack_rpc_peer::{Error, RpcPeer};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, timeout},
};

#[derive(Serialize)]
struct Person {
    name: &'static str,
}

#[tokio::test]
async fn outgoing_requests_match_literal_fixtures_and_preserve_remote_errors() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(128);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        let calling = async {
            let name: String = peer
                .request("echo", (Person { name: "Ada" },))
                .await
                .unwrap();
            assert_eq!(name, "Ada");
            // Non-nil false is a failure, even though it is not truthy or a map.
            assert_eq!(
                peer.request::<_, ()>("empty", ()).await,
                Err(Error::Remote(false.into()))
            );
        };
        let remote = async {
            let request = b"\x94\x00\x00\xa4echo\x91\x81\xa4name\xa3Ada";
            let mut actual = vec![0; request.len()];
            raw.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, request);
            raw.write_all(b"\x94\x01\x00\xc0\xa3Ada").await.unwrap();
            let request = b"\x94\x00\x01\xa5empty\x90";
            let mut actual = vec![0; request.len()];
            raw.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, request);
            raw.write_all(b"\x94\x01\x01\xc2\xc0").await.unwrap();
        };
        tokio::join!(calling, remote);
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn incoming_request_and_success_response_match_literal_fixtures() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(128);
        let (_peer, connection) = RpcPeer::builder()
            .method("inc", |_, (n,): (u32,)| async move { Ok(n + 1) })
            .unwrap()
            .start(stream)
            .unwrap();
        // [0, u32::MAX, "inc", [42]], fragmented across the ID payload.
        raw.write_all(b"\x94\x00\xce\xff").await.unwrap();
        raw.write_all(b"\xff\xff\xff\xa3inc\x91\x2a").await.unwrap();
        let mut response = [0; 9];
        raw.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"\x94\x01\xce\xff\xff\xff\xff\xc0\x2b");
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unknown_methods_and_bad_arguments_return_errors_without_closing() {
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(128);
        let (_peer, connection) = RpcPeer::builder()
            .method("inc", |_, (n,): (u32,)| async move { Ok(n + 1) })
            .unwrap()
            .start(stream)
            .unwrap();
        for (request, expected) in [
            (
                b"\x94\x00\x2a\xa1x\x90".as_slice(),
                b"\x94\x01\x2a\x82\xa4code\xd1\x80\xa7\xa7message\xaeunknown method\xc0".as_slice(),
            ),
            (
                b"\x94\x00\x2b\xa3inc\x91\xa1x".as_slice(),
                b"\x94\x01\x2b\x82\xa4code\xd1\x80\xa6\xa7message\xb2invalid parameters\xc0"
                    .as_slice(),
            ),
            (
                b"\x94\x00\x2c\xa3inc\x91\x2a".as_slice(),
                b"\x94\x01\x2c\xc0\x2b".as_slice(),
            ),
        ] {
            raw.write_all(request).await.unwrap();
            let mut actual = vec![0; expected.len()];
            raw.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, expected);
        }
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn arbitrary_remote_error_values_are_preserved() {
    use msgpack_rpc_peer::Value;
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(128);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        for (id, bytes, expected) in [
            (0, b"\x00".as_slice(), Value::from(0)),
            (
                1,
                b"\xc4\x02\xaa\xbb".as_slice(),
                Value::Binary(vec![0xaa, 0xbb]),
            ),
            (
                2,
                b"\x92\x01\xa1x".as_slice(),
                Value::Array(vec![1.into(), "x".into()]),
            ),
            (
                3,
                b"\x81\xa1x\x02".as_slice(),
                Value::Map(vec![("x".into(), 2.into())]),
            ),
            (4, b"\xd4\x03\xaa".as_slice(), Value::Ext(3, vec![0xaa])),
        ] {
            let calling = peer.request::<_, ()>("x", ());
            let remote = async {
                let mut request = [0; 6];
                raw.read_exact(&mut request).await.unwrap();
                assert_eq!(request, [0x94, 0, id, 0xa1, b'x', 0x90]);
                let mut response = vec![0x94, 1, id];
                response.extend_from_slice(bytes);
                response.push(0xc0);
                raw.write_all(&response).await.unwrap();
            };
            let (result, ()) = tokio::join!(calling, remote);
            assert_eq!(result, Err(Error::Remote(expected)));
        }
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn notifications_match_literal_fixtures_in_both_directions() {
    timeout(Duration::from_secs(3), async {
        let (received, mut events) = tokio::sync::mpsc::channel(2);
        let (stream, mut raw) = tokio::io::duplex(128);
        let (peer, connection) = RpcPeer::builder()
            .notification("event", move |_, (n,): (u32,)| {
                let received = received.clone();
                async move {
                    received.send(n).await.unwrap();
                    Ok(())
                }
            })
            .unwrap()
            .start(stream)
            .unwrap();
        peer.notify("event", (Person { name: "Ada" },))
            .await
            .unwrap();
        let expected = b"\x93\x02\xa5event\x91\x81\xa4name\xa3Ada";
        let mut actual = vec![0; expected.len()];
        raw.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        peer.notify("empty", ()).await.unwrap();
        let mut empty = [0; 9];
        raw.read_exact(&mut empty).await.unwrap();
        assert_eq!(&empty, b"\x93\x02\xa5empty\x90");
        // Fragmented notification followed by another object in the same stream.
        raw.write_all(b"\x93\x02\xa5ev").await.unwrap();
        raw.write_all(b"ent\x91\x2a\x93\x02\xa5event\x91\x07")
            .await
            .unwrap();
        let mut received = [events.recv().await.unwrap(), events.recv().await.unwrap()];
        received.sort();
        assert_eq!(received, [7, 42]);
        connection.terminate();
        connection.wait().await.unwrap();
        let mut remaining = Vec::new();
        raw.read_to_end(&mut remaining).await.unwrap();
        assert!(
            remaining.is_empty(),
            "notifications must not generate responses"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_notification_envelopes_close_the_connection() {
    timeout(Duration::from_secs(3), async {
        for (frame, reason) in [
            (
                b"\x92\x02\xa1x".as_slice(),
                "notification must have three fields",
            ),
            (
                b"\x94\x02\xa1x\x90\xc0".as_slice(),
                "notification must have three fields",
            ),
            (b"\x93\x02\x00\x90".as_slice(), "method must be a string"),
            (b"\x93\x02\xa1x\xc0".as_slice(), "params must be an array"),
        ] {
            let (stream, mut raw) = tokio::io::duplex(128);
            let (_peer, connection) = RpcPeer::builder().start(stream).unwrap();
            raw.write_all(frame).await.unwrap();
            assert_eq!(connection.wait().await, Err(Error::Protocol(reason)));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn notification_failures_never_reply_or_close_the_connection() {
    timeout(Duration::from_secs(3), async {
        let (invoked, mut calls) = tokio::sync::mpsc::channel(2);
        let panicked = invoked.clone();
        let (stream, mut raw) = tokio::io::duplex(128);
        let (_peer, connection) = RpcPeer::builder()
            .notification("err", move |_, (): ()| {
                let invoked = invoked.clone();
                async move {
                    invoked.send(()).await.unwrap();
                    Err(msgpack_rpc_peer::RemoteError::new("private error".into()).unwrap())
                }
            })
            .unwrap()
            .notification("panic", move |_, (): ()| {
                let panicked = panicked.clone();
                async move {
                    panicked.send(()).await.unwrap();
                    panic!("private notification panic");
                }
            })
            .unwrap()
            .method("ok", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(stream)
            .unwrap();
        raw.write_all(b"\x93\x02\xa3err\x90\x93\x02\xa5panic\x90")
            .await
            .unwrap();
        calls.recv().await.unwrap();
        calls.recv().await.unwrap();
        // Unknown names and invalid typed arguments are also one-way failures.
        raw.write_all(b"\x93\x02\xa1x\x90\x93\x02\xa3err\x91\x01\x94\x00\x00\xa2ok\x90")
            .await
            .unwrap();
        let mut response = [0; 5];
        raw.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"\x94\x01\x00\xc0\x2a");
        connection.terminate();
        connection.wait().await.unwrap();
        let mut remaining = Vec::new();
        raw.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.is_empty());
        assert!(
            calls.recv().await.is_none(),
            "invalid arguments must not invoke the handler"
        );
    })
    .await
    .unwrap();
}
