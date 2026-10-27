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

#[tokio::test]
async fn hostile_advertised_lengths_are_rejected_without_waiting_for_payload() {
    timeout(Duration::from_secs(3), async {
        for frame in [
            b"\xdd\xff\xff\xff\xff".as_slice(),
            b"\xdf\xff\xff\xff\xff".as_slice(),
            b"\xdb\xff\xff\xff\xff".as_slice(),
            b"\xc6\xff\xff\xff\xff".as_slice(),
            b"\xc9\xff\xff\xff\xff".as_slice(),
        ] {
            let (stream, mut raw) = tokio::io::duplex(128);
            let (_, connection) = RpcPeer::builder().start(stream).unwrap();
            raw.write_all(frame).await.unwrap();
            assert!(matches!(
                connection.wait().await,
                Err(Error::ResourceLimit(_))
            ));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn configured_wire_budgets_are_enforced_and_outgoing_rejection_keeps_stream_usable() {
    use msgpack_rpc_peer::Config;
    timeout(Duration::from_secs(3), async {
        let config = Config {
            max_incoming_bytes: 64,
            max_outgoing_bytes: 64,
            max_depth: 4,
            max_nodes: 12,
            ..Config::default()
        };
        for (frame, resource) in [
            (
                b"\x94\x00\x00\xa1x\x91\x91\x91\x90".as_slice(),
                "MessagePack nesting depth",
            ),
            (b"\x94\x00\x00\xa1x\x99".as_slice(), "decoded value nodes"),
            (
                b"\x94\x00\x00\xa1x\x91\xd9\x40".as_slice(),
                "incoming message bytes",
            ),
        ] {
            let (stream, mut raw) = tokio::io::duplex(128);
            let (_, connection) = RpcPeer::builder()
                .config(config.clone())
                .start(stream)
                .unwrap();
            raw.write_all(frame).await.unwrap();
            assert_eq!(connection.wait().await, Err(Error::ResourceLimit(resource)));
        }
        let (stream, mut raw) = tokio::io::duplex(128);
        let (peer, connection) = RpcPeer::builder().config(config).start(stream).unwrap();
        assert!(peer.notify(&"x".repeat(64), ()).await.is_err());
        assert!(peer.request::<_, ()>("x", ("x".repeat(64),)).await.is_err());
        peer.notify("x", ()).await.unwrap();
        let mut frame = [0; 5];
        raw.read_exact(&mut frame).await.unwrap();
        assert_eq!(frame, *b"\x93\x02\xa1x\x90");
        connection.terminate();
        connection.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn every_fragment_boundary_and_concatenated_objects_keep_standard_framing() {
    timeout(Duration::from_secs(3), async {
        let fixture = b"\x94\x00\xce\xff\xff\xff\xff\xa3inc\x91\xcc\xff";
        for boundary in 1..fixture.len() {
            let (stream, mut raw) = tokio::io::duplex(128);
            let (_, connection) = RpcPeer::builder()
                .method("inc", |_, (n,): (u32,)| async move { Ok(n + 1) })
                .unwrap()
                .start(stream)
                .unwrap();
            raw.write_all(&fixture[..boundary]).await.unwrap();
            tokio::task::yield_now().await;
            raw.write_all(&fixture[boundary..]).await.unwrap();
            raw.write_all(b"\x94\x00\x00\xa3inc\x91\x00").await.unwrap();
            let mut actual = [0; 16];
            raw.read_exact(&mut actual).await.unwrap();
            // Handlers are unordered; independently decode the two responses.
            let mut bytes = actual.as_slice();
            let mut responses = [
                rmpv::decode::read_value(&mut bytes).unwrap(),
                rmpv::decode::read_value(&mut bytes).unwrap(),
            ];
            responses.sort_by_key(|v| v[1].as_u64());
            assert_eq!(
                responses[0],
                rmpv::Value::Array(vec![1.into(), 0.into(), rmpv::Value::Nil, 1.into()])
            );
            assert_eq!(
                responses[1],
                rmpv::Value::Array(vec![
                    1.into(),
                    u32::MAX.into(),
                    rmpv::Value::Nil,
                    256.into()
                ])
            );
            connection.terminate();
            connection.wait().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_wire_and_truncated_eof_preserve_terminal_categories_for_pending_calls() {
    timeout(Duration::from_secs(3), async {
        for frame in [
            b"\xc1".as_slice(),
            b"\x94\x00".as_slice(),
            b"\x94\x00\x00\xa1x\x91\xda\x00".as_slice(),
            b"\x94\x00\xff\xa1x\x90".as_slice(),
            b"\x94\x00\xcf\x00\x00\x00\x01\x00\x00\x00\x00\xa1x\x90".as_slice(),
            b"\x94\x00\xcb\x00\x00\x00\x00\x00\x00\x00\x00\xa1x\x90".as_slice(),
            b"\x94\x03\x00\xc0\xc0".as_slice(),
            b"\x94\x01\x00\x01\x02".as_slice(),
            b"\x94\x00\x00\x00\x90".as_slice(),
            b"\x94\x00\x00\xa1x\x80".as_slice(),
            b"\x94\xca\x00\x00\x00\x00\x00\xa1x\x90".as_slice(),
        ] {
            let (stream, mut raw) = tokio::io::duplex(128);
            let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
            let call = peer.request::<_, ()>("x", ());
            let remote = async {
                let mut request = [0; 6];
                raw.read_exact(&mut request).await.unwrap();
                raw.write_all(frame).await.unwrap();
                raw.shutdown().await.unwrap();
            };
            let (result, ()) = tokio::join!(call, remote);
            let terminal = connection.wait().await;
            assert!(matches!(
                &terminal,
                Err(Error::Decode(_) | Error::Protocol(_))
            ));
            assert_eq!(result, terminal);
        }
    })
    .await
    .unwrap();
}

struct BrokenValue;
impl Serialize for BrokenValue {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "sensitive custom serializer failure",
        ))
    }
}

struct AdvertisedHugeArray;
impl Serialize for AdvertisedHugeArray {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        serializer.serialize_seq(Some(u32::MAX as usize))?.end()
    }
}

#[tokio::test]
async fn rejected_results_errors_and_custom_serializers_leave_a_usable_connection() {
    use msgpack_rpc_peer::{Config, RemoteError, Value};
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(256);
        let (peer, owner) = RpcPeer::builder().start(left).unwrap();
        let (_, remote) = RpcPeer::builder()
            .config(Config {
                max_outgoing_bytes: 96,
                ..Config::default()
            })
            .method("large", |_, (): ()| async { Ok("x".repeat(96)) })
            .unwrap()
            .method("error", |_, (): ()| async {
                Err::<(), _>(RemoteError::new(Value::from("x".repeat(96))).unwrap())
            })
            .unwrap()
            .method("broken", |_, (): ()| async { Ok(BrokenValue) })
            .unwrap()
            .method("advertised", |_, (): ()| async { Ok(AdvertisedHugeArray) })
            .unwrap()
            .method("ok", |_, (): ()| async { Ok(42u32) })
            .unwrap()
            .start(right)
            .unwrap();
        for method in ["large", "error", "broken", "advertised"] {
            let error = peer.request::<_, ()>(method, ()).await.unwrap_err();
            let Error::Remote(value) = error else {
                panic!("expected remote failure");
            };
            assert_eq!(value["code"], Value::from(-32603));
            assert!(!format!("{value:?}").contains("sensitive"));
            assert_eq!(peer.request::<_, u32>("ok", ()).await.unwrap(), 42);
        }
        assert_eq!(
            peer.notify("ok", (BrokenValue,)).await,
            Err(Error::InvalidParams)
        );
        assert_eq!(peer.request::<_, u32>("ok", ()).await.unwrap(), 42);
        owner.terminate();
        owner.wait().await.unwrap();
        remote.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn response_that_cannot_fit_even_a_generic_error_terminates_explicitly() {
    use msgpack_rpc_peer::Config;
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(128);
        let (_, owner) = RpcPeer::builder()
            .config(Config {
                max_outgoing_bytes: 5,
                ..Config::default()
            })
            .method("x", |_, (): ()| async { Ok("too long") })
            .unwrap()
            .start(stream)
            .unwrap();
        raw.write_all(b"\x94\x00\x00\xa1x\x90").await.unwrap();
        assert!(matches!(owner.wait().await, Err(Error::Encode(_))));
        let mut bytes = Vec::new();
        raw.read_to_end(&mut bytes).await.unwrap();
        assert!(
            bytes.is_empty(),
            "rejected frame must not emit partial bytes"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_config_is_rejected_before_spawning_connection_tasks() {
    use msgpack_rpc_peer::Config;
    let invalid = [
        Config {
            max_incoming_bytes: 0,
            ..Config::default()
        },
        Config {
            max_outgoing_bytes: 0,
            ..Config::default()
        },
        Config {
            max_depth: 0,
            ..Config::default()
        },
        Config {
            max_depth: 1025,
            ..Config::default()
        },
        Config {
            max_nodes: 0,
            ..Config::default()
        },
        Config {
            default_call_timeout: Duration::ZERO,
            ..Config::default()
        },
    ];
    for config in invalid {
        let (stream, _) = tokio::io::duplex(128);
        assert!(matches!(
            RpcPeer::builder().config(config).start(stream),
            Err(Error::InvalidConfig(_))
        ));
    }
}

#[tokio::test]
async fn exact_incoming_depth_node_and_byte_limits_are_accepted() {
    use msgpack_rpc_peer::{Config, Value};
    timeout(Duration::from_secs(3), async {
        for depth in [3, 64] {
            for leaf in [b"\xa1x".as_slice(), b"\xd4\x01\x7f".as_slice()] {
                let mut frame = b"\x94\x00\x00\xa1x\x91".to_vec();
                frame.extend(std::iter::repeat_n(0x91, depth - 2));
                frame.extend_from_slice(leaf);
                let (stream, mut raw) = tokio::io::duplex(128);
                let (_, owner) = RpcPeer::builder()
                    .config(Config {
                        max_depth: depth,
                        max_nodes: depth + 4,
                        max_incoming_bytes: frame.len(),
                        ..Config::default()
                    })
                    .method("x", |_, (_value,): (Value,)| async { Ok(42) })
                    .unwrap()
                    .start(stream)
                    .unwrap();
                raw.write_all(&frame).await.unwrap();
                let mut reply = [0; 5];
                raw.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"\x94\x01\x00\xc0\x2a");
                owner.terminate();
                owner.wait().await.unwrap();
            }
        }
    })
    .await
    .unwrap();
}

struct UnknownSequence(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Serialize for UnknownSequence {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(None)?;
        for _ in 0..1000 {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            seq.serialize_element(&0u8)?;
        }
        seq.end()
    }
}

#[tokio::test]
async fn unknown_length_nested_containers_stop_encoding_at_the_byte_limit() {
    use msgpack_rpc_peer::Config;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let (stream, _raw) = tokio::io::duplex(128);
    let (peer, owner) = RpcPeer::builder()
        .config(Config {
            max_outgoing_bytes: 32,
            ..Config::default()
        })
        .start(stream)
        .unwrap();
    let encoded = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        peer.notify("x", (UnknownSequence(encoded.clone()),)).await,
        Err(Error::InvalidParams)
    );
    assert!(
        encoded.load(Ordering::Relaxed) < 32,
        "encoder must stop before buffering the whole unknown-length sequence"
    );
    owner.terminate();
    owner.wait().await.unwrap();
}

struct UnknownMap {
    encoded: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    count: usize,
}
impl Serialize for UnknownMap {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for n in 0..self.count {
            self.encoded
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map.serialize_entry("n", &n)?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct Flattened {
    fixed: u32,
    #[serde(flatten)]
    extra: UnknownMap,
}

#[tokio::test]
async fn flatten_and_nested_unknown_maps_use_the_same_bounded_sink() {
    use msgpack_rpc_peer::{Config, Value};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(256);
        let (peer, owner) = RpcPeer::builder()
            .config(Config {
                max_outgoing_bytes: 64,
                ..Config::default()
            })
            .start(left)
            .unwrap();
        let (_, remote) = RpcPeer::builder()
            .config(Config {
                max_outgoing_bytes: 64,
                ..Config::default()
            })
            .method("echo", |_, (value,): (Value,)| async { Ok(value) })
            .unwrap()
            .method("map", |_, (): ()| async {
                Ok(UnknownMap {
                    encoded: Arc::default(),
                    count: 1000,
                })
            })
            .unwrap()
            .start(right)
            .unwrap();
        let encoded = Arc::new(AtomicUsize::new(0));
        let value: Value = peer
            .request(
                "echo",
                (Flattened {
                    fixed: 42,
                    extra: UnknownMap {
                        encoded: encoded.clone(),
                        count: 1,
                    },
                },),
            )
            .await
            .unwrap();
        assert_eq!(value["fixed"], Value::from(42));
        assert_eq!(value["n"], Value::from(0));
        encoded.store(0, Ordering::Relaxed);
        assert_eq!(
            peer.notify(
                "x",
                (Flattened {
                    fixed: 42,
                    extra: UnknownMap {
                        encoded: encoded.clone(),
                        count: 1000
                    }
                },)
            )
            .await,
            Err(Error::InvalidParams)
        );
        assert!(encoded.load(Ordering::Relaxed) < 32);
        assert!(matches!(
            peer.request::<_, ()>("map", ()).await,
            Err(Error::Remote(_))
        ));
        let result: u32 = peer.request("echo", (42u32,)).await.unwrap();
        assert_eq!(result, 42);
        owner.terminate();
        owner.wait().await.unwrap();
        remote.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_encoding_preserves_enum_binary_and_extension_contracts() {
    use msgpack_rpc_peer::Value;
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    enum Choice {
        Unit,
        Newtype(u32),
        Tuple(u32, bool),
        Struct { n: u32 },
    }
    timeout(Duration::from_secs(3), async {
        let (left, right) = tokio::io::duplex(256);
        let (peer, owner) = RpcPeer::builder().start(left).unwrap();
        let (_, remote) = RpcPeer::builder()
            .method("echo", |_, (value,): (Value,)| async { Ok(value) })
            .unwrap()
            .start(right)
            .unwrap();
        for (choice, expected) in [
            (Choice::Unit, Value::from("Unit")),
            (
                Choice::Newtype(4),
                Value::Map(vec![("Newtype".into(), 4.into())]),
            ),
            (
                Choice::Tuple(2, true),
                Value::Map(vec![(
                    "Tuple".into(),
                    Value::Array(vec![2.into(), true.into()]),
                )]),
            ),
            (
                Choice::Struct { n: 9 },
                Value::Map(vec![(
                    "Struct".into(),
                    Value::Map(vec![("n".into(), 9.into())]),
                )]),
            ),
        ] {
            let result: Value = peer.request("echo", (&choice,)).await.unwrap();
            assert_eq!(result, expected);
        }
        for value in [Value::Binary(vec![0xaa, 0xbb]), Value::Ext(3, vec![0xaa])] {
            let result: Value = peer.request("echo", (&value,)).await.unwrap();
            assert_eq!(result, value);
        }
        owner.terminate();
        owner.wait().await.unwrap();
        remote.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn request_envelope_limit_is_distinct_from_invalid_arguments() {
    use msgpack_rpc_peer::Config;
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, owner) = RpcPeer::builder()
            .config(Config {
                max_outgoing_bytes: 6,
                ..Config::default()
            })
            .start(stream)
            .unwrap();
        assert_eq!(
            peer.request::<_, ()>("x", 7).await,
            Err(Error::InvalidParams)
        );
        assert!(matches!(
            peer.request::<_, ()>("xx", ()).await,
            Err(Error::Encode(_))
        ));
        let calling = peer.request::<_, u32>("x", ());
        let remote = async {
            let mut frame = [0; 6];
            raw.read_exact(&mut frame).await.unwrap();
            assert_eq!(&frame, b"\x94\x00\x02\xa1x\x90");
            raw.write_all(b"\x94\x01\x02\xc0\x2a").await.unwrap();
        };
        let (result, ()) = tokio::join!(calling, remote);
        assert_eq!(result, Ok(42));
        owner.terminate();
        owner.wait().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn complete_outgoing_envelopes_accept_the_exact_byte_limit() {
    use msgpack_rpc_peer::Config;
    timeout(Duration::from_secs(3), async {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, owner) = RpcPeer::builder()
            .config(Config {
                max_outgoing_bytes: 5,
                ..Config::default()
            })
            .start(stream)
            .unwrap();
        peer.notify("x", ()).await.unwrap();
        assert!(matches!(peer.notify("xx", ()).await, Err(Error::Encode(_))));
        let mut frame = [0; 5];
        raw.read_exact(&mut frame).await.unwrap();
        assert_eq!(&frame, b"\x93\x02\xa1x\x90");
        owner.terminate();
        owner.wait().await.unwrap();
    })
    .await
    .unwrap();
}

struct FormattedValue(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl std::fmt::Display for FormattedValue {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for _ in 0..1000 {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            output.write_str("x")?;
        }
        Ok(())
    }
}
impl Serialize for FormattedValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[tokio::test]
async fn formatted_strings_stop_at_the_byte_limit_without_a_temporary_string() {
    use msgpack_rpc_peer::Config;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let (stream, _raw) = tokio::io::duplex(128);
    let (peer, owner) = RpcPeer::builder()
        .config(Config {
            max_outgoing_bytes: 32,
            ..Config::default()
        })
        .start(stream)
        .unwrap();
    let encoded = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        peer.notify("x", (FormattedValue(encoded.clone()),)).await,
        Err(Error::InvalidParams)
    );
    assert!(encoded.load(Ordering::Relaxed) < 32);
    owner.terminate();
    owner.wait().await.unwrap();
}
