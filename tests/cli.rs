#![cfg(feature = "cli")]
use msgpack_rpc_peer::RpcPeer;
use std::{process::Stdio, time::Duration};
use tokio::{net::TcpListener, process::Command, time::timeout};

fn cli() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrpc"));
    command
        .env_remove("MRPC_TARGET")
        .env_remove("MRPC_OUTPUT")
        .env_remove("MRPC_TIMEOUT")
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[tokio::test]
async fn request_returns_a_record_and_closes_the_connection() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("tcp://{}", listener.local_addr().unwrap());
        let child = cli()
            .args(["request", "sum", "--target", &target, "-p", "[2,3]"])
            .spawn()
            .unwrap();
        let (_, owner) = RpcPeer::builder()
            .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })
            .unwrap()
            .start(listener.accept().await.unwrap().0)
            .unwrap();
        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success(), "{:?}", output);
        assert!(output.stdout.contains(&b'\n'));
        assert!(output.stdout.contains(&b' '));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            serde_json::json!({"type":"result", "encoding":"json", "value":5})
        );
        owner.wait().await.unwrap();
    })
    .await
    .unwrap();
}

async fn receive<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> msgpack_rpc_peer::Value {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    loop {
        bytes.push(stream.read_u8().await.unwrap());
        if let Ok(value) = rmpv::decode::read_value(&mut bytes.as_slice()) {
            return value;
        }
    }
}
async fn send<S: tokio::io::AsyncWrite + Unpin>(stream: &mut S, value: msgpack_rpc_peer::Value) {
    use tokio::io::AsyncWriteExt;
    let mut bytes = Vec::new();
    fixture_bytes(&value, &mut bytes);
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
}
fn response(
    id: msgpack_rpc_peer::Value,
    value: msgpack_rpc_peer::Value,
) -> msgpack_rpc_peer::Value {
    msgpack_rpc_peer::Value::Array(vec![1.into(), id, msgpack_rpc_peer::Value::Nil, value])
}
async fn caller(address: &str) -> tokio::net::TcpStream {
    loop {
        if let Ok(stream) = tokio::net::TcpStream::connect(address).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn shared_session_routes_overlapping_ids_and_reverse_traffic_then_fails_on_target_eof() {
    use msgpack_rpc_peer::Value as V;
    timeout(Duration::from_secs(8), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reserve.local_addr().unwrap().to_string();
        drop(reserve);
        let mut forward = cli().args(["forward", "--target", &format!("tcp://{}",target.local_addr().unwrap()), "--listen", &format!("tcp://{address}")]).spawn().unwrap();
        let (mut session, _) = target.accept().await.unwrap();
        let mut a = caller(&address).await;
        let mut b = caller(&address).await;
        send(&mut a, V::Array(vec![0.into(), 7.into(), "login".into(), V::Array(vec![])])).await;
        let login = receive(&mut session).await;
        send(&mut session, response(login[1].clone(), "logged-in".into())).await;
        assert_eq!(receive(&mut a).await, response(7.into(), "logged-in".into()));
        drop(a);
        let mut a = caller(&address).await;
        send(&mut a, V::Array(vec![0.into(), 7.into(), "query".into(), V::Array(vec![])])).await;
        let first = receive(&mut session).await;
        send(&mut b, V::Array(vec![0.into(), 7.into(), "fail".into(), V::Array(vec![])])).await;
        let second = receive(&mut session).await;
        assert_ne!(first[1], second[1]);
        send(&mut session, V::Array(vec![1.into(), second[1].clone(), "denied".into(), V::Nil])).await;
        send(&mut session, response(first[1].clone(), 42.into())).await;
        assert_eq!(receive(&mut a).await, response(7.into(), 42.into()));
        assert_eq!(receive(&mut b).await, V::Array(vec![1.into(), 7.into(), "denied".into(), V::Nil]));
        send(&mut session, V::Array(vec![2.into(), "event".into(), V::Array(vec![9.into()])])).await;
        send(&mut session, V::Array(vec![0.into(), 7.into(), "reverse".into(), V::Array(vec![])])).await;
        let reverse = receive(&mut session).await;
        assert_eq!(reverse[1], V::from(7));
        assert_eq!(reverse[2]["code"], V::from(-32601));
        send(&mut a, V::Array(vec![0.into(), 8.into(), "pending".into(), V::Array(vec![])])).await;
        receive(&mut session).await;
        drop(session);
        assert!(!forward.wait().await.unwrap().success());
        let output = forward.wait_with_output().await.unwrap();
        let event: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(event, serde_json::json!({"type":"notification","method":"event","encoding":"json","params":[9]}));
        use tokio::io::AsyncReadExt;
        let mut remaining = Vec::new();
        a.read_to_end(&mut remaining).await.unwrap();
        assert!(timeout(Duration::from_millis(100), target.accept()).await.is_err());
    }).await.unwrap();
}

#[tokio::test]
async fn invalid_inputs_fail_before_connecting() {
    timeout(Duration::from_secs(5), async {
        for args in [
            vec!["request", "x"],
            vec!["request", "x", "--target", "unix://relative"],
            vec!["request", "x", "--target", "http://localhost:1"],
            vec!["request", "x", "--params", "[]", "--params-file", "-"],
            vec![
                "request",
                "x",
                "--target",
                "tcp://localhost:1",
                "--timeout",
                "0",
            ],
        ] {
            let output = cli().args(args).output().await.unwrap();
            assert!(!output.status.success());
            assert!(output.stdout.is_empty());
            assert!(!output.stderr.is_empty());
        }
        for text in ["null", "{}", "1", "true", "[", "[] junk"] {
            let output = cli()
                .args([
                    "request",
                    "x",
                    "--target",
                    "tcp://localhost:1",
                    "--params",
                    text,
                ])
                .output()
                .await
                .unwrap();
            assert!(!output.status.success());
            assert!(!String::from_utf8_lossy(&output.stderr).contains("Connection refused"));
        }
        let output = cli()
            .args([
                "request",
                "x",
                "--target",
                "tcp://localhost:1",
                "--params-file",
                "/no/such/mrpc-file",
            ])
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
    })
    .await
    .unwrap();
}

fn decode_record(bytes: &[u8], field: &str) -> msgpack_rpc_peer::Value {
    let record: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(record["encoding"], "msgpack-hex");
    let hex = record[field].as_str().unwrap();
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect();
    rmpv::decode::read_value(&mut bytes.as_slice()).unwrap()
}

#[tokio::test]
async fn results_and_remote_errors_preserve_special_values_in_both_formats() {
    use msgpack_rpc_peer::Value as V;
    timeout(Duration::from_secs(12), async {
        let invalid_string = rmpv::decode::read_value(&mut b"\xa1\xff".as_slice()).unwrap();
        let values = [
            V::Binary(vec![0, 255]),
            V::Ext(3, vec![1, 2]),
            V::Map(vec![(1.into(), "one".into())]),
            V::Map(vec![("x".into(), 1.into()), ("x".into(), 2.into())]),
            invalid_string,
            V::F64(f64::INFINITY),
            V::F64(f64::NAN),
        ];
        for value in values {
            for format in ["compact", "pretty"] {
                for remote_error in [false, true] {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let child = cli()
                        .args([
                            "request",
                            "x",
                            "--target",
                            &format!("tcp://{}", listener.local_addr().unwrap()),
                            "--output",
                            format,
                        ])
                        .spawn()
                        .unwrap();
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let request = receive(&mut stream).await;
                    let message = if remote_error {
                        V::Array(vec![1.into(), request[1].clone(), value.clone(), V::Nil])
                    } else {
                        response(request[1].clone(), value.clone())
                    };
                    send(&mut stream, message).await;
                    let output = child.wait_with_output().await.unwrap();
                    assert_eq!(output.status.success(), !remote_error);
                    let bytes = if remote_error {
                        output.stderr
                    } else {
                        output.stdout
                    };
                    let decoded = decode_record(&bytes, "value");
                    if matches!(value, V::F64(number) if number.is_nan()) {
                        assert!(matches!(decoded, V::F64(number) if number.is_nan()));
                    } else {
                        assert_eq!(decoded, value);
                    }
                }
            }
        }
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_calls_support_all_params_sources_environment_override_and_notify_flush() {
    use msgpack_rpc_peer::Value as V;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    timeout(Duration::from_secs(10), async {
        let directory = std::env::temp_dir().join(format!("mrpc-inputs-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let path = directory.join("rpc.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let target = format!("unix://{}", path.display());
        let file = directory.join("params.json");
        std::fs::write(&file, "[1, {\"x\":true}]").unwrap();
        for mode in ["default", "inline", "file", "stdin", "notify"] {
            let mut command = cli();
            command.env("MRPC_TARGET", &target);
            command.args([
                if mode == "notify" {
                    "notify"
                } else {
                    "request"
                },
                "echo",
            ]);
            if mode == "inline" {
                command
                    .args(["--target", &target, "--params", "[1, {\"x\":true}]"])
                    .env("MRPC_TARGET", "tcp://127.0.0.1:1");
            }
            if mode == "file" {
                command.args(["--params-file", file.to_str().unwrap()]);
            }
            if mode == "stdin" {
                command.args(["--params-file", "-"]).stdin(Stdio::piped());
            }
            let mut child = command.spawn().unwrap();
            if mode == "stdin" {
                let mut stdin = child.stdin.take().unwrap();
                stdin.write_all(b"[1, {\"x\":true}]").await.unwrap();
                drop(stdin);
            }
            let (mut stream, _) = listener.accept().await.unwrap();
            let frame = receive(&mut stream).await;
            assert_eq!(frame[if mode == "notify" { 1 } else { 2 }], V::from("echo"));
            let expected = if mode == "default" || mode == "notify" {
                V::Array(vec![])
            } else {
                V::Array(vec![1.into(), V::Map(vec![("x".into(), true.into())])])
            };
            assert_eq!(frame[if mode == "notify" { 2 } else { 3 }], expected);
            if mode != "notify" {
                send(&mut stream, response(frame[1].clone(), expected)).await;
            }
            let output = child.wait_with_output().await.unwrap();
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            if mode == "notify" {
                assert!(output.stdout.is_empty());
            }
            assert!(stream.read_u8().await.is_err());
        }
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn disconnected_and_timed_out_callers_leave_cross_transport_session_usable() {
    use msgpack_rpc_peer::Value as V;
    timeout(Duration::from_secs(10), async {
        let path = std::env::temp_dir().join(format!("mrpc-forward-{}.sock", std::process::id()));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = Cleanup(path.clone());
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reserve.local_addr().unwrap().to_string();
        drop(reserve);
        let target = format!("tcp://{address}");
        let forward = cli()
            .args([
                "forward",
                "--target",
                &format!("unix://{}", path.display()),
                "--listen",
                &target,
            ])
            .spawn()
            .unwrap();
        let (mut session, _) = listener.accept().await.unwrap();
        let mut abandoned = caller(&address).await;
        send(
            &mut abandoned,
            V::Array(vec![0.into(), 0.into(), "abandon".into(), V::Array(vec![])]),
        )
        .await;
        let abandoned_request = receive(&mut session).await;
        drop(abandoned);
        let timed = cli()
            .args(["request", "slow", "--target", &target, "--timeout", "1"])
            .spawn()
            .unwrap();
        let slow = receive(&mut session).await;
        assert!(!timed.wait_with_output().await.unwrap().status.success());
        let next = cli()
            .args(["request", "query", "--target", &target])
            .spawn()
            .unwrap();
        let query = receive(&mut session).await;
        send(
            &mut session,
            response(abandoned_request[1].clone(), "old".into()),
        )
        .await;
        send(&mut session, response(slow[1].clone(), "late".into())).await;
        send(
            &mut session,
            response(query[1].clone(), "session-alive".into()),
        )
        .await;
        let output = next.wait_with_output().await.unwrap();
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["value"],
            "session-alive"
        );
        drop(session);
        assert!(!forward.wait_with_output().await.unwrap().status.success());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn direct_calls_reject_reverse_requests_and_tcp_notify_flushes() {
    use msgpack_rpc_peer::Value as V;
    use tokio::io::AsyncReadExt;
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("tcp://{}", listener.local_addr().unwrap());
        let child = cli()
            .args(["request", "echo", "--target", &target])
            .spawn()
            .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = receive(&mut stream).await;
        send(
            &mut stream,
            V::Array(vec![0.into(), 0.into(), "reverse".into(), V::Array(vec![])]),
        )
        .await;
        assert_eq!(receive(&mut stream).await[2]["code"], V::from(-32601));
        send(&mut stream, response(request[1].clone(), true.into())).await;
        assert!(child.wait_with_output().await.unwrap().status.success());
        let child = cli()
            .args(["notify", "event", "--target", &target, "--params", "[42]"])
            .spawn()
            .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(
            receive(&mut stream).await,
            V::Array(vec![2.into(), "event".into(), V::Array(vec![42.into()])])
        );
        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
        assert!(stream.read_u8().await.is_err());
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_forward_listener_preserves_dynamic_requests_results_errors_and_notifications() {
    use msgpack_rpc_peer::Value as V;
    use tokio::io::{AsyncBufReadExt, BufReader};
    timeout(Duration::from_secs(8), async {
        for format in ["compact", "pretty"] {
            let directory =
                std::env::temp_dir().join(format!("mrpc-output-{}-{format}", std::process::id()));
            std::fs::create_dir(&directory).unwrap();
            struct Cleanup(std::path::PathBuf);
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    let _ = std::fs::remove_dir_all(&self.0);
                }
            }
            let _cleanup = Cleanup(directory.clone());
            let path = directory.join("caller.sock");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut forward = cli()
                .args([
                    "forward",
                    "--listen",
                    &format!("unix://{}", path.display()),
                    "--target",
                    &format!("tcp://{}", listener.local_addr().unwrap()),
                    "--output",
                    format,
                ])
                .spawn()
                .unwrap();
            let (mut session, _) = listener.accept().await.unwrap();
            let mut caller = tokio::net::UnixStream::connect(&path).await.unwrap();
            let invalid = rmpv::decode::read_value(&mut b"\xa1\xff".as_slice()).unwrap();
            let params = V::Array(vec![V::Ext(4, vec![1, 2]), invalid.clone()]);
            send(
                &mut caller,
                V::Array(vec![0.into(), 0.into(), "echo".into(), params.clone()]),
            )
            .await;
            let request = receive(&mut session).await;
            assert_eq!(request[3], params);
            send(&mut session, response(request[1].clone(), invalid.clone())).await;
            assert_eq!(
                receive(&mut caller).await,
                response(0.into(), invalid.clone())
            );
            send(
                &mut caller,
                V::Array(vec![0.into(), 1.into(), "error".into(), V::Array(vec![])]),
            )
            .await;
            let request = receive(&mut session).await;
            send(
                &mut session,
                V::Array(vec![1.into(), request[1].clone(), invalid.clone(), V::Nil]),
            )
            .await;
            assert_eq!(
                receive(&mut caller).await,
                V::Array(vec![1.into(), 1.into(), invalid, V::Nil])
            );
            send(
                &mut caller,
                V::Array(vec![2.into(), "event".into(), params.clone()]),
            )
            .await;
            assert_eq!(
                receive(&mut session).await,
                V::Array(vec![2.into(), "event".into(), params.clone()])
            );
            send(
                &mut session,
                V::Array(vec![2.into(), "special".into(), params.clone()]),
            )
            .await;
            let mut output = BufReader::new(forward.stdout.take().unwrap());
            let mut record = String::new();
            loop {
                assert_ne!(output.read_line(&mut record).await.unwrap(), 0);
                if serde_json::from_str::<serde_json::Value>(&record).is_ok() {
                    break;
                }
            }
            assert_eq!(decode_record(record.as_bytes(), "params"), params);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&record).unwrap()["method"],
                "special"
            );
            drop(session);
            assert!(!forward.wait().await.unwrap().success());
        }
    })
    .await
    .unwrap();
}

// Independent fixtures use fixed-width container/string headers, not the library encoder.
fn fixture_bytes(value: &msgpack_rpc_peer::Value, bytes: &mut Vec<u8>) {
    use msgpack_rpc_peer::Value as V;
    match value {
        V::String(text) if text.as_str().is_none() => {
            bytes.push(0xdb);
            bytes.extend_from_slice(&(text.as_bytes().len() as u32).to_be_bytes());
            bytes.extend_from_slice(text.as_bytes());
        }
        V::Array(values) => {
            bytes.push(0xdd);
            bytes.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                fixture_bytes(value, bytes);
            }
        }
        V::Map(values) => {
            bytes.push(0xdf);
            bytes.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for (key, value) in values {
                fixture_bytes(key, bytes);
                fixture_bytes(value, bytes);
            }
        }
        _ => rmpv::encode::write_value(bytes, value).unwrap(),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn notify_does_not_report_success_for_a_partial_frame_when_target_half_closes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    timeout(Duration::from_secs(5), async {
        let path =
            std::env::temp_dir().join(format!("mrpc-half-close-{}.sock", std::process::id()));
        let params = std::env::temp_dir().join(format!("mrpc-large-{}.json", std::process::id()));
        struct Cleanup(Vec<std::path::PathBuf>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                for path in &self.0 {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        let _cleanup = Cleanup(vec![path.clone(), params.clone()]);
        std::fs::write(&params, format!("[\"{}\"]", "x".repeat(800_000))).unwrap();
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let child = cli()
            .args([
                "notify",
                "event",
                "--target",
                &format!("unix://{}", path.display()),
                "--params-file",
                params.to_str().unwrap(),
            ])
            .spawn()
            .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.shutdown().await.unwrap();
        let output = child.wait_with_output().await.unwrap();
        let mut received = Vec::new();
        stream.read_to_end(&mut received).await.unwrap();
        if output.status.success() {
            let frame = rmpv::decode::read_value(&mut received.as_slice())
                .expect("success requires a complete flushed frame");
            assert_eq!(frame[2][0].as_str().unwrap().len(), 800_000);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn forward_prints_a_received_notification_before_exiting_on_immediate_target_eof() {
    use msgpack_rpc_peer::Value as V;
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let forward = cli().args(["forward","--listen","tcp://127.0.0.1:0","--target",&format!("tcp://{}",listener.local_addr().unwrap())]).spawn().unwrap();
        let (mut session,_) = listener.accept().await.unwrap();
        send(&mut session,V::Array(vec![2.into(),"last-event".into(),V::Array(vec![1.into()])])).await;
        drop(session);
        let output = forward.wait_with_output().await.unwrap();
        assert!(!output.status.success());
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),serde_json::json!({"type":"notification","method":"last-event","encoding":"json","params":[1]}));
    }).await.unwrap();
}

#[tokio::test]
async fn independent_cli_calls_reuse_login_state_on_one_target_connection() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reserve.local_addr().unwrap().to_string();
        drop(reserve);
        let target = format!("tcp://{address}");
        let forward = cli()
            .args([
                "forward",
                "--listen",
                &target,
                "--target",
                &format!("tcp://{}", listener.local_addr().unwrap()),
            ])
            .spawn()
            .unwrap();
        let logged_in = Arc::new(AtomicBool::new(false));
        let login_state = logged_in.clone();
        let query_state = logged_in.clone();
        let (_, owner) = RpcPeer::builder()
            .method("login", move |_, (): ()| {
                login_state.store(true, Ordering::SeqCst);
                async { Ok(true) }
            })
            .unwrap()
            .method("query", move |_, (): ()| {
                let logged_in = query_state.load(Ordering::SeqCst);
                async move { Ok(logged_in) }
            })
            .unwrap()
            .start(listener.accept().await.unwrap().0)
            .unwrap();
        drop(caller(&address).await);
        for method in ["login", "query"] {
            let output = cli()
                .args(["request", method, "--target", &target])
                .output()
                .await
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["value"],
                true
            );
        }
        assert!(logged_in.load(Ordering::SeqCst));
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        owner.terminate();
        owner.wait().await.unwrap();
        assert!(!forward.wait_with_output().await.unwrap().status.success());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn environment_options_are_validated_and_cli_options_override_them() {
    let output = cli()
        .env("MRPC_TIMEOUT", "0")
        .args(["request", "x", "--target", "invalid"])
        .output()
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&output.stderr).contains("--timeout"));

    let output = cli()
        .env("MRPC_TIMEOUT", "0")
        .env("MRPC_OUTPUT", "invalid")
        .args([
            "request",
            "x",
            "--target",
            "invalid",
            "--timeout",
            "1",
            "-o",
            "compact",
        ])
        .output()
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&output.stderr).contains("address must"));
}

#[cfg(unix)]
#[tokio::test]
async fn subprocess_command_overrides_environment_and_reports_clean_exit_as_session_end() {
    let output = timeout(
        Duration::from_secs(8),
        cli()
            .env("MRPC_TARGET", "invalid")
            .args([
                "forward",
                "--listen",
                "tcp://127.0.0.1:0",
                "--",
                "/bin/sh",
                "-c",
                "echo child-diagnostic >&2; exit 0",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("child-diagnostic"), "{stderr}");
    assert!(stderr.contains("subprocess exited"), "{stderr}");
    assert!(output.stdout.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn subprocess_arguments_reject_conflicts_empty_commands_and_spawn_failures() {
    for (args, diagnostic) in [
        (
            vec![
                "forward",
                "--listen",
                "tcp://127.0.0.1:0",
                "--target",
                "tcp://127.0.0.1:1",
                "--",
                "/bin/true",
            ],
            "conflicts",
        ),
        (
            vec!["forward", "--listen", "tcp://127.0.0.1:0", "--"],
            "program",
        ),
        (
            vec![
                "forward",
                "--listen",
                "tcp://127.0.0.1:0",
                "--",
                "/no/such/mrpc-program",
            ],
            "No such file",
        ),
    ] {
        let output = cli()
            .env("MRPC_TARGET", "invalid")
            .args(args)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(diagnostic), "{stderr}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn subprocess_signal_cleanup_closes_stdin_and_forces_unresponsive_children() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    timeout(Duration::from_secs(18), async {
        for (signal, code, script) in [
            (
                "-INT",
                130,
                "echo ready >&2; cat >/dev/null; echo input-closed >&2",
            ),
            ("-TERM", 143, "echo ready >&2; exec sleep 60"),
        ] {
            let mut forward = cli()
                .args([
                    "forward",
                    "--listen",
                    "tcp://127.0.0.1:0",
                    "--",
                    "/bin/sh",
                    "-c",
                    script,
                ])
                .spawn()
                .unwrap();
            let mut stderr = BufReader::new(forward.stderr.take().unwrap());
            let mut text = String::new();
            stderr.read_line(&mut text).await.unwrap();
            assert_eq!(text, "ready\n");
            assert!(
                Command::new("kill")
                    .args([signal, &forward.id().unwrap().to_string()])
                    .status()
                    .await
                    .unwrap()
                    .success()
            );
            assert_eq!(forward.wait().await.unwrap().code(), Some(code));
            use tokio::io::AsyncReadExt;
            stderr.read_to_string(&mut text).await.unwrap();
            assert!(text.contains("subprocess exited"), "{text}");
            if signal == "-INT" {
                assert!(text.contains("input-closed"), "{text}");
            }
        }
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn subprocess_stdout_eof_cleans_up_a_still_running_child() {
    let output = timeout(
        Duration::from_secs(8),
        cli()
            .args([
                "forward",
                "--listen",
                "tcp://127.0.0.1:0",
                "--",
                "/bin/sh",
                "-c",
                "exec 1>&-; exec sleep 60",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("subprocess exited"));
}

#[tokio::test]
async fn missing_target_is_rejected_before_reading_stdin() {
    let mut child = cli()
        .args(["request", "echo", "--params-file", "-"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let _open_stdin = child.stdin.take().unwrap();
    let output = timeout(Duration::from_secs(2), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--target"));
}

#[cfg(unix)]
#[tokio::test]
async fn rpc_server_example_serves_sum_over_all_transports() {
    let build = std::process::Command::new(env!("CARGO"))
        .args([
            "build",
            "--locked",
            "--example",
            "rpc-server",
            "--message-format=json",
        ])
        .output()
        .unwrap();
    assert!(
        build.status.success(),
        "{}",
        String::from_utf8_lossy(&build.stderr)
    );
    let example = String::from_utf8(build.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|artifact| {
            artifact["target"]["name"] == "rpc-server" && artifact["executable"].is_string()
        })
        .unwrap()["executable"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut stdio = Command::new(&example)
        .arg("stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    drop(stdio.stdin.take());
    assert!(
        timeout(Duration::from_secs(2), stdio.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(15), async {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let directory =
            std::env::temp_dir().join(format!("rpc-server-test-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let socket = directory.join("rpc.sock");
        for mode in ["tcp", "unix", "stdio"] {
            let (mut process, target) = if mode == "stdio" {
                let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = reserve.local_addr().unwrap().to_string();
                drop(reserve);
                let target = format!("tcp://{address}");
                let process = cli()
                    .args(["forward", "--listen", &target, "--"])
                    .arg(&example)
                    .arg("stdio")
                    .spawn()
                    .unwrap();
                drop(caller(&address).await);
                (process, target)
            } else {
                let address = if mode == "tcp" {
                    "127.0.0.1:0".to_owned()
                } else {
                    socket.to_str().unwrap().to_owned()
                };
                let mut process = Command::new(&example)
                    .args([mode, &address])
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()
                    .unwrap();
                let mut ready = String::new();
                BufReader::new(process.stderr.take().unwrap())
                    .read_line(&mut ready)
                    .await
                    .unwrap();
                let address = ready.trim().strip_prefix("listening on ").unwrap();
                let target = format!("{mode}://{address}");
                (process, target)
            };
            for params in ["[2,3]", "[4294967295,4294967295]"] {
                let output = cli()
                    .args(["request", "sum", "--target", &target, "-p", params])
                    .output()
                    .await
                    .unwrap();
                assert!(output.status.success(), "{mode}: {:?}", output);
                let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(
                    value["value"],
                    if params == "[2,3]" { 5_u64 } else { 8589934590 }
                );
            }
            if mode == "stdio" {
                let status = Command::new("kill")
                    .args(["-TERM", &process.id().unwrap().to_string()])
                    .status()
                    .await
                    .unwrap();
                assert!(status.success());
            } else {
                process.start_kill().unwrap();
            }
            process.wait().await.unwrap();
        }
    })
    .await
    .unwrap();
}
