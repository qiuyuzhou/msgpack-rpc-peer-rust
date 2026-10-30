use msgpack_rpc_peer::{Error, RpcPeer, SplitIo};
use std::process::Stdio;
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    time::{Duration, timeout},
};

fn spawn(mode: &str) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .arg(mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn child(mode: &str) {
    if mode == "--exit" {
        let mut byte = [0];
        tokio::io::stdin().read_exact(&mut byte).await.unwrap();
        return;
    }
    let (_peer, owner) = RpcPeer::builder()
        .method("calculate", |context, (n,): (u32,)| async move {
            let doubled: u32 = context.request("double", (n,)).await.unwrap();
            Ok(doubled + 1)
        })
        .unwrap()
        .start(SplitIo::new(tokio::io::stdin(), tokio::io::stdout()))
        .unwrap();
    owner.wait().await.unwrap();
}

async fn verify() {
    let mut child = spawn("--peer");
    let stream = SplitIo::new(child.stdout.take().unwrap(), child.stdin.take().unwrap());
    let (peer, owner) = RpcPeer::builder()
        .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })
        .unwrap()
        .start(stream)
        .unwrap();
    assert_eq!(peer.request::<_, u32>("calculate", (7,)).await.unwrap(), 15);
    owner.terminate();
    owner.wait().await.unwrap();
    assert!(child.wait().await.unwrap().success());

    let mut child = spawn("--exit");
    let (peer, owner) = RpcPeer::builder()
        .start(SplitIo::new(
            child.stdout.take().unwrap(),
            child.stdin.take().unwrap(),
        ))
        .unwrap();
    assert_eq!(peer.request::<_, ()>("wait", ()).await, Err(Error::Closed));
    owner.wait().await.unwrap();
    assert!(child.wait().await.unwrap().success());

    #[cfg(unix)]
    {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 0<&-; printf ready; exec sleep 30")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut ready = [0; 5];
        stdout.read_exact(&mut ready).await.unwrap();
        assert_eq!(&ready, b"ready");
        let (peer, owner) = RpcPeer::builder()
            .start(SplitIo::new(stdout, child.stdin.take().unwrap()))
            .unwrap();
        let result = peer.request::<_, ()>("wait", ()).await;
        assert!(matches!(result, Err(Error::Transport(_))));
        assert_eq!(owner.wait().await, result);
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }
}

#[cfg(feature = "cli")]
async fn forward_peer() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    assert_eq!(
        std::env::args().nth(2).as_deref(),
        Some("literal $HOME ; spaces")
    );
    assert_eq!(std::env::var("MRPC_FIXTURE_ENV").unwrap(), "inherited");
    let state = Arc::new(AtomicBool::new(false));
    let login = state.clone();
    let (_peer, owner) = RpcPeer::builder()
        .method("login", move |_, (): ()| {
            login.store(true, Ordering::SeqCst);
            async { Ok(true) }
        })
        .unwrap()
        .method("query", move |_, (): ()| {
            let logged_in = state.load(Ordering::SeqCst);
            async move { Ok(logged_in) }
        })
        .unwrap()
        .method("exit", |_, (): ()| async {
            std::process::exit(0);
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap()
        .start(SplitIo::new(tokio::io::stdin(), tokio::io::stdout()))
        .unwrap();
    owner.wait().await.unwrap();
}

#[cfg(feature = "cli")]
async fn verify_forward() {
    use tokio::net::{TcpListener, TcpStream};
    let reserve = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reserve.local_addr().unwrap();
    drop(reserve);
    let mut forward = Command::new(env!("CARGO_BIN_EXE_mrpc"))
        .args(["forward", "--listen", &format!("tcp://{address}"), "--"])
        .arg(std::env::current_exe().unwrap())
        .args(["--forward-peer", "literal $HOME ; spaces"])
        .env("MRPC_TARGET", "invalid")
        .env("MRPC_FIXTURE_ENV", "inherited")
        .env_remove("MRPC_TIMEOUT")
        .env_remove("MRPC_OUTPUT")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    for method in ["login", "query", "exit"] {
        let stream = loop {
            if let Ok(stream) = TcpStream::connect(address).await {
                break stream;
            }
            assert!(
                forward.try_wait().unwrap().is_none(),
                "forward exited before accepting calls"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let (peer, owner) = RpcPeer::builder().start(stream).unwrap();
        let result = peer.request::<_, bool>(method, ()).await;
        if method == "exit" {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap());
        }
        owner.terminate();
        owner.wait().await.unwrap();
    }
    let output = forward.wait_with_output().await.unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("subprocess exited"));
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    #[cfg(all(feature = "cli", unix))]
    if std::env::args().nth(1).as_deref() == Some("--last-notification") {
        let notification = msgpack_rpc_peer::Value::Array(vec![
            2.into(),
            "last-event".into(),
            msgpack_rpc_peer::Value::Array(vec!["x".repeat(800_000).into()]),
        ]);
        rmpv::encode::write_value(&mut std::io::stdout(), &notification).unwrap();
        return;
    }
    #[cfg(feature = "cli")]
    if std::env::args().nth(1).as_deref() == Some("--forward-peer") {
        forward_peer().await;
        return;
    }
    if let Some(mode) = std::env::args()
        .nth(1)
        .filter(|arg| arg.starts_with("--peer") || arg == "--exit" || arg == "--no-input")
    {
        child(&mode).await;
    } else {
        timeout(Duration::from_secs(10), verify()).await.unwrap();
        #[cfg(feature = "cli")]
        timeout(Duration::from_secs(10), verify_forward())
            .await
            .unwrap();
        #[cfg(all(feature = "cli", unix))]
        timeout(Duration::from_secs(10), verify_blocked_output_cleanup())
            .await
            .unwrap();
    }
}

#[cfg(all(feature = "cli", unix))]
async fn verify_blocked_output_cleanup() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut forward = Command::new(env!("CARGO_BIN_EXE_mrpc"))
        .args([
            "forward",
            "--listen",
            "tcp://127.0.0.1:0",
            "--",
            "/bin/sh",
            "-c",
            "\"$1\" --last-notification; exec 1>&-; echo ready >&2; cat >/dev/null",
            "sh",
        ])
        .arg(std::env::current_exe().unwrap())
        .env_remove("MRPC_TARGET")
        .env_remove("MRPC_TIMEOUT")
        .env_remove("MRPC_OUTPUT")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(forward.stderr.take().unwrap()).lines();
    assert_eq!(stderr.next_line().await.unwrap().as_deref(), Some("ready"));
    // Leave stdout unread: draining a large notification must not delay reaping.
    let reaped = timeout(Duration::from_secs(2), stderr.next_line())
        .await
        .expect("child cleanup waited for blocked notification output")
        .unwrap()
        .unwrap();
    assert!(reaped.contains("subprocess exited"), "{reaped}");
    assert_eq!(forward.wait().await.unwrap().code(), Some(1));
}
