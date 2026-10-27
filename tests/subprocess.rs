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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Some(mode) = std::env::args()
        .nth(1)
        .filter(|arg| arg.starts_with("--peer") || arg == "--exit" || arg == "--no-input")
    {
        child(&mode).await;
    } else {
        timeout(Duration::from_secs(10), verify()).await.unwrap();
    }
}
