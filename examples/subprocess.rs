use msgpack_rpc_peer::{RpcPeer, SplitIo};
use std::process::Stdio;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("--peer") {
        // stdout is exclusively RPC bytes. Diagnostics belong on stderr.
        let (_peer, owner) = RpcPeer::builder()
            .method("calculate", |context, (n,): (u32,)| async move {
                let doubled: u32 = context.request("double", (n,)).await.unwrap();
                Ok(doubled + 1)
            })?
            .start(SplitIo::new(tokio::io::stdin(), tokio::io::stdout()))?;
        owner.wait().await?;
    } else {
        let mut child = tokio::process::Command::new(std::env::current_exe()?)
            .arg("--peer")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let (peer, owner) = RpcPeer::builder()
            .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })?
            .start(SplitIo::new(
                child.stdout.take().unwrap(),
                child.stdin.take().unwrap(),
            ))?;
        let answer: u32 = peer.request("calculate", (7,)).await?;
        assert_eq!(answer, 15);
        eprintln!("subprocess returned {answer} after a reverse call");
        owner.terminate();
        owner.wait().await?;
        // The application owns process policy; this fixture exits when its input closes.
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await??;
        assert!(status.success());
    }
    Ok(())
}
