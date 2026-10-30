use super::{Failure, transport::Socket};
use msgpack_rpc_peer::{Connection, PeerBuilder, RpcPeer, SplitIo};
use std::{ffi::OsString, process::Stdio, time::Duration};
use tokio::process::{Child, Command};

pub(super) struct Target {
    owner: Option<Connection>,
    child: Option<Child>,
}

impl Target {
    pub(super) fn connected(
        builder: &PeerBuilder,
        stream: Socket,
    ) -> Result<(RpcPeer, Self), Failure> {
        let (peer, owner) = builder.start(stream)?;
        Ok((
            peer,
            Self {
                owner: Some(owner),
                child: None,
            },
        ))
    }

    pub(super) async fn spawn(
        builder: &PeerBuilder,
        command: &[OsString],
    ) -> Result<(RpcPeer, Self), Failure> {
        let mut child = Command::new(&command[0])
            .args(&command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stream = SplitIo::new(child.stdout.take().unwrap(), child.stdin.take().unwrap());
        let mut target = Self {
            owner: None,
            child: Some(child),
        };
        match builder.start(stream) {
            Ok((peer, owner)) => {
                target.owner = Some(owner);
                Ok((peer, target))
            }
            Err(error) => {
                if let Err(cleanup) = target.cleanup().await {
                    eprintln!("mrpc: target cleanup failed: {cleanup}");
                }
                Err(error.into())
            }
        }
    }

    pub(super) async fn wait(&mut self) -> Result<(), Failure> {
        let owner = self.owner.take().unwrap();
        tokio::select! {
            status = async {
                match self.child.as_mut() {
                    Some(child) => child.wait().await,
                    None => std::future::pending().await,
                }
            } => Err(format!("subprocess exited: {}; shared session ended", status?).into()),
            result = owner.wait() => {
                result?;
                Err("target connection closed; shared session ended".into())
            }
        }
    }

    pub(super) async fn cleanup(mut self) -> Result<(), Failure> {
        // Close the RPC connection and stdin before giving the child its exit grace period.
        drop(self.owner.take());
        if let Some(child) = self.child.as_mut() {
            let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
                Ok(status) => status?,
                Err(_) => {
                    child.kill().await?;
                    child.wait().await?
                }
            };
            eprintln!("mrpc: subprocess exited: {status}");
        }
        Ok(())
    }
}
