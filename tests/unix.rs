#![cfg(unix)]

use msgpack_rpc_peer::{Error, RpcPeer};
use tokio::{
    net::{UnixListener, UnixStream},
    time::{Duration, timeout},
};

#[tokio::test]
async fn connected_and_accepted_peers_have_equal_capabilities_and_observe_eof() {
    timeout(Duration::from_secs(3), async {
        let directory =
            std::env::temp_dir().join(format!("msgpack-rpc-unix-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let path = directory.join("rpc.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let (connected, accepted) = tokio::join!(UnixStream::connect(&path), listener.accept());
        let (a, owner_a) = RpcPeer::builder()
            .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })
            .unwrap()
            .start(connected.unwrap())
            .unwrap();
        let (b, owner_b) = RpcPeer::builder()
            .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })
            .unwrap()
            .start(accepted.unwrap().0)
            .unwrap();
        let (sum, doubled) = tokio::join!(
            a.request::<_, u32>("sum", (2, 3)),
            b.request::<_, u32>("double", (7,))
        );
        assert_eq!(sum.unwrap(), 5);
        assert_eq!(doubled.unwrap(), 14);
        owner_a.terminate();
        owner_a.wait().await.unwrap();
        owner_b.wait().await.unwrap();
        assert_eq!(b.request::<_, ()>("sum", ()).await, Err(Error::Closed));
    })
    .await
    .unwrap();
}
