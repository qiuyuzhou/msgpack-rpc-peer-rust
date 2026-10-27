# msgpack-rpc-peer

Typed asynchronous MessagePack-RPC between equal peers over an existing Tokio byte
stream. Either endpoint can initiate requests and notifications, answer requests,
and make reverse calls from a handler. The application supplies the runtime,
transport, and any child-process lifecycle policy.

## Installation

Use this checkout as a path dependency until a registry release is available:

```toml
[dependencies]
msgpack-rpc-peer = { path = "../msgpack-rpc-peer" }
tokio = { version = "1.53", features = ["rt", "macros", "io-util"] }
```

The MSRV is Rust 1.85: edition 2024 and the selected `rmp`, `rmp-serde`, `rmpv`,
and Tokio versions require it, and the lockfile declares no higher minimum.

## Basic usage

```rust,no_run
use msgpack_rpc_peer::RpcPeer;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (left, right) = tokio::io::duplex(4096);
    let (a, a_connection) = RpcPeer::builder()
        .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })?
        .start(left)?;
    let (b, b_connection) = RpcPeer::builder()
        .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })?
        .start(right)?;

    let (sum, doubled) = tokio::join!(
        a.request::<_, u32>("sum", (2, 3)),
        b.request::<_, u32>("double", (7,)),
    );
    assert_eq!((sum?, doubled?), (5, 14));

    let (a_done, b_done) = tokio::join!(a_connection.shutdown(), b_connection.shutdown());
    a_done?;
    b_done?;
    Ok(())
}
```

Build a `PeerBuilder` with typed methods and notifications, then `start` it on a
stream. `start` returns a cloneable `RpcPeer` and a separate `Connection` owner
that supervises the connection.

The API documentation, built with `cargo doc --open`, covers argument
conventions, timeouts and cancellation, the connection lifecycle, configuration
limits, error categories, and diagnostics.

## Guides

- [Transport integration](docs/transports.md)
- [Architecture overview](docs/architecture.md)
- [Development](docs/development.md)
