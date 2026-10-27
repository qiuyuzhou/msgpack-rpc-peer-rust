# msgpack-rpc-peer

Typed asynchronous MessagePack-RPC requests and notifications between equal peers
over an existing Tokio byte stream. Either peer can initiate and answer requests on the same
connection. The library creates no runtime and establishes no transport.

This initial implementation supports concurrent typed requests, notifications,
nested reverse calls, and immediate connection termination.
It is not yet suitable for untrusted streams. See the limitations below.

## Installation

Until a registry release is available, add this checkout as a path dependency:

```toml
[dependencies]
msgpack-rpc-peer = { path = "../msgpack-rpc-peer" }
tokio = { version = "1.53", features = ["rt", "macros", "io-util"] }
```

The declared minimum Rust version is 1.85 (edition 2024 and the minimum required
by the selected MessagePack and Tokio releases). Validation currently uses Rust
1.98.1; Rust 1.85 and downstream application compatibility have not been tested.

## Usage

```rust,no_run
use msgpack_rpc_peer::RpcPeer;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (left, right) = tokio::io::duplex(4096);
let (a, a_connection) = RpcPeer::builder()
    .method("double", |_, (n,): (u32,)| async move { Ok(n * 2) })?
    .start(left)?;
let (b, b_connection) = RpcPeer::builder()
    .method("sum", |_, (a, b): (u32, u32)| async move { Ok(a + b) })?
    .start(right)?;

let sum: u32 = a.request("sum", (2, 3)).await?;
let doubled: u32 = b.clone().request("double", (7,)).await?;
assert_eq!((sum, doubled), (5, 14));

a_connection.terminate();
a_connection.wait().await?;
b_connection.terminate();
b_connection.wait().await?;
# Ok(())
# }
```

Run the complete example with `cargo run --example duplex`.
Use `cargo run --example bidirectional` for concurrent requests and notifications
in both directions, or `cargo run --example nested` for a reverse-call chain.

Registration consumes and returns the builder. Duplicate names fail locally.
Request and notification names have separate registries, so the same name may be
used for both. Starting consumes the builder, so both registries are immutable afterward.
Any owned `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream can be used.
Startup requires the application's current Tokio runtime.

Arguments are explicit tuple or sequence lists: `()` for none, `(value,)` for one,
and `(first, second)` for two. Scalars, maps, structs, and nil at the list's outer
level are rejected locally as `Error::InvalidParams`. Put a struct *inside* the
list to encode its named fields: `(MyArguments { ... },)`. Handlers declare the
same tuple or sequence type, or `()` for an empty list. Results can be any
Serde-serializable type. Struct results also use named fields.

Handlers return `Result<R, RemoteError>` and receive a `HandlerContext` for reverse
calls. `context.request("method", args).await` continues to make progress while
the receive loop processes responses. For application errors, use
`RemoteError::new(value)`; nil is rejected because it denotes success on the wire.
`Value` is re-exported for arbitrary MessagePack values.

Register notifications with `.notification("event", |context, (value,): (u32,)|
async move { Ok(()) })`. Their typed handlers return `Result<(), RemoteError>`.
Send with `peer.notify("event", (value,)).await` or `context.notify(...)`.
Success means the frame was queued locally; it does not acknowledge delivery,
execution, or handler success. Notifications allocate no request ID or pending
response entry, and never receive a response, including on handler failure.

Request and notification handlers execute concurrently and may finish out of
arrival order. A handler may await reverse calls while other messages and responses
continue to be processed. Multiple outstanding requests correlate responses by ID,
regardless of completion order. Notifications have no completion-order or delivery
guarantee; applications needing ordering or acknowledgments must implement them
through application methods.

## Ownership and failure

Retain each `Connection` owner. `terminate()` initiates immediate cancellation;
`wait().await` observes transport closure and termination of all supervised
handler tasks. Clean EOF and local termination complete successfully. Owner drop
initiates the same cancellation but cannot await cleanup. Peer handles and
handler contexts cannot keep a connection alive after its owner is dropped.

The supervisor owns the read and write futures and a bounded `JoinSet` of handler
tasks. It polls responses independently of handlers and write capacity. One writer
serializes complete frames, fairly selecting between separate bounded application
and response queues. No locks span an await. Under panic unwinding a request-handler
panic returns a generic remote error. Notification failures and panics generate no
response and leave the connection running. Panic-abort builds cannot recover from panics.

Local `Error` variants distinguish transport, decoding, protocol, encoding,
parameter/result type, resource, startup, and connection failures. Terminal causes
are propagated to pending calls and future calls. Remote failures remain
`Error::Remote(Value)`, with their original non-nil value, regardless of schema.
Unknown methods and invalid typed arguments return remote errors without closing
the connection. Built-in errors have `{ "code": integer, "message": string }`:

| Code | Meaning |
| --- | --- |
| -32601 | Unknown method |
| -32602 | Invalid parameters |
| -32603 | Handler failure or result serialization failure |
| -32000 | Handler capacity exhausted |

These codes are library conventions. The
[MessagePack-RPC specification](https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md)
does not prescribe an error schema. Requests are `[0, u32_id, method, params]`;
responses are `[1, u32_id, error, result]`; notifications are `[2, method, params]`.
Methods are strings and params are arrays. There is no extra framing header.
Successful responses have nil error; failures have nil result. Outgoing IDs
increase without reuse, and the two peers have independent ID namespaces.

## Current resource bounds and limitations

This slice uses fixed limits: 64 pending outgoing calls, 64 active handler tasks,
64 application frames and 64 response frames. Calls wait for pending/queue
capacity; notifications wait for queue capacity. Requests and notifications share
the handler budget. Excess incoming requests receive overload errors; excess
notifications are dropped. Unknown notifications and notifications with invalid
typed arguments are ignored. A full response queue terminates the connection.
Nested calls can therefore fail under saturation.

Read buffering and encoded output have a 1 MiB byte limit. Encoding enforces the
limit as bytes are written, including complete envelopes. Incoming decoding has
a depth limit of 64. **Decoded container/node allocation is not hardened yet**:
these are not total memory bounds or hostile-input guarantees. Custom Serde code
and application handlers also control their own CPU and allocations.

Dropping a request future removes its pending entry. Already queued or started
writes can still reach the remote peer; cancellation does not prevent remote side
effects. Unmatched responses are ignored. No whole-call timeout exists yet: use
`tokio::time::timeout` at the application boundary when needed. A started write
continues independently of call cancellation while the connection remains active.

Configurable byte/node/capacity limits, hostile-input hardening,
comprehensive saturation and cancellation guarantees, diagnostics, graceful
draining, separate read/write adapters, and Unix/subprocess examples remain for
later implementation slices. There is no reconnect, retry, process ownership, or
application integration.

## Dependencies and verification

- Tokio: asynchronous stream I/O, bounded channels, and supervised task execution.
- Serde: typed argument and result contracts; derive support for examples/tests.
- rmp-serde: bounded MessagePack encoding with named struct fields.
- rmpv: incremental MessagePack values, typed decoding, arbitrary remote errors.
- thiserror: distinct, clonable local error categories.

Tests use the public peer/owner API on controlled streams and literal independent
request/response/notification fixtures. Checks:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo run --example duplex
cargo run --example bidirectional
cargo run --example nested
```

Calls have a 30-second whole-call timeout by default, configurable with
`Config::default_call_timeout`. `request_with_timeout` overrides it for one call,
including waits for pending and outgoing queue capacity. Deadlines cannot preempt
synchronous Serde code or application handlers. Dropping, aborting, or timing out
a call releases its pending entry; queued calls that have not started writing are
skipped. A started frame finishes while the connection remains active, so local
cancellation cannot prevent remote side effects. Late responses are ignored.
Outgoing IDs increase without reuse; exhaustion returns `Error::ResourceLimit`
rather than wrapping. `Error::Timeout` does not terminate the connection.

`Config` defaults to 64 pending calls, 64 concurrent handlers shared by requests
and notifications, and 64 frames in each application and response queue. Zero or
unsupported capacities are rejected at startup. Application operations wait for
queue space; incoming admission never waits for a handler or writer. Excess
requests receive the library-defined `-32000` overload error; excess notifications
are dropped and counted by `RpcPeer::dropped_notifications`. Arbitrarily nested
calls can therefore fail under finite limits. A full internal response queue
terminates the connection with `Error::ResourceLimit("response queue")`. The single
writer alternates preference between queues so neither continuously ready queue
starves the other. Queue lengths bound frame counts, not total process memory;
frames, decoded values, handlers, and application allocations have separate costs.
