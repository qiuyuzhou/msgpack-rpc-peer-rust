# msgpack-rpc-peer

Typed asynchronous MessagePack-RPC requests and notifications between equal peers
over an existing Tokio byte stream. Either peer can initiate and answer requests on the same
connection. The library creates no runtime and establishes no transport.

This initial implementation supports concurrent typed requests, notifications,
nested reverse calls, and immediate connection termination.
Encoded bytes, decoded nodes, and nesting are bounded before incoming values are allocated.
See the resource limits and remaining lifecycle limitations below.

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

`PeerBuilder::config(Config { ..Config::default() })` sets wire limits. Defaults
are 1 MiB per incoming object and complete outgoing envelope, 64 nested containers
(including the envelope), and 65,536 decoded value nodes (including map keys and
envelope fields). Limits must be positive; supported depth is at most 1024 to bound
recursive value decoding. Startup returns `Error::InvalidConfig` for invalid limits.

The incremental scanner checks MessagePack headers before allocating decoded
containers, rejecting hostile advertised string, binary, extension, array, and map
lengths against byte and node budgets. Stream fragmentation and consecutive objects
need no delimiter or length prefix. Read buffering never exceeds the configured
incoming byte length. Decoded values have count and nesting bounds; these are not
a bound on allocator overhead or application memory.

Encoding enforces the outgoing byte limit as bytes are written, including methods
and error values. Argument/result conversion also checks depth and node budgets
before creating dynamic values; the final outgoing envelope is byte bounded.
Rejected calls and notifications send no partial frame. Oversized or unserializable
results produce a generic remote failure; oversized error envelopes fall back to
that failure. If even the generic response cannot fit, the connection terminates
with `Error::Encode`, rather than leaving the remote request waiting indefinitely.
Custom Serde code and application handlers control their own CPU and allocations;
asynchronous deadlines cannot preempt synchronous application code.

Dropping a request future removes its pending entry. Already queued or started
writes can still reach the remote peer; cancellation does not prevent remote side
effects. Unmatched responses are ignored. No whole-call timeout exists yet: use
`tokio::time::timeout` at the application boundary when needed. A started write
continues independently of call cancellation while the connection remains active.

Configurable work capacity limits,
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

### Handler failures and diagnostics

Unknown request methods return `{"code": -32601, "message": "unknown method"}`;
invalid typed arguments return code `-32602` and message `invalid parameters`.
Handler panics return code `-32603` and message `handler failed`. Result or response
serialization failures also use `-32603` with `result serialization failed` or
`response serialization failed` when the fallback response fits the configured
limit. Overload uses `-32000`, `handler capacity exhausted`. These codes are local
library conventions, not MessagePack-RPC standard codes. Arbitrary non-nil remote
error values are preserved in `Error::Remote`; applications must interpret them.

`peer.diagnostics()` returns fixed counters for handler panics, notification
failures, dropped notifications and lost diagnostic events. Counters remain active
without event observation. Enable `builder.diagnostic_events(true)` before startup,
then call `connection.take_events()` once and await `events.recv()`. The optional
queue holds at most 64 fixed `DiagnosticEvent` variants. A full or dropped receiver
increments `lost_events`; disabled observation does not. Diagnostics never wait
for event consumers or spawn extra tasks. Snapshots read each counter independently.
The event stream closes when the connection terminates, even if peer handles survive.

Under panic unwinding, request and notification handler invocation and future
execution are isolated in supervised tasks; subsequent requests can still succeed.
Notification errors (including invalid arguments) and panics are counted separately.
Default library errors generated for handlers and diagnostic events contain no
parameters, remote method names, or application panic payloads. This does not
control application logging, arbitrary application-provided remote error values,
or the process panic hook, which runs before a panic is caught. Panic-abort builds
cannot recover from a handler panic.

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

`Connection::shutdown()` atomically starts draining with the configured five-second
`Config::graceful_shutdown_timeout`; `shutdown_with_timeout` supplies a fixed override.
New application calls and notifications return `Error::Draining`. Operations already
admitted, including those waiting for pending or outgoing capacity, may finish.
Admitted handlers can make reverse calls and notifications while draining; cloned
handler contexts lose that privilege when their handler ends. Incoming work remains
admissible within the same limits because the wire cannot identify reverse calls.
Traffic does not extend the deadline. Success requires all admitted operations,
handlers, queued frames, and in-flight writes to finish. Deadline expiration returns
`Error::ShutdownTimeout` after canceling and joining owned work; pending calls see
the same cause. EOF still completes the owner successfully and fails outstanding
calls with `Error::Closed`; other terminal failures preserve their cause.

`terminate()` cancels immediately. `wait()` and either shutdown operation observe
transport release and all supervised handler termination. Dropping the owner,
including canceling its `wait` or shutdown future, initiates cancellation but cannot
synchronously join tasks. Surviving peer handles and contexts do not keep transport
execution alive. Queue capacity is reserved before outgoing serialization, so
producers waiting for capacity retain no library-encoded frame. Reserved queue
slots, the single active writer, active handlers, and bounded typed conversions
are separate contributors to library-owned memory; application-provided arguments,
custom serializers, and handler allocations remain application responsibilities.
