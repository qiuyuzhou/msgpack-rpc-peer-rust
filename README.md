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

The declared MSRV is Rust 1.85: edition 2024 and the selected `rmp`, `rmp-serde`,
`rmpv`, and Tokio versions require it. The locked dependency metadata declares no
higher minimum. Validation used Rust 1.98.1 on macOS; Rust 1.85 is unavailable in
the validation environment and remains unverified. As a compiler-policy check,
the inspected [shadowsocks-rust manifest](https://github.com/shadowsocks/shadowsocks-rust/blob/master/Cargo.toml)
declares Rust 1.91, above this crate's declared minimum. This checks declared
compatibility only; no downstream build or application integration was performed.

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

`PeerBuilder` registers typed asynchronous methods and notifications before
`start` consumes it. Duplicate names fail with `Error::DuplicateMethod`; request
and notification names have separate registries. Startup validates configuration
and requires the current Tokio runtime. It returns a cloneable `RpcPeer` and a
separate `Connection` owner; retain the owner while using the peer.

Arguments are explicit tuples or sequences: `()` for none, `(value,)` for one,
and `(first, second)` for two. Top-level scalars, maps, structs, and nil are
rejected as `Error::InvalidParams`. A structured argument goes inside the list:
`(MyArguments { ... },)`. Structs encode with named fields. A handler declares the
matching list type and returns `Result<R, RemoteError>`; results can be any
supported Serde value. `RemoteError::new(Value)` accepts any non-nil error value.
The crate re-exports `Value` for arbitrary MessagePack data.

Register a one-way handler with
`.notification("event", |_, (value,): (u32,)| async move { Ok(()) })`, then call
`peer.notify("event", (value,)).await`. Success acknowledges local queueing, not
remote delivery, execution, or success. Notifications consume no request ID or
pending-call slot and never receive responses. Unknown notifications are ignored;
invalid typed arguments and other notification failures are counted.

## Concurrency, calls, and cancellation

Handlers receive a `HandlerContext`; `context.request(...)` and `context.notify(...)`
initiate reverse operations while the receive loop continues processing responses.
Requests and notifications share a bounded concurrent handler budget and can finish
out of arrival order. Multiple outstanding outgoing calls correlate responses by ID.
Both endpoints have independent outgoing ID namespaces, so opposite requests with
the same numeric ID do not collide. Incoming request IDs remain active from admission
until their response has been completely written and flushed, including unknown
method and overload responses. Repeating an ID during that interval is a fatal
protocol violation. A remote peer may reuse an incoming ID after that response
completes; this library's own outgoing IDs still never repeat.

`request` has a 30-second whole-call deadline by default. Set
`Config::default_call_timeout`, or use `request_with_timeout(method, args, duration)`
on either peer or handler context. The deadline includes pending-slot and outgoing
queue waits, serialization, and response waiting. Synchronous Serde code and
handler code cannot be preempted by an asynchronous deadline.

Dropping, aborting, or timing out a call removes its pending entry and releases
capacity. Calls canceled before their frame starts writing are skipped. A started
frame finishes while the connection remains active, preserving framing; local
cancellation cannot prevent remote side effects. Late and otherwise unmatched
responses are discarded. Outgoing u32 IDs increase without reuse even after
cancellation; exhaustion returns `Error::ResourceLimit("message ID namespace")`
instead of wrapping. `Error::Timeout` leaves the connection usable.

## Connection lifecycle

`Connection::shutdown()` starts graceful draining with a five-second deadline;
`shutdown_with_timeout(duration)` overrides it. New application calls and
notifications return `Error::Draining`. Operations already admitted, including
capacity waiters, may finish. Active handlers retain reverse-call admission;
cloned contexts lose this privilege when their handler ends. Incoming work remains
admissible within the same bounds because standard messages cannot identify
reverse calls. Sustained traffic never extends the fixed deadline.

Successful draining waits for admitted operations, handlers, queued frames, and
in-flight writes. At deadline expiration, remaining work is canceled and joined;
the owner and pending calls receive `Error::ShutdownTimeout`. `terminate()`
initiates immediate cancellation. `wait()` observes transport release and all
supervised handler termination. Local termination and clean EOF complete the
owner successfully, while outstanding calls fail with `Error::Closed`; malformed
traffic, resource violations, and I/O failures preserve their terminal cause.
EOF can therefore end a drain before all accepted work succeeds.

Dropping the owner initiates cancellation even if peer handles or contexts survive.
A destructor cannot synchronously join tasks. Canceling a `wait` or shutdown future
also drops its owner and initiates cancellation. The supervisor owns the read and
single-writer futures and a bounded set of handler tasks. Internal locks do not
span asynchronous waits.

## Configuration and resource bounds

Pass `Config { ... , ..Config::default() }` to `PeerBuilder::config`. Startup
validates it before spawning work.

| Field | Default | Meaning |
| --- | --- | --- |
| `max_incoming_bytes` | 1 MiB | Encoded bytes per incoming MessagePack object |
| `max_outgoing_bytes` | 1 MiB | Encoded bytes per complete outgoing envelope |
| `max_depth` | 64 | Nested containers, including an incoming envelope |
| `max_nodes` | 65,536 | Decoded values, including map keys and envelope fields |
| `max_pending_calls` | 64 | Outgoing calls awaiting queueing or responses |
| `max_handlers` | 64 | Shared active request and notification handlers |
| `application_queue_capacity` | 64 | Reserved or queued application frames |
| `response_queue_capacity` | 64 | Queued internally generated responses |
| `default_call_timeout` | 30 seconds | Whole-call deadline |
| `graceful_shutdown_timeout` | 5 seconds | Fixed drain deadline |

Byte and node limits must be positive. Depth must be between 1 and 1024. Work and
queue capacities must be positive and within Tokio's semaphore limit. Default
deadlines must be positive and representable by the monotonic clock; unsupported
per-operation deadline overrides also return `Error::InvalidConfig`.

Application operations wait for bounded pending or queue capacity. The receive
loop never waits for a handler or writer slot: excess requests receive an overload
error and excess notifications are dropped and counted. Full response queues
terminate with `Error::ResourceLimit("response queue")`. The writer alternates
preference between application and response queues. Response processing remains
independent of handler execution and sustained application writes. Finite budgets
allow bounded overload failure rather than guaranteed successful arbitrary nesting.

The incremental scanner checks MessagePack headers before allocating decoded
containers. It rejects hostile advertised string, binary, extension, array, and map
lengths against byte and node budgets. Encoded buffering is bounded by the incoming
byte limit. Fragmented and concatenated objects need no delimiter or length prefix.
Arguments and results also undergo depth/node checks before conversion to dynamic
values; the complete outgoing envelope is byte bounded during serialization.
Unknown-length Serde sequences, maps, and formatted strings obey the same encoding
byte limit without unbounded staging buffers. Rejected outgoing calls and
notifications emit no partial frame. Oversized or
unserializable results and errors use a generic remote failure if it fits; otherwise
the connection terminates with `Error::Encode` instead of stranding a remote call.

Queue slots are reserved before application serialization, so capacity waiters
retain no library-encoded frame. Reserved queue slots, the single active writer,
active handlers and their result conversions, and the reader have separate bounded
costs. Active incoming IDs are bounded by admitted handlers, queued responses,
and the one in-flight response. Byte buffers, value nodes, queue counts, and active-work counts do not specify
allocator overhead or a total process-memory bound. Application-provided arguments,
custom Serde implementations, transport implementations, and handlers remain
responsible for their own allocations and CPU use.

## Errors and diagnostics

Local `Error` categories distinguish `Transport`, `Decode`, `Protocol`, `Encode`,
`InvalidParams`, `InvalidResult`, `ResourceLimit`, `Timeout`, `Draining`,
`ShutdownTimeout`, `Closed`, `DuplicateMethod`, `InvalidConfig`, and `NoRuntime`.
`Error::Remote(Value)` preserves an arbitrary non-nil remote failure without
assuming a schema. Valid unknown methods and invalid typed arguments produce
remote failures while leaving the connection usable. Malformed envelopes,
duplicate active incoming IDs, invalid MessagePack, truncated EOF, and fatal wire
resource violations terminate the connection and resolve pending calls.

Library-generated remote errors use `{ "code": integer, "message": string }`:

| Code | Message | Trigger |
| --- | --- | --- |
| -32601 | `unknown method` | No registered request method |
| -32602 | `invalid parameters` | Typed argument decoding failed |
| -32603 | `handler failed` | Request handler panic under unwinding |
| -32603 | `result serialization failed` | Result encoding or conversion failed |
| -32603 | `response serialization failed` | Complete response needed a fallback |
| -32000 | `handler capacity exhausted` | Shared handler budget exhausted |

These codes are library conventions, not MessagePack-RPC standard codes.
Notification errors and panics have no response. Under panic unwinding, handler
invocation and future execution are isolated in supervised tasks. Panic-abort builds
cannot recover from panics.

`peer.diagnostics()` returns fixed counters for handler panics, notification
failures, dropped notifications, and lost events, even when observation is disabled.
`peer.dropped_notifications()` reads the drop counter directly. Snapshots read
counters independently. Enable `builder.diagnostic_events(true)`, then take the
optional stream once with `connection.take_events()` and await `events.recv()`.
It holds at most `DIAGNOSTIC_EVENT_CAPACITY` (64) fixed enum events; a full or dropped
receiver increments `lost_events`. Disabled observation loses no events. Producers
never wait for event consumers or spawn additional tasks. The stream closes when
the connection terminates, even if handles survive.

Library-generated handler errors and diagnostic events exclude parameters,
remote method names, and panic payloads. This does not control application logging,
application-provided remote errors, custom serializer errors, or the process panic
hook, which executes before a panic is caught.

## Wire interoperability and transports

The [MessagePack-RPC specification](https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md)
defines the envelopes used here:

- Request: `[0, u32_id, string_method, array_params]`.
- Response: `[1, u32_id, error, result]`.
- Notification: `[2, string_method, array_params]`.

Success has nil error; failure has a non-nil error and nil result. Messages are
consecutive MessagePack objects with no extra framing, handshake, or custom types.
Named struct fields and explicit argument lists make the Rust contract usable by
an independent peer. Tests inspect literal independent request, response, and
notification fixtures, arbitrary remote errors, fragmented headers/payloads, and
concatenated objects; two-library round trips are additional behavior tests.

Attach any owned `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream. Unix
sockets use the same API on supported platforms. `SplitIo::new(reader, writer)`
pairs separate handles, such as child stdout and stdin, without taking ownership
of the process or adding buffering. Reserve subprocess stdout for RPC bytes and
send diagnostics to stderr. See the [transport guide](docs/transports.md) and accepted
[architecture overview](docs/architecture.md).

## Dependencies and verification

The crate adds no runtime owner, networking framework, logging framework, or
lifecycle dependency:

| Dependency | Purpose and enabled features |
| --- | --- |
| Tokio | Generic async I/O, task supervision, bounded channels, and deadlines; `io-util`, `rt`, `sync`, `macros`, `time` |
| Serde | Typed contracts; derive support is enabled only for development |
| rmp-serde | Typed MessagePack decoding and serialization with named struct fields |
| rmpv | Dynamic MessagePack values and arbitrary remote errors; `with-serde` |
| thiserror | Clonable local error categories |

Tokio's `net`, `process`, and `io-std` features are enabled only for tests and
examples. There are no crate-specific optional features. Reproduce validation:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo run --example duplex
cargo run --example bidirectional
cargo run --example nested
cargo run --example unix
cargo run --example subprocess
```

The Unix example runs only on Unix and reports that limitation elsewhere.
Subprocess tests use a separate executable harness so test output cannot corrupt
RPC stdout. Validation includes actual Unix sockets and child processes, bounded
queue saturation, cancellation, panic diagnostics, and owned shutdown. Compiler
and platform coverage beyond the validation environment remain unverified.

## Remaining limitations

There is no Swift implementation, application integration, reconnect/retry policy,
connection pool, runtime ownership, process management, authentication, encryption,
remote cancellation, shutdown handshake, mutable registry, reliable notification
delivery, or notification completion ordering. Applications supply transport
security and any stronger ordering or acknowledgment policy. The library cannot
preempt blocking synchronous application code, bound application-owned allocations,
recover from panic-abort, or synchronously join work from a destructor.
