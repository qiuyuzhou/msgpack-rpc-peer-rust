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

`PeerBuilder` registers typed asynchronous methods and notifications, and `start`
consumes it, validates the configuration, and requires the current Tokio runtime.
It returns a cloneable `RpcPeer` and a separate `Connection` owner; keep the owner
alive while you use the peer. Duplicate names fail with `Error::DuplicateMethod`,
and request and notification names have separate registries.

Arguments are explicit tuples or sequences: `()` for none, `(value,)` for one,
`(first, second)` for two. Top-level scalars, maps, structs, and nil are rejected
as `Error::InvalidParams`. A structured argument goes inside the list as
`(MyArguments { ... },)` and encodes with named fields. A handler declares the
matching list type implementing `RpcParams` and returns `Result<R, RemoteError>`;
results can be any supported Serde value. `RemoteError::new(Value)` accepts any
non-nil error value. The crate re-exports `Value` for arbitrary MessagePack data.

Handler registration checks `RpcParams` at compile time. Implementations cover
`()`, tuples of 1–16 elements, `Vec<T>`, and the arrays Serde supports. Custom
types can implement `RpcParams` if they deserialize from an argument array.
Incoming values and argument counts are still validated at runtime.

Register a one-way handler with
`.notification("event", |_, (value,): (u32,)| async move { Ok(()) })`, then call
`peer.notify("event", (value,)).await`. A successful await means the notification
is queued locally; it does not confirm remote delivery, execution, or success.
Notifications consume no request ID or pending-call slot and never receive
responses. Unknown notifications are ignored; invalid typed arguments and other
notification failures are counted.

## Concurrency, calls, and cancellation

Handlers receive a `HandlerContext`; `context.request(...)` and
`context.notify(...)` initiate reverse operations while the receive loop continues
processing responses. Requests and notifications share a bounded concurrent
handler budget and can finish out of arrival order. Multiple outstanding outgoing
calls correlate responses by ID. Both endpoints have independent outgoing ID
namespaces, so opposite requests with the same numeric ID do not collide. An
incoming request ID stays active from admission until its response is written and
flushed, including unknown-method and overload responses; repeating the ID in
that window is a fatal protocol violation. After the response completes, the
remote peer may reuse the ID, though this library's outgoing IDs never repeat.

`request` has a 30-second whole-call deadline by default. Set
`Config::default_call_timeout`, or use `request_with_timeout(method, args,
duration)` on either peer or handler context. The deadline includes pending-slot
and outgoing-queue waits, serialization, and response waiting. Synchronous Serde
code and handler code cannot be preempted by an asynchronous deadline.

Dropping, aborting, or timing out a call removes its pending entry and releases
capacity. Calls canceled before their frame starts writing are skipped. A started
frame finishes while the connection remains active, preserving framing; local
cancellation cannot prevent remote side effects. Late and unmatched responses are
discarded. Outgoing u32 IDs increase without reuse even after cancellation;
exhaustion returns `Error::ResourceLimit("message ID namespace")` instead of
wrapping. `Error::Timeout` leaves the connection usable.

## Connection lifecycle

`Connection::shutdown()` starts graceful draining with a five-second deadline;
`shutdown_with_timeout(duration)` overrides it. New application calls and
notifications return `Error::Draining`, while operations already admitted,
including capacity waiters, may finish. Active handlers retain reverse-call
admission; cloned contexts lose it when their handler ends. Incoming work stays
admissible within the same bounds, because standard messages cannot identify
reverse calls. Sustained traffic never extends the fixed deadline.

Successful draining waits for admitted operations, handlers, queued frames, and
in-flight writes. At the deadline, remaining work is canceled and joined, and the
owner and pending calls receive `Error::ShutdownTimeout`. `terminate()` cancels
immediately. `wait()` resolves when the transport is released and all supervised
handlers have ended. Local termination and clean EOF complete the owner
successfully, and outstanding calls fail with `Error::Closed`; malformed traffic,
resource violations, and I/O failures keep their terminal cause, so EOF can end a
drain before all accepted work succeeds.

Dropping the owner initiates cancellation even if peer handles or contexts
survive. A destructor cannot synchronously join tasks, and canceling a `wait` or
shutdown future also drops its owner and cancels. The supervisor owns the read
and single-writer futures and a bounded set of handler tasks. Internal locks do
not span asynchronous waits.

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

Byte and node limits must be positive, and depth must be between 1 and 1024. Work
and queue capacities must be positive and within Tokio's semaphore limit. Default
deadlines must be positive and representable by the monotonic clock; unsupported
per-operation deadline overrides also return `Error::InvalidConfig`.

Application operations wait for bounded pending or queue capacity. The receive
loop never waits for a handler or writer slot: excess requests get an overload
error, and excess notifications are dropped and counted. A full response queue
terminates the connection with `Error::ResourceLimit("response queue")`. The
writer alternates preference between the application and response queues, so
response processing stays independent of handler execution and sustained
application writes. Budgets are finite, so overload fails in a bounded way; the
library does not guarantee success for arbitrary nesting.

The incremental scanner checks MessagePack headers before allocating decoded
containers, and rejects hostile advertised string, binary, extension, array, and
map lengths against the byte and node budgets. Encoded buffering is bounded by
the incoming byte limit, and fragmented or concatenated objects need no delimiter
or length prefix. Arguments and results also pass depth and node checks before
conversion to dynamic values, and the complete outgoing envelope stays within the
byte limit during serialization. Unknown-length Serde sequences, maps, and
formatted strings obey the same encoding byte limit without unbounded staging
buffers. Rejected outgoing calls and notifications emit no partial frame.
Oversized or unserializable results and errors use a generic remote failure if it
fits; otherwise the connection terminates with `Error::Encode` instead of
stranding a remote call.

Queue slots are reserved before application serialization, so capacity waiters
hold no library-encoded frame. Reserved slots, the single active writer, active
handlers and their result conversions, and the reader each have separate bounded
costs. Active incoming IDs are bounded by admitted handlers, queued responses,
and the one in-flight response. The byte, node, queue, and active-work counts do
not include allocator overhead or bound total process memory. Application-provided
arguments, custom Serde implementations, transport implementations, and handlers
remain responsible for their own allocations and CPU use.

## Errors and diagnostics

Local `Error` has the categories `Transport`, `Decode`, `Protocol`, `Encode`,
`InvalidParams`, `InvalidResult`, `ResourceLimit`, `Timeout`, `Draining`,
`ShutdownTimeout`, `Closed`, `DuplicateMethod`, `InvalidConfig`, and `NoRuntime`.
`Error::Remote(Value)` preserves an arbitrary non-nil remote failure without
assuming a schema. Valid unknown methods and invalid typed arguments produce
remote failures and leave the connection usable. Malformed envelopes, duplicate
active incoming IDs, invalid MessagePack, truncated EOF, and fatal wire resource
violations terminate the connection and resolve pending calls.

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
invocation and future execution are isolated in supervised tasks; panic-abort
builds cannot recover from panics.

`peer.diagnostics()` returns fixed counters for handler panics, notification
failures, dropped notifications, and lost events, even when observation is
disabled. `peer.dropped_notifications()` reads the drop counter directly, and
snapshots read counters independently. To observe events, enable
`builder.diagnostic_events(true)`, take the optional stream once with
`connection.take_events()`, and await `events.recv()`. The stream holds at most
`DIAGNOSTIC_EVENT_CAPACITY` (64) fixed enum events; a full or dropped receiver
increments `lost_events`. Disabled observation loses no events, producers never
wait for consumers or spawn extra tasks, and the stream closes when the
connection terminates, even if handles survive.

Library-generated handler errors and diagnostic events exclude parameters, remote
method names, and panic payloads. They do not cover application logging,
application-provided remote errors, custom serializer errors, or the process
panic hook, which runs before a panic is caught.

## Wire interoperability and transports

The [MessagePack-RPC specification](https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md)
defines the envelopes used here:

- Request: `[0, u32_id, string_method, array_params]`.
- Response: `[1, u32_id, error, result]`.
- Notification: `[2, string_method, array_params]`.

Success has a nil error; failure has a non-nil error and a nil result. Messages
are consecutive MessagePack objects with no extra framing, handshake, or custom
types. Named struct fields and explicit argument lists keep the Rust contract
usable by an independent peer. Tests use literal independent fixtures for
requests, responses, and notifications, plus arbitrary remote errors, fragmented
headers and payloads, concatenated objects, and two-library round trips.

Attach any owned `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream; Unix
sockets use the same API on supported platforms. `SplitIo::new(reader, writer)`
pairs separate handles, such as child stdout and stdin, without owning the
process or adding buffering. Reserve subprocess stdout for RPC bytes and send
diagnostics to stderr. See the [transport guide](docs/transports.md) and the
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
examples. There are no crate-specific optional features.

The [GitHub Actions CI workflow](.github/workflows/ci.yml) runs on pushes, pull
requests, and manual dispatch. It checks formatting, Clippy, and documentation on
stable Rust, runs tests including doctests on Linux and macOS, and checks all
targets with Rust 1.85 on Linux. Cargo commands use `--locked` to validate the
committed dependency versions.

### Commit checks

Install [pre-commit](https://pre-commit.com/#installation),
[actionlint](https://github.com/rhysd/actionlint#installation), and the Rust
components, then enable the hooks for this checkout:

```sh
rustup component add rustfmt clippy
pre-commit install
pre-commit run --all-files
```

The [pre-commit configuration](.pre-commit-config.yaml) checks staged whitespace
and conflict markers on every commit. Rust source or manifest changes trigger
format checking; Rust source, dependency, toolchain, or Cargo configuration
changes trigger Clippy with warnings denied, including examples. Workflow changes
trigger actionlint. Hooks check files without fixing them; tests, documentation
builds, and MSRV checks stay in CI, and hooks do not run examples.

`pre-commit install` without `--overwrite` preserves existing hooks through
pre-commit's migration mode. During a commit, pre-commit temporarily hides
unstaged changes to tracked files, but Cargo still checks the whole crate and may
see untracked files. Manual `--all-files` runs check the working tree; the
staged-diff hook always checks the index.

### Local verification

Run the stable checks and examples locally:

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
cargo run --locked --example duplex
cargo run --locked --example bidirectional
cargo run --locked --example nested
cargo run --locked --example unix
cargo run --locked --example subprocess
```

The `unix` example runs only on Unix. Subprocess tests use a separate executable
harness so test output cannot corrupt RPC stdout. Tests cover real Unix sockets
and child processes, bounded queue saturation, cancellation, panic diagnostics,
and owned shutdown. Compiler and platform coverage beyond this validation
environment remains unverified.
