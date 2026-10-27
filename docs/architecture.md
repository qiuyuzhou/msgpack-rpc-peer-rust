# Architecture

Status: accepted decisions through ADR 0010 are implemented by the Rust crate, with executable transport examples and behavior-level verification.

## Equal peers

Both endpoints have identical request, response, and notification capabilities. Transport establishment and process ownership belong to the application. The wire format follows the [MessagePack-RPC specification](https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md), using consecutive MessagePack objects without an additional framing header.

## Public API design

A builder accepts configuration and typed request and notification handlers before startup. Startup on the caller's Tokio runtime returns a cloneable `RpcPeer` handle and a separate connection owner. The connection owner supervises execution and provides graceful shutdown, immediate termination, and observable completion; dropping it initiates cancellation. Registration is immutable after startup.

Calls and notifications accept explicit tuple or sequence argument lists; handlers decode the corresponding list. Empty arguments use `()`, one structured argument uses `(argument,)`, and structured arguments use named fields. Handler context exposes outgoing RPC operations, including reverse calls during draining. Ordinary peer handles reject new application calls once draining begins. The implementation must keep these two admission paths explicit.

Generic `AsyncRead + AsyncWrite + Unpin + Send + 'static` streams are accepted directly. A small adapter pairs separate read and write halves for subprocess stdio. Unix sockets require no special RPC implementation. The library creates no runtime and owns no child process.

## Execution and ownership

A reader incrementally frames and validates incoming messages, correlates responses, and admits incoming handlers without awaiting handler capacity or write capacity. A single writer fairly services bounded application and response queues and completes each started frame. A supervisor owns reader, writer, and handler tasks; no handler or transport task is detached from its lifecycle.

Request and notification handlers share one concurrency budget. Excess requests receive overload errors; excess notifications are dropped and counted. Full response queues terminate the connection. No arbitrary nesting depth is guaranteed to succeed under finite handler limits.

Pending outgoing calls are bounded and correlated by unsigned 32-bit IDs that are never reused on the same connection. Cancellation removes the pending entry; late and otherwise unmatched responses are discarded. Incoming IDs belong to the other peer's independent namespace. Duplicate active incoming IDs terminate the connection.

## Failure and shutdown

Typed local errors distinguish transport, decode, protocol, timeout, closure, resource exhaustion, invalid parameters, unknown method, and remote method failures. Arbitrary remote error values are preserved. Built-in remote errors use library-defined `code` and `message` fields; they are not protocol-standard codes. Default diagnostics omit parameters and application panic payloads.

Graceful shutdown rejects new application calls, drains admitted work, permits handler reverse calls, and continues admitting incoming requests within limits. A fixed drain deadline bounds the process even under sustained traffic. The deadline or immediate termination cancels remaining tasks, fails pending calls, and closes transport. Owner drop initiates cancellation but cannot synchronously await task completion.

## Initial defaults

All limits are configurable and validated at startup. Zero capacities and invalid combinations are rejected.

| Setting | Default |
| --- | --- |
| Maximum encoded message size, incoming and outgoing | 1 MiB |
| Maximum MessagePack nesting depth | 64 |
| Maximum decoded value nodes per message | 65,536 |
| Maximum pending outgoing calls | 64 |
| Maximum concurrently executing handlers | 64 |
| Application outgoing queue | 64 messages |
| Internal response queue | 64 messages |
| Default whole-call timeout | 30 seconds |
| Graceful shutdown deadline | 5 seconds |

Queues contain size-limited frames and active decodes obey byte, depth, and node limits, so message counts alone are not presented as memory bounds. Diagnostics use fixed counters and a bounded event channel with an explicit lost-event count; callers opt into receiving events. Remote method strings and parameter values are not included by default. Custom serializers and handlers remain responsible for their own CPU use and allocations.

## Implementation scope

Keep a single crate with modules for the peer lifecycle, codec, wire messages, registry, transport adapter, and errors as needed. Use Tokio, Serde, rmp-serde, rmpv, and thiserror only where each has a concrete role; add no runtime, networking framework, logging framework, or protocol extension. Resolve the MSRV from the selected dependency versions and check compatibility with the intended initial consumer before documenting it.

Verification includes independent wire fixtures, malformed and fragmented inputs, symmetric and nested calls, queue saturation, cancellation and stale responses, panic isolation, shutdown task ownership, duplex streams, Unix sockets, and a subprocess stdio fixture. Examples demonstrate the same transports and symmetric behavior. Run formatting, warning-free Clippy across all targets and features, the full suite, and every example before reporting implementation complete.
