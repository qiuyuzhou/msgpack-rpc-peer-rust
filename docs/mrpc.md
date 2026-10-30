# mrpc test CLI

Install from this checkout (Rust 1.85 or newer):

```sh
cargo install --path . --features cli --bin mrpc --locked
mrpc --help
mrpc request --help
mrpc forward --help
```

The `cli` feature is optional; default library builds do not enable clap,
serde_json, or Tokio's networking, filesystem, and standard-input support.
Addresses use `tcp://host:port` (including `tcp://[::1]:1234`) or, on Unix,
`unix:///absolute/path`. A forwarder never overwrites an existing Unix socket.
Remove its socket after stopping it and confirming it is no longer in use.

## Direct calls

```sh
mrpc request sum --target tcp://127.0.0.1:1234 --params '[2,3]'
mrpc notify refresh --target unix:///tmp/peer.sock
export MRPC_TARGET=unix:///tmp/peer.sock
mrpc request status                       # default params: []
mrpc request update --params-file params.json
printf '["hello"]' | mrpc request echo --params-file -
mrpc request status --target tcp://localhost:1234 --output pretty --timeout 5
```

`--target` overrides `MRPC_TARGET`. `--params` and `--params-file` conflict.
Input must be exactly one JSON array; scalar, object, null, malformed JSON,
and trailing non-whitespace content fail. Without an input option, the CLI
uses `[]` and does not read stdin. File/stdin input is bounded to 1 MiB;
the peer also validates the encoded frame against its default resource limits.

Requests wait up to `--timeout` positive seconds (default 30). Connection
establishment uses the same duration. A timeout ends local waiting and does
not promise to stop remote effects. Each direct command closes its connection.
A successful notification prints nothing and drains and flushes writes before
exiting. There is no protocol-level acknowledgement of reception or execution.

## Shared RPC session

```sh
mrpc forward --listen unix:///tmp/shared.sock --target tcp://127.0.0.1:1234
# In another terminal:
export MRPC_TARGET=unix:///tmp/shared.sock
mrpc request login --params '["test-user","test-password"]'
mrpc request query --params '["current-user"]'
```

Wait for login success before issuing query. Both commands use the same
persistent target connection. Multiple callers can issue concurrent requests;
the forwarder remaps request IDs and restores each caller's original ID when
returning success or remote errors. Both listener and target support Unix/TCP,
including mixed transports. At most 64 caller connections are active; each
uses the library's default bounded handler/queue budgets, while the target
has one shared budget of 64 pending calls. Notification forwarding and output
use separate bounded queues of 64 events. Excess incoming requests follow
the library's overload policy; notifications can be dropped under saturation.

Caller timeout or disconnect leaves the target session alive. Disconnect
cancels that caller's pending handlers; late responses cannot match later
requests. `forward --timeout` bounds forwarded request waits independently of
the calling command's timeout. Target disconnection drains already-received notification output for at most
five seconds, then terminates the forwarder
with a nonzero exit status and closes its callers. It never reconnects or
replays operations.

Target notifications appear on the forwarder's stdout; they are not sent to
callers. Target requests receive the library's unknown-method error because
there is no unambiguous caller for reverse RPC. Direct commands also reject
reverse requests. A notification caller's local flush through a forwarder
is not confirmation that the target received or executed it.

## Output

Successful requests write one JSON record to stdout:

```json
{"type":"result","encoding":"json","value":5}
```

Remote errors write a record to stderr and exit nonzero:

```json
{"type":"remote-error","encoding":"json","value":{"message":"denied"}}
```

Forwarded target notifications use this structure:

```json
{"type":"notification","method":"changed","encoding":"json","params":[1]}
```

Local diagnostics use stderr and a nonzero exit status. `--output compact`
(the default) emits one record per line. `--output pretty` preserves the same
structure with multiline formatting. Each process controls its own output;
caller formatting options do not affect forwarder notifications.

When a complete value cannot be represented losslessly as JSON, `encoding`
is `msgpack-hex`, and `value` (or notification `params`) contains a hex string
encoding that entire single MessagePack value, without the RPC envelope.
This includes binary, extension, invalid UTF-8 strings, maps with non-string
or duplicate keys, and non-finite floats, even inside nested values. Hex
preserves value semantics, not original wire widths or byte layout. JSON
params do not provide a syntax for constructing these special values.
