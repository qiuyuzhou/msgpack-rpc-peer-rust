# mrpc test CLI

Install from this checkout (Rust 1.85 or newer):

```sh
cargo install --path . --features cli --bin mrpc --locked
mrpc --help
mrpc request --help
mrpc forward --help
```

The optional `cli` feature enables clap, serde_json, and Tokio's networking,
filesystem, process, signal, and standard-input support. Default library builds leave them
disabled. Use `tcp://host:port` (including `tcp://[::1]:1234`) for TCP
addresses or `unix:///absolute/path` for Unix addresses. A forwarder never
overwrites an existing Unix socket. After stopping the forwarder, confirm that
its socket is no longer in use before removing it.

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

`--target`, `--output` (`-o`), and `--timeout` override `MRPC_TARGET`, `MRPC_OUTPUT`, and `MRPC_TIMEOUT`, respectively. `--params` also has the short form `-p`. `--params` and `--params-file` cannot be used together.
Input must be exactly one JSON array. Scalars, objects, null, malformed JSON,
and trailing non-whitespace content are rejected. Without an input option, the
CLI uses `[]` and does not read stdin. File and stdin input are limited to
1 MiB; the peer also checks the encoded frame against its default resource limits.

Requests wait for up to `--timeout` seconds, which must be positive and default
to 30. Connection establishment uses the same duration. A timeout ends local
waiting but does not guarantee that remote effects stop. Each direct command
closes its connection. A successful notification prints nothing and drains and
flushes writes before exiting. The protocol does not acknowledge reception or
execution.

## Shared RPC session

```sh
mrpc forward --listen unix:///tmp/shared.sock --target tcp://127.0.0.1:1234
# In another terminal:
export MRPC_TARGET=unix:///tmp/shared.sock
mrpc request login --params '["test-user","test-password"]'
mrpc request query --params '["current-user"]'
```

Wait for login to succeed before issuing the query. Both commands use the same
persistent target connection. Multiple callers can issue concurrent requests;
the forwarder remaps request IDs and restores each caller's original ID when
returning successful responses or remote errors. The listener and target both
support Unix and TCP, including mixed transports. At most 64 caller connections
are active. Each uses the library's default bounded handler and queue budgets,
while the target has one shared budget of 64 pending calls. Notification
forwarding and output use separate bounded queues of 64 events. Excess incoming
requests follow the library's overload policy; notifications can be dropped
under saturation.

Caller timeout or disconnect leaves the target session alive. Disconnect
cancels that caller's pending handlers; late responses cannot match later
requests. `forward --timeout` limits forwarded request waits independently of
the calling command's timeout. When the target disconnects, the forwarder
stops accepting callers and closes their connections, then drains
already-received notification output for at most five seconds before exiting
with a nonzero status. It never reconnects or replays operations.

Target notifications appear on the forwarder's stdout and are not sent to
callers. Target requests receive the library's unknown-method error because
there is no unambiguous caller for reverse RPC. Direct commands also reject
reverse requests. A notification caller's local flush through a forwarder
is not confirmation that the target received or executed it.

### Subprocess target

```sh
mrpc forward --listen unix:///tmp/shared.sock -- my-rpc-program --arg value
```

Only `forward` accepts a command after `--`. It starts one subprocess shared by
all callers and exchanges binary MessagePack-RPC through its stdin and stdout.
Arguments are passed directly, without shell interpretation; invoke `sh -c`
explicitly if a shell is needed. The subprocess inherits the working directory,
environment, and stderr, so diagnostics must go to stderr rather than stdout.

A command overrides `MRPC_TARGET`. An explicit `--target` together with a
command, or `--` without a program, is an error. Process creation and pipe setup
define startup; there is no readiness handshake. Unresponsive peers use the
existing request timeout and invalid traffic uses the library's protocol policy.

Subprocess exit, even with status zero, or stdout EOF ends the shared session
and makes the forwarder exit with status 1. The subprocess status is reported
when available. The forwarder never restarts the subprocess or replays calls.
On Ctrl-C or Unix SIGTERM it stops accepting callers, closes the RPC connection
and subprocess stdin, and returns status 130 or 143 respectively. Cleanup on
every exit path waits up to five seconds for the direct child to exit, then
forcibly terminates and reaps it. This grace period is independent of
`--timeout`; signals are not explicitly relayed, and descendant processes are
not managed.

After subprocess exit or stdout EOF, notification output drains concurrently
with child cleanup, so blocked output does not delay child reaping. Signal
exits do not drain additional notifications. Cleanup and output-drain failures
are reported separately and do not replace the original exit cause or status.

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
emits one record per line. `--output pretty` (the default) preserves the same
structure with multiline formatting. Each process controls its own output;
caller formatting options do not affect forwarder notifications.

When a complete value cannot be represented losslessly as JSON, `encoding`
is `msgpack-hex`. The `value` (or notification `params`) then contains a hex
string encoding the entire MessagePack value without the RPC envelope. This
applies to binary and extension values, invalid UTF-8 strings, maps with
non-string or duplicate keys, and non-finite floats, even when nested.
Hex preserves value semantics, not original wire widths or byte layout. JSON
params do not provide a syntax for constructing these special values.
