# Transport integration

The application establishes a transport and passes an owned Tokio `AsyncRead +
AsyncWrite + Unpin + Send + 'static` stream to `PeerBuilder::start`. Connection
establishment does not assign RPC roles: either endpoint can initiate requests
and notifications on the same connection. Retain the returned connection owner
and use `terminate` followed by `wait` for immediate cancellation and completed
cleanup.

## Unix domain sockets

On Unix platforms, attach an established `tokio::net::UnixStream` directly. Use
`UnixListener::bind`/`accept` and `UnixStream::connect` when communicating between
processes; the application owns socket path selection and cleanup. The executable
`cargo run --example unix` demonstrates equal peers using a socket pair. The
integration test also exercises a real filesystem listener and connecting peer.
Unix coverage is conditionally compiled; it does not provide Windows coverage.

## Subprocess stdin/stdout

Pair the child's stdout reader and stdin writer with
`SplitIo::new(child.stdout.take().unwrap(), child.stdin.take().unwrap())` and
attach it with the same builder API. Inside the child, pair `tokio::io::stdin()`
and `tokio::io::stdout()`. The adapter adds no framing or buffering. Its shutdown
operation delegates to the supplied writer; dropping the connection releases
both owned handles. The library owns neither a Tokio runtime nor a child process.

`cargo run --example subprocess` spawns another copy of the executable and makes
a request whose handler calls back into the spawning process. Reserve child
stdout exclusively for RPC bytes; send diagnostics to stderr. The application
must choose spawning, waiting, termination, timeout and cleanup policy. The example
uses `kill_on_drop(true)` as a cleanup fallback and explicitly waits for child exit
after closing RPC. A production child may need an application-specific exit
method or explicit application-owned termination instead. A process that retains
stdout after exiting its RPC loop can delay EOF.

The subprocess integration executable uses its own child mode, so normal Rust
test-harness output cannot corrupt the pipe. It tests bidirectional RPC, child
exit with a pending call, connection completion, and explicit process reaping.
Write-side broken-pipe coverage additionally closes a real child's input on Unix.
Tokio's `net`, `process`, and `io-std` features are development-only requirements
for these tests and examples; the core dependency uses generic asynchronous I/O.
