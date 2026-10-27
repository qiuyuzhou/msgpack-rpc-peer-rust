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
