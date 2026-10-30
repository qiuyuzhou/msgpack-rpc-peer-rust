// Run with `cargo run --locked --example rpc-server -- <transport> ...`.
// stdout is reserved for RPC bytes; readiness and diagnostics use stderr.
use clap::{Parser, Subcommand};
use msgpack_rpc_peer::{PeerBuilder, RpcPeer, SplitIo};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Parser)]
#[command(
    name = "rpc-server",
    about = "Serve sum over stdio, TCP, or Unix sockets"
)]
struct Args {
    #[command(subcommand)]
    transport: Transport,
}

#[derive(Subcommand)]
enum Transport {
    /// Use stdin/stdout until the RPC connection closes.
    Stdio,
    /// Listen on an IP address and port (use port 0 for an assigned port).
    Tcp { address: std::net::SocketAddr },
    /// Listen on a Unix socket; the caller owns socket file cleanup.
    #[cfg(unix)]
    Unix { path: std::path::PathBuf },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let registry = RpcPeer::builder().method("sum", |_, (a, b): (u32, u32)| async move {
        Ok(u64::from(a) + u64::from(b))
    })?;

    match args.transport {
        Transport::Stdio => {
            let (_peer, owner) =
                registry.start(SplitIo::new(tokio::io::stdin(), tokio::io::stdout()))?;
            owner.wait().await?;
        }
        Transport::Tcp { address } => {
            let listener = tokio::net::TcpListener::bind(address).await?;
            eprintln!("listening on {}", listener.local_addr()?);
            loop {
                let (stream, _) = listener.accept().await?;
                serve_connection(&registry, stream)?;
            }
        }
        #[cfg(unix)]
        Transport::Unix { path } => {
            let listener = tokio::net::UnixListener::bind(&path)?;
            eprintln!("listening on {}", path.display());
            loop {
                let (stream, _) = listener.accept().await?;
                serve_connection(&registry, stream)?;
            }
        }
    }
    Ok(())
}

fn serve_connection(
    registry: &PeerBuilder,
    stream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
) -> Result<(), msgpack_rpc_peer::Error> {
    let (_peer, owner) = registry.start(stream)?;
    tokio::spawn(async move {
        if let Err(error) = owner.wait().await {
            eprintln!("connection failed: {error}");
        }
    });
    Ok(())
}
