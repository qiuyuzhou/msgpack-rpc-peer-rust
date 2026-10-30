//! Command-line entry point for MessagePack-RPC calls and shared sessions.
//!
//! `request` sends a call and prints its result; `notify` sends a notification
//! and flushes the connection without waiting for an execution acknowledgement.
//! `forward` shares a persistent target session with local caller connections.
//! Run `mrpc --help` or `mrpc <command> --help` for arguments and options.

use args::{Cli, Operation};
use clap::Parser;
use msgpack_rpc_peer::Error;
use std::error::Error as StdError;

#[path = "mrpc/args.rs"]
mod args;
#[path = "mrpc/call.rs"]
mod call;
#[path = "mrpc/forward.rs"]
mod forward;
#[path = "mrpc/output.rs"]
mod output;
#[path = "mrpc/target.rs"]
mod target;
#[path = "mrpc/transport.rs"]
mod transport;

/// An error that can be propagated across the asynchronous command modules.
type Failure = Box<dyn StdError + Send + Sync>;

/// Dispatches the selected command and converts failures into process status.
///
/// Remote errors have already been emitted by the command, so they are not
/// printed again. Other failures are reported on stderr. Failures exit with
/// status 1, except interrupted forwarding, which retains its signal status.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = match Cli::parse().command {
        Operation::Request(args) => call::run(args, false).await,
        Operation::Notify(args) => call::run(args, true).await,
        Operation::Forward {
            options,
            listen,
            command,
        } => forward::run(options, listen, command).await,
    };
    if let Err(error) = result {
        if !matches!(error.downcast_ref::<Error>(), Some(Error::Remote(_))) {
            eprintln!("mrpc: {error}");
        }
        std::process::exit(
            error
                .downcast_ref::<forward::Interrupted>()
                .map_or(1, |signal| signal.0),
        );
    }
}
