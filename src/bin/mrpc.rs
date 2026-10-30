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
#[path = "mrpc/transport.rs"]
mod transport;

type Failure = Box<dyn StdError + Send + Sync>;

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
