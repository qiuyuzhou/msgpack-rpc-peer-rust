use super::{Failure, transport::Address};
use clap::{Args, Parser, Subcommand, ValueEnum};
use msgpack_rpc_peer::Config;
use std::{ffi::OsString, time::Duration};

#[derive(Parser)]
#[command(
    name = "mrpc",
    version,
    about = "MessagePack-RPC calls and shared sessions"
)]
pub(super) struct Cli {
    #[command(subcommand)]
    pub(super) command: Operation,
}
#[derive(Subcommand)]
pub(super) enum Operation {
    /// Send a request and print its result.
    Request(Call),
    /// Send a notification and flush the connection (no execution acknowledgement).
    Notify(Call),
    /// Share one persistent target session across bounded caller connections.
    Forward {
        #[command(flatten)]
        options: Options,
        #[arg(long)]
        listen: String,
        /// Program and arguments; stdin/stdout carry MessagePack-RPC.
        #[arg(last = true, num_args = 1..)]
        command: Vec<OsString>,
    },
}
#[derive(Args)]
pub(super) struct Options {
    #[arg(long, help = "Target address [env: MRPC_TARGET]")]
    pub(super) target: Option<String>,
    #[arg(long, env = "MRPC_TIMEOUT", default_value = "30", value_parser = clap::value_parser!(u64).range(1..))]
    pub(super) timeout: u64,
    #[arg(short, long, env = "MRPC_OUTPUT", value_enum, default_value = "pretty")]
    pub(super) output: Output,
}
#[derive(Clone, Copy, ValueEnum)]
pub(super) enum Output {
    Compact,
    Pretty,
}
#[derive(Args)]
pub(super) struct Call {
    pub(super) method: String,
    #[command(flatten)]
    pub(super) options: Options,
    #[arg(short, long, conflicts_with = "params_file")]
    pub(super) params: Option<String>,
    /// JSON array file; use - to read stdin to EOF.
    #[arg(long)]
    pub(super) params_file: Option<String>,
}

impl Options {
    pub(super) fn address(&self) -> Result<Address, Failure> {
        let target = self
            .target
            .clone()
            .or_else(|| std::env::var("MRPC_TARGET").ok())
            .ok_or("--target or MRPC_TARGET is required")?;
        Address::parse(&target)
    }
    pub(super) fn config(&self) -> Config {
        Config {
            default_call_timeout: Duration::from_secs(self.timeout),
            ..Config::default()
        }
    }
}
