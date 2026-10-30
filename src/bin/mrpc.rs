use clap::{Args, Parser, Subcommand, ValueEnum};
use msgpack_rpc_peer::{Config, Error, RemoteError, RpcPeer, SplitIo, Value};
use std::{error::Error as StdError, ffi::OsString, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    task::JoinSet,
};

#[path = "mrpc/output.rs"]
mod output;
type Failure = Box<dyn StdError + Send + Sync>;
trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
type Socket = Box<dyn Stream>;

#[derive(Parser)]
#[command(
    name = "mrpc",
    version,
    about = "MessagePack-RPC calls and shared sessions"
)]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}
#[derive(Subcommand)]
enum Operation {
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
struct Options {
    #[arg(long, help = "Target address [env: MRPC_TARGET]")]
    target: Option<String>,
    #[arg(long, env = "MRPC_TIMEOUT", default_value = "30", value_parser = clap::value_parser!(u64).range(1..))]
    timeout: u64,
    #[arg(short, long, env = "MRPC_OUTPUT", value_enum, default_value = "pretty")]
    output: Output,
}
#[derive(Clone, Copy, ValueEnum)]
enum Output {
    Compact,
    Pretty,
}
#[derive(Args)]
struct Call {
    method: String,
    #[command(flatten)]
    options: Options,
    #[arg(short, long, conflicts_with = "params_file")]
    params: Option<String>,
    /// JSON array file; use - to read stdin to EOF.
    #[arg(long)]
    params_file: Option<String>,
}

enum Address {
    Tcp(String),
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}
impl Address {
    fn parse(text: &str) -> Result<Self, Failure> {
        if let Some(address) = text.strip_prefix("tcp://") {
            if let Some((host, port)) = address.rsplit_once(':') {
                if !host.is_empty() && !host.contains('/') && port.parse::<u16>().is_ok() {
                    return Ok(Self::Tcp(address.into()));
                }
            }
        }
        #[cfg(unix)]
        if let Some(path) = text.strip_prefix("unix://") {
            if std::path::Path::new(path).is_absolute() {
                return Ok(Self::Unix(path.into()));
            }
        }
        Err("address must be tcp://host:port or unix:///absolute/path".into())
    }
    async fn connect(&self) -> Result<Socket, Failure> {
        match self {
            Self::Tcp(address) => Ok(Box::new(TcpStream::connect(address).await?)),
            #[cfg(unix)]
            Self::Unix(path) => Ok(Box::new(tokio::net::UnixStream::connect(path).await?)),
        }
    }
}
enum Listener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
}
impl Listener {
    async fn bind(address: Address) -> Result<Self, Failure> {
        match address {
            Address::Tcp(address) => Ok(Self::Tcp(TcpListener::bind(address).await?)),
            #[cfg(unix)]
            Address::Unix(path) => Ok(Self::Unix(tokio::net::UnixListener::bind(path)?)),
        }
    }
    async fn accept(&self) -> Result<Socket, Failure> {
        match self {
            Self::Tcp(listener) => Ok(Box::new(listener.accept().await?.0)),
            #[cfg(unix)]
            Self::Unix(listener) => Ok(Box::new(listener.accept().await?.0)),
        }
    }
}

impl Options {
    fn address(&self) -> Result<Address, Failure> {
        let target = self
            .target
            .clone()
            .or_else(|| std::env::var("MRPC_TARGET").ok())
            .ok_or("--target or MRPC_TARGET is required")?;
        Address::parse(&target)
    }
    fn config(&self) -> Config {
        Config {
            default_call_timeout: Duration::from_secs(self.timeout),
            ..Config::default()
        }
    }
}
async fn params(call: &Call) -> Result<Value, Failure> {
    // Bound file and stdin staging just like the peer's default frame budget.
    let limit = Config::default().max_outgoing_bytes;
    let bytes = if let Some(text) = &call.params {
        text.as_bytes().to_vec()
    } else if let Some(path) = &call.params_file {
        let reader: Box<dyn AsyncRead + Unpin + Send> = if path == "-" {
            Box::new(tokio::io::stdin())
        } else {
            Box::new(tokio::fs::File::open(path).await?)
        };
        let mut bytes = Vec::new();
        reader
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .await?;
        bytes
    } else {
        b"[]".to_vec()
    };
    if bytes.len() > limit {
        return Err("params exceed message byte limit".into());
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    if !value.is_array() {
        return Err("params must be a JSON array".into());
    }
    Ok(rmpv::ext::to_value(value)?)
}
async fn emit(
    kind: &str,
    method: Option<&str>,
    value: Value,
    format: Output,
    error: bool,
) -> Result<(), Failure> {
    let record = output::record(kind, method, value)?;
    let text = match format {
        Output::Compact => serde_json::to_string(&record)?,
        Output::Pretty => serde_json::to_string_pretty(&record)?,
    };
    let bytes = format!("{text}\n");
    if error {
        let mut output = tokio::io::stderr();
        output.write_all(bytes.as_bytes()).await?;
        output.flush().await?;
    } else {
        let mut output = tokio::io::stdout();
        output.write_all(bytes.as_bytes()).await?;
        output.flush().await?;
    }
    Ok(())
}
async fn call(call: Call, notification: bool) -> Result<(), Failure> {
    let target = call.options.address()?;
    let params = params(&call).await?;
    let stream =
        tokio::time::timeout(Duration::from_secs(call.options.timeout), target.connect()).await??;
    let (peer, owner) = RpcPeer::builder()
        .config(call.options.config())
        .start(stream)?;
    if notification {
        peer.notify_value(&call.method, params).await?;
        owner.shutdown().await?;
        return Ok(());
    }
    let result = peer.request_value(&call.method, params).await;
    owner.terminate();
    owner.wait().await?;
    match result {
        Ok(value) => emit("result", None, value, call.options.output, false).await,
        Err(Error::Remote(value)) => {
            emit(
                "remote-error",
                None,
                value.clone(),
                call.options.output,
                true,
            )
            .await?;
            Err(Error::Remote(value).into())
        }
        Err(error) => Err(error.into()),
    }
}
fn forward_error(error: Error) -> RemoteError {
    match error {
        Error::Remote(value) => RemoteError::new(value).unwrap(),
        error => RemoteError::new(Value::from(error.to_string())).unwrap(),
    }
}
async fn forward_session(
    options: &Options,
    listener: Listener,
    stream: Socket,
    child: &mut Option<Child>,
) -> Result<(), Failure> {
    let format = options.output;
    let (events, mut receiver) = tokio::sync::mpsc::channel(Config::default().max_handlers);
    let builder = RpcPeer::builder()
        .config(options.config())
        .notification_sink(events);
    let output = async move {
        while let Some((method, params)) = receiver.recv().await {
            emit("notification", Some(&method), params, format, false).await?;
        }
        Ok::<(), Failure>(())
    };
    tokio::pin!(output);
    let (target, owner) = builder.start(stream)?;
    let requests = target.clone();
    let (notifications, mut notification_rx) =
        tokio::sync::mpsc::channel::<(String, Value)>(Config::default().application_queue_capacity);
    let notifying = async {
        while let Some((method, params)) = notification_rx.recv().await {
            if let Err(error) = target.notify_value(&method, params).await {
                eprintln!("mrpc: notification forwarding failed: {error}");
            }
        }
        Ok::<(), Error>(())
    };
    tokio::pin!(notifying);
    let callers = RpcPeer::builder()
        .config(options.config())
        .fallback_method(move |_, method, params| {
            let peer = requests.clone();
            async move {
                peer.request_value(&method, params)
                    .await
                    .map_err(forward_error)
            }
        })
        .notification_sink(notifications);
    let mut connections = JoinSet::new();
    let mut waiting = Box::pin(owner.wait());
    loop {
        tokio::select! {
            status = async {
                match child.as_mut() {
                    Some(child) => child.wait().await,
                    None => std::future::pending().await,
                }
            } => {
                drop(waiting);
                drop(builder);
                tokio::time::timeout(Config::default().graceful_shutdown_timeout, &mut output).await??;
                return Err(format!("subprocess exited: {}; shared session ended", status?).into());
            }
            result = &mut output => { result?; return Err("notification output closed".into()); }
            result = &mut notifying => { result?; return Err("notification forwarding closed".into()); }
            result = &mut waiting => {
                drop(builder);
                tokio::time::timeout(Config::default().graceful_shutdown_timeout, &mut output).await??;
                result?;
                return Err("target connection closed; shared session ended".into());
            }
            stream = listener.accept(), if connections.len() < Config::default().max_pending_calls => {
                let (_, owner) = callers.start(stream?)?;
                connections.spawn(owner.wait());
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}
fn stop_signal() -> Result<impl std::future::Future<Output = i32>, Failure> {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => 130,
                _ = terminate.recv() => 143,
            }
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async {
            let _ = tokio::signal::ctrl_c().await;
            130
        })
    }
}

#[derive(Debug)]
struct Interrupted(i32);
impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "forward interrupted")
    }
}
impl StdError for Interrupted {}

async fn forward(options: Options, listen: String, command: Vec<OsString>) -> Result<(), Failure> {
    if !command.is_empty() && options.target.is_some() {
        return Err("--target conflicts with a subprocess command".into());
    }
    if command.is_empty()
        && std::env::args_os().last().as_deref() == Some(std::ffi::OsStr::new("--"))
    {
        return Err("-- requires a subprocess program".into());
    }
    let address = if command.is_empty() {
        Some(options.address()?)
    } else {
        None
    };
    let listener = Listener::bind(Address::parse(&listen)?).await?;
    let signal = stop_signal()?;
    tokio::pin!(signal);
    let mut child = None;
    let stream: Socket = if let Some(address) = address {
        tokio::select! {
            stream = tokio::time::timeout(Duration::from_secs(options.timeout), address.connect()) => stream??,
            code = &mut signal => return Err(Box::new(Interrupted(code))),
        }
    } else {
        let mut process = Command::new(&command[0])
            .args(&command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stream = SplitIo::new(
            process.stdout.take().unwrap(),
            process.stdin.take().unwrap(),
        );
        child = Some(process);
        Box::new(stream)
    };
    let result = {
        let session = forward_session(&options, listener, stream, &mut child);
        tokio::pin!(session);
        tokio::select! {
            result = &mut session => result,
            code = &mut signal => Err(Box::new(Interrupted(code)) as Failure),
        }
    };
    if let Some(child) = child.as_mut() {
        let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(status) => status?,
            Err(_) => {
                child.kill().await?;
                child.wait().await?
            }
        };
        eprintln!("mrpc: subprocess exited: {status}");
    }
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = match Cli::parse().command {
        Operation::Request(args) => call(args, false).await,
        Operation::Notify(args) => call(args, true).await,
        Operation::Forward {
            options,
            listen,
            command,
        } => forward(options, listen, command).await,
    };
    if let Err(error) = result {
        if !matches!(error.downcast_ref::<Error>(), Some(Error::Remote(_))) {
            eprintln!("mrpc: {error}");
        }
        std::process::exit(
            error
                .downcast_ref::<Interrupted>()
                .map_or(1, |signal| signal.0),
        );
    }
}
