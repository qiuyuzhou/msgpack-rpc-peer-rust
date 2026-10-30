use super::{
    Failure,
    args::Options,
    output::emit,
    transport::{Address, Listener, Socket},
};
use msgpack_rpc_peer::{Config, Error, RemoteError, RpcPeer, SplitIo, Value};
use std::{error::Error as StdError, ffi::OsString, process::Stdio, time::Duration};
use tokio::{
    process::{Child, Command},
    task::JoinSet,
};

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
pub(super) struct Interrupted(pub(super) i32);
impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "forward interrupted")
    }
}
impl StdError for Interrupted {}

pub(super) async fn run(
    options: Options,
    listen: String,
    command: Vec<OsString>,
) -> Result<(), Failure> {
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
