use super::{
    Failure,
    args::Options,
    output::emit,
    target::Target,
    transport::{Address, Listener},
};
use msgpack_rpc_peer::{Config, Error, RemoteError, RpcPeer, Value};
use std::{error::Error as StdError, ffi::OsString, future::Future, time::Duration};
use tokio::task::JoinSet;

fn forward_error(error: Error) -> RemoteError {
    match error {
        Error::Remote(value) => RemoteError::new(value).unwrap(),
        error => RemoteError::new(Value::from(error.to_string())).unwrap(),
    }
}
async fn forward_session(
    options: &Options,
    listener: Listener,
    target: RpcPeer,
) -> Result<(), Failure> {
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
    loop {
        tokio::select! {
            result = &mut notifying => { result?; return Err("notification forwarding closed".into()); }
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
    let (events, mut receiver) = tokio::sync::mpsc::channel(Config::default().max_handlers);
    let builder = RpcPeer::builder()
        .config(options.config())
        .notification_sink(events);
    let (peer, mut target) = if let Some(address) = address {
        let stream = tokio::select! {
            stream = tokio::time::timeout(Duration::from_secs(options.timeout), address.connect()) => stream??,
            code = &mut signal => return Err(Box::new(Interrupted(code))),
        };
        Target::connected(&builder, stream)?
    } else {
        Target::spawn(&builder, &command).await?
    };
    let output = async {
        while let Some((method, params)) = receiver.recv().await {
            emit("notification", Some(&method), params, options.output, false).await?;
        }
        Ok::<(), Failure>(())
    };
    tokio::pin!(output);
    let (result, drain_output) = {
        let session = forward_session(&options, listener, peer);
        tokio::pin!(session);
        tokio::select! {
            result = target.wait() => (result, true),
            result = &mut session => (result, false),
            result = &mut output => (result.and(Err("notification output closed".into())), false),
            code = &mut signal => (Err(Box::new(Interrupted(code)) as Failure), false),
        }
    };
    drop(builder);
    finish(result, target.cleanup(), async {
        if drain_output {
            tokio::time::timeout(Config::default().graceful_shutdown_timeout, output).await??;
        }
        Ok(())
    })
    .await
}

async fn finish(
    result: Result<(), Failure>,
    cleanup: impl Future<Output = Result<(), Failure>>,
    output: impl Future<Output = Result<(), Failure>>,
) -> Result<(), Failure> {
    let (cleanup, output) = tokio::join!(cleanup, output);
    if let Err(error) = cleanup {
        eprintln!("mrpc: target cleanup failed: {error}");
    }
    if let Err(error) = output {
        eprintln!("mrpc: notification output drain failed: {error}");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;

    #[tokio::test]
    async fn cleanup_and_output_failures_preserve_signal_exit_codes() {
        for code in [130, 143] {
            for cleanup_fails in [false, true] {
                let cleanup = if cleanup_fails {
                    Err("injected child wait failure".into())
                } else {
                    Ok(())
                };
                let error = finish(
                    Err(Box::new(Interrupted(code))),
                    ready(cleanup),
                    ready(Err("injected output failure".into())),
                )
                .await
                .unwrap_err();
                assert_eq!(
                    error.downcast_ref::<Interrupted>().map(|error| error.0),
                    Some(code)
                );
            }
        }
    }
}
