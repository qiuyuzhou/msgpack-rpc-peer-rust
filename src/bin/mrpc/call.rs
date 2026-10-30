use super::{Failure, args::Call, output::emit};
use msgpack_rpc_peer::{Config, Error, RpcPeer, Value};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};

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
pub(super) async fn run(call: Call, notification: bool) -> Result<(), Failure> {
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
