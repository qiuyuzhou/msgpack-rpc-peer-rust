use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Connection, Error, HandlerContext, RemoteError, RpcParams, RpcPeer, Value, typed};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, RemoteError>> + Send>>;
pub(crate) type Handler = Arc<dyn Fn(HandlerContext, Value) -> HandlerFuture + Send + Sync>;

/// Register typed methods before consuming the builder to start a connection.
#[derive(Default)]
pub struct PeerBuilder {
    pub(crate) diagnostic_events: bool,
    pub(crate) config: crate::Config,
    pub(crate) methods: HashMap<String, Handler>,
    pub(crate) notifications: HashMap<String, Handler>,
}

impl PeerBuilder {
    /// Enable a bounded diagnostic event stream, disabled by default.
    /// Retrieve it once with `Connection::take_events`; counters are always enabled.
    pub fn diagnostic_events(mut self, enabled: bool) -> Self {
        self.diagnostic_events = enabled;
        self
    }

    /// Set limits and deadlines, validated when the connection starts.
    pub fn config(mut self, config: crate::Config) -> Self {
        self.config = config;
        self
    }

    /// Register an asynchronous request handler. `P` is an [`RpcParams`] argument list.
    /// Duplicate names are rejected. Handler errors retain their arbitrary wire value.
    pub fn method<P, R, F, Fut>(
        mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<Self, Error>
    where
        P: RpcParams + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(HandlerContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, RemoteError>> + Send + 'static,
    {
        let name = name.into();
        if self.methods.contains_key(&name) {
            return Err(Error::DuplicateMethod);
        }
        self.methods.insert(name, typed_handler(handler));
        Ok(self)
    }

    /// Register a typed asynchronous notification handler returning `Result<(), RemoteError>`.
    /// `P` is an [`RpcParams`] argument list.
    /// Notification names are independent of request method names; duplicates are rejected.
    /// Handlers execute concurrently, without completion-order or delivery guarantees.
    pub fn notification<P, F, Fut>(
        mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<Self, Error>
    where
        P: RpcParams + Send + 'static,
        F: Fn(HandlerContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), RemoteError>> + Send + 'static,
    {
        let name = name.into();
        if self.notifications.contains_key(&name) {
            return Err(Error::DuplicateMethod);
        }
        self.notifications.insert(name, typed_handler(handler));
        Ok(self)
    }

    /// Attach an already-established stream using the current Tokio runtime.
    /// The returned connection owner must be retained for the connection to stay alive.
    pub fn start<S>(self, stream: S) -> Result<(RpcPeer, Connection), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        crate::peer::start(self, stream)
    }
}

fn typed_handler<P, R, F, Fut>(handler: F) -> Handler
where
    P: RpcParams + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(HandlerContext, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RemoteError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(move |context, params| {
        let handler = handler.clone();
        Box::pin(async move {
            let config = context.config().clone();
            let args = typed::decode_args::<P>(params, config.max_incoming_bytes)?;
            typed::encode_result(handler(context, args).await?, &config)
        })
    })
}
