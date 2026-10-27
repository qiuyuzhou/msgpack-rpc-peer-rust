use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Connection, Error, HandlerContext, RemoteError, RpcPeer, Value, wire};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, RemoteError>> + Send>>;
pub(crate) type Handler = Arc<dyn Fn(HandlerContext, Value) -> HandlerFuture + Send + Sync>;

/// Register typed methods before consuming the builder to start a connection.
#[derive(Default)]
pub struct PeerBuilder {
    pub(crate) config: crate::Config,
    pub(crate) methods: HashMap<String, Handler>,
    pub(crate) notifications: HashMap<String, Handler>,
}

impl PeerBuilder {
    /// Set resource limits, validated when the connection starts.
    pub fn config(mut self, config: crate::Config) -> Self {
        self.config = config;
        self
    }

    /// Register an asynchronous request handler. `A` is a tuple or sequence of arguments.
    /// Duplicate names are rejected. Handler errors retain their arbitrary wire value.
    pub fn method<A, R, F, Fut>(
        mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<Self, Error>
    where
        A: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(HandlerContext, A) -> Fut + Send + Sync + 'static,
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
    /// Notification names are independent of request method names; duplicates are rejected.
    /// Handlers execute concurrently, without completion-order or delivery guarantees.
    pub fn notification<A, F, Fut>(
        mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<Self, Error>
    where
        A: DeserializeOwned + Send + 'static,
        F: Fn(HandlerContext, A) -> Fut + Send + Sync + 'static,
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

fn typed_handler<A, R, F, Fut>(handler: F) -> Handler
where
    A: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(HandlerContext, A) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RemoteError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(move |context, params| {
        let handler = handler.clone();
        Box::pin(async move {
            let args = A::deserialize(crate::arguments::ListDeserializer(params))
                .map_err(|_| RemoteError::builtin(-32602, "invalid parameters"))?;
            let config = context.config().clone();
            encode_result(handler(context, args).await?, &config)
        })
    })
}

fn encode_result<R: Serialize>(result: R, config: &crate::Config) -> Result<Value, RemoteError> {
    let bytes = wire::encode(&result, config.max_outgoing_bytes)
        .map_err(|_| RemoteError::builtin(-32603, "result serialization failed"))?;
    wire::value(bytes, config)
        .map_err(|_| RemoteError::builtin(-32603, "result serialization failed"))
}
