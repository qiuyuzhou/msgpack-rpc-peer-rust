use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{Connection, Error, HandlerContext, RemoteError, RpcParams, RpcPeer, Value, typed};

type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, RemoteError>> + Send>>;
pub(crate) type Handler = Arc<dyn Fn(HandlerContext, Value) -> HandlerFuture + Send + Sync>;

/// Register typed asynchronous methods and notifications, then reuse the
/// builder to start any number of connections.
///
/// Duplicate names fail with [`Error::DuplicateMethod`], and request and
/// notification names have separate registries. [`Self::start`] validates the
/// configuration, requires the current Tokio runtime, and returns a cloneable
/// [`RpcPeer`] and a separate [`Connection`] owner; keep the owner alive while
/// you use the peer. Calling `start` again reuses the registered handlers for
/// another connection without re-registering them.
#[derive(Clone, Default)]
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

    /// Register an asynchronous request handler with typed params.
    ///
    /// `P` is an [`RpcParams`] params list, checked at compile time. The
    /// handler returns `Result<R, RemoteError>`, and results can be any
    /// supported Serde value. Incoming values and param counts are still
    /// validated at runtime. Handler errors retain their arbitrary wire
    /// value; build one with [`RemoteError::new`]. Duplicate names are
    /// rejected.
    pub fn method<P, R, F, Fut>(
        mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<Self, Error>
    where
        P: RpcParams + DeserializeOwned + Send + 'static,
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

    /// Register a typed asynchronous notification handler returning
    /// `Result<(), RemoteError>`.
    ///
    /// `P` is an [`RpcParams`] params list, checked at compile time.
    /// Handlers execute concurrently, without completion-order or delivery
    /// guarantees. Notification names are independent of request method
    /// names; duplicates are rejected.
    pub fn notification<P, F, Fut>(
        mut self,
        name: impl Into<String>,
        handler: F,
    ) -> Result<Self, Error>
    where
        P: RpcParams + DeserializeOwned + Send + 'static,
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
    ///
    /// Accepts any owned `AsyncRead + AsyncWrite + Unpin + Send + 'static`
    /// stream. The returned connection owner must be retained for the
    /// connection to stay alive. The builder is not consumed: call `start`
    /// once per connection to share the registered handlers and configuration,
    /// for example inside an accept loop. Each connection snapshots the
    /// builder, so later registrations do not affect already-started
    /// connections.
    pub fn start<S>(&self, stream: S) -> Result<(RpcPeer, Connection), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        crate::peer::start(self, stream)
    }
}

fn typed_handler<P, R, F, Fut>(handler: F) -> Handler
where
    P: RpcParams + DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(HandlerContext, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R, RemoteError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    Arc::new(move |context, params| {
        let handler = handler.clone();
        Box::pin(async move {
            let config = context.config().clone();
            let params = typed::decode_params::<P>(params, config.max_incoming_bytes)?;
            typed::encode_result(handler(context, params).await?, &config)
        })
    })
}
