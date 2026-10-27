use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Semaphore, mpsc, oneshot},
    task::{Id, JoinHandle, JoinSet},
};

use crate::{
    Error, PeerBuilder, RemoteError, Value,
    arguments::Arguments,
    wire::{self, Message},
};

const CAPACITY: usize = 64;
type Reply = oneshot::Sender<Result<Value, Error>>;

struct State {
    next_id: u64,
    pending: HashMap<u32, Reply>,
    terminal: Option<Error>,
}

struct Shared {
    state: Mutex<State>,
    outgoing: mpsc::Sender<Vec<u8>>,
    slots: Semaphore,
    config: crate::Config,
}

impl Shared {
    fn terminal(&self) -> Error {
        self.state
            .lock()
            .unwrap()
            .terminal
            .clone()
            .unwrap_or(Error::Closed)
    }

    fn finish(&self, error: Error) {
        let mut state = self.state.lock().unwrap();
        state.terminal = Some(error.clone());
        self.slots.close();
        for (_, reply) in state.pending.drain() {
            let _ = reply.send(Err(error.clone()));
        }
    }
}

struct PendingGuard {
    shared: Arc<Shared>,
    id: u32,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().pending.remove(&self.id);
    }
}

/// Cloneable outgoing capabilities. Keeping a handle does not keep a connection alive.
#[derive(Clone)]
pub struct RpcPeer {
    shared: Arc<Shared>,
}

impl RpcPeer {
    /// Configure an immutable registry before attaching a stream.
    pub fn builder() -> PeerBuilder {
        PeerBuilder::default()
    }

    /// Queue a one-way notification with an explicit argument list.
    /// Success means queued locally, without acknowledgment of delivery or execution.
    /// Waits for bounded outgoing capacity; consumes no request ID or pending-call slot.
    pub async fn notify<A: Serialize>(&self, method: &str, args: A) -> Result<(), Error> {
        if let Some(error) = &self.shared.state.lock().unwrap().terminal {
            return Err(error.clone());
        }
        let args = wire::encode(&Arguments(args), self.shared.config.max_outgoing_bytes)
            .map_err(|_| Error::InvalidParams)?;
        let params = wire::value(args, &self.shared.config).map_err(|_| Error::InvalidParams)?;
        let frame = wire::encode(
            &(2u8, method, params),
            self.shared.config.max_outgoing_bytes,
        )?;
        self.shared
            .outgoing
            .send(frame)
            .await
            .map_err(|_| self.shared.terminal())
    }

    /// Send an explicit argument list and decode a typed result.
    /// Use `()` for no arguments and `(value,)` for one argument.
    /// Calls wait for bounded capacity; dropping the future releases its pending entry.
    pub async fn request<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
    ) -> Result<R, Error> {
        let _slot = self
            .shared
            .slots
            .acquire()
            .await
            .map_err(|_| self.shared.terminal())?;
        let (sender, receiver) = oneshot::channel();
        let id = {
            let mut state = self.shared.state.lock().unwrap();
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            let id = u32::try_from(state.next_id)
                .map_err(|_| Error::ResourceLimit("message ID namespace"))?;
            state.next_id += 1;
            state.pending.insert(id, sender);
            id
        };
        let _pending = PendingGuard {
            shared: self.shared.clone(),
            id,
        };
        let args = wire::encode(&Arguments(args), self.shared.config.max_outgoing_bytes)
            .map_err(|_| Error::InvalidParams)?;
        let params = wire::value(args, &self.shared.config).map_err(|_| Error::InvalidParams)?;
        let frame = wire::encode(
            &(0u8, id, method, params),
            self.shared.config.max_outgoing_bytes,
        )?;
        self.shared
            .outgoing
            .send(frame)
            .await
            .map_err(|_| self.shared.terminal())?;
        let value = receiver.await.map_err(|_| self.shared.terminal())??;
        wire::decode(value)
    }
}

/// Capabilities supplied to a method handler, including reverse RPC requests.
#[derive(Clone)]
pub struct HandlerContext {
    peer: RpcPeer,
}

impl HandlerContext {
    pub(crate) fn config(&self) -> &crate::Config {
        &self.peer.shared.config
    }

    /// Queue a notification while incoming messages continue to be processed.
    pub async fn notify<A: Serialize>(&self, method: &str, args: A) -> Result<(), Error> {
        self.peer.notify(method, args).await
    }

    /// Issue a request while the connection continues processing responses.
    pub async fn request<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
    ) -> Result<R, Error> {
        self.peer.request(method, args).await
    }
}

/// Sole lifecycle owner of a connection and all its background work.
/// Drop initiates cancellation. Use [`Self::wait`] to observe completed cleanup.
#[must_use = "dropping the connection cancels transport and handlers"]
pub struct Connection {
    cancel: Mutex<Option<oneshot::Sender<()>>>,
    task: Option<JoinHandle<Result<(), Error>>>,
}

impl Connection {
    /// Initiate immediate cancellation. This does not drain accepted work.
    pub fn terminate(&self) {
        if let Some(cancel) = self.cancel.lock().unwrap().take() {
            let _ = cancel.send(());
        }
    }

    /// Wait for transport closure and all supervised handler tasks to terminate.
    /// Local termination and clean EOF complete successfully; failures preserve a cause.
    pub async fn wait(mut self) -> Result<(), Error> {
        // Borrow until completion so canceling wait still drops the lifecycle owner.
        let result = self.task.as_mut().unwrap().await;
        self.task.take();
        result.map_err(|_| Error::Protocol("connection supervisor failed"))?
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.terminate();
    }
}

pub(crate) fn start<S>(builder: PeerBuilder, stream: S) -> Result<(RpcPeer, Connection), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    builder.config.validate()?;
    let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
    let (outgoing, application) = mpsc::channel(CAPACITY);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            next_id: 0,
            pending: HashMap::new(),
            terminal: None,
        }),
        outgoing,
        slots: Semaphore::new(CAPACITY),
        config: builder.config.clone(),
    });
    let peer = RpcPeer { shared };
    let (cancel, canceled) = oneshot::channel();
    let task = runtime.spawn(run(stream, builder, peer.clone(), application, canceled));
    Ok((
        peer,
        Connection {
            cancel: Mutex::new(Some(cancel)),
            task: Some(task),
        },
    ))
}

async fn writer<W: AsyncWrite + Unpin>(
    mut writer: W,
    application: &mut mpsc::Receiver<Vec<u8>>,
    responses: &mut mpsc::Receiver<Vec<u8>>,
) -> Error {
    loop {
        let frame = tokio::select! {
            Some(frame) = application.recv() => frame,
            Some(frame) = responses.recv() => frame,
            else => return Error::Closed,
        };
        if let Err(e) = writer.write_all(&frame).await {
            return Error::Transport(e.to_string());
        }
        if let Err(e) = writer.flush().await {
            return Error::Transport(e.to_string());
        }
    }
}

fn respond(
    responses: &mpsc::Sender<Vec<u8>>,
    id: u32,
    outcome: Result<Value, RemoteError>,
    limit: usize,
) -> Result<(), Error> {
    let frame = wire::response(id, outcome, limit).or_else(|_| {
        wire::response(
            id,
            Err(RemoteError::builtin(
                -32603,
                "response serialization failed",
            )),
            limit,
        )
    })?;
    responses
        .try_send(frame)
        .map_err(|_| Error::ResourceLimit("response queue"))
}

async fn run<S>(
    stream: S,
    builder: PeerBuilder,
    peer: RpcPeer,
    mut application: mpsc::Receiver<Vec<u8>>,
    mut canceled: oneshot::Receiver<()>,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, write_half) = tokio::io::split(stream);
    let (responses, mut response_rx) = mpsc::channel(CAPACITY);
    let mut handlers = JoinSet::new();
    let mut active: HashMap<Id, Option<u32>> = HashMap::new();
    let mut buffer = wire::Decoder::default();
    let terminal = {
        let writing = writer(write_half, &mut application, &mut response_rx);
        tokio::pin!(writing);
        let terminal = loop {
            tokio::select! {
                _ = &mut canceled => break Error::Closed,
                error = &mut writing => break error,
                completed = handlers.join_next_with_id(), if !handlers.is_empty() => {
                    let (task_id, outcome) = match completed.unwrap() {
                        Ok((task_id, outcome)) => (task_id, outcome),
                        Err(error) => (error.id(), Err(RemoteError::builtin(-32603, "handler failed"))),
                    };
                    if let Some(id) = active.remove(&task_id).unwrap() {
                        if let Err(error) = respond(&responses, id, outcome, builder.config.max_outgoing_bytes) { break error; }
                    }
                }
                message = wire::read(&mut reader, &mut buffer, &builder.config) => {
                    match message {
                        Err(error) => break error,
                        Ok(Message::Response { id, outcome }) => {
                            if let Some(reply) = peer.shared.state.lock().unwrap().pending.remove(&id) { let _ = reply.send(outcome); }
                        }
                        Ok(Message::Request { id, method, params }) => {
                            if active.values().any(|active_id| *active_id == Some(id)) { break Error::Protocol("duplicate active request ID"); }
                            let Some(handler) = builder.methods.get(&method).cloned() else {
                                if let Err(error) = respond(&responses, id, Err(RemoteError::builtin(-32601, "unknown method")), builder.config.max_outgoing_bytes) { break error; }
                                continue;
                            };
                            if handlers.len() >= CAPACITY {
                                if let Err(error) = respond(&responses, id, Err(RemoteError::builtin(-32000, "handler capacity exhausted")), builder.config.max_outgoing_bytes) { break error; }
                                continue;
                            }
                            let context = HandlerContext { peer: peer.clone() };
                            let task = handlers.spawn(async move { handler(context, params).await });
                            active.insert(task.id(), Some(id));
                        }
                        Ok(Message::Notification { method, params }) => {
                            if handlers.len() >= CAPACITY { continue; }
                            if let Some(handler) = builder.notifications.get(&method).cloned() {
                                let context = HandlerContext { peer: peer.clone() };
                                let task = handlers.spawn(async move { handler(context, params).await });
                                active.insert(task.id(), None);
                            }
                        }
                    }
                }
            }
        };
        // Publish the cause before closing receivers that can wake outgoing senders.
        peer.shared.finish(terminal.clone());
        terminal
    };
    drop(application);
    drop(response_rx);
    handlers.shutdown().await;
    // Drop both transport halves before reporting completion to the owner.
    drop(reader);
    if terminal == Error::Closed {
        Ok(())
    } else {
        Err(terminal)
    }
}
