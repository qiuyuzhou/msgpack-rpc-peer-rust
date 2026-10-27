use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Notify, Semaphore, mpsc, oneshot},
    task::{Id, JoinHandle, JoinSet},
};

use crate::{
    DiagnosticEvent, DiagnosticEvents, DiagnosticsSnapshot, Error, PeerBuilder, RemoteError, Value,
    arguments::Arguments,
    diagnostics::Diagnostics,
    wire::{self, Message},
};

mod incoming;
use incoming::{IncomingRequest, IncomingRequests};

struct Outgoing {
    bytes: Vec<u8>,
    request_id: Option<u32>,
    _incoming: Option<IncomingRequest>,
    _frame: WorkGuard,
}

type Reply = oneshot::Sender<Result<Value, Error>>;

struct State {
    next_id: u64,
    pending: HashMap<u32, Reply>,
    terminal: Option<Error>,
    drain_deadline: Option<tokio::time::Instant>,
    operations: usize,
    frames: usize,
}

struct Shared {
    state: Mutex<State>,
    outgoing: mpsc::Sender<Outgoing>,
    slots: Semaphore,
    config: crate::Config,
    diagnostics: Diagnostics,
    changed: Notify,
}

impl Shared {
    fn admit(self: &Arc<Self>, lease: Option<&AtomicBool>) -> Result<WorkGuard, Error> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        if state.drain_deadline.is_some()
            && !lease.is_some_and(|active| active.load(Ordering::Relaxed))
        {
            return Err(Error::Draining);
        }
        state.operations += 1;
        Ok(WorkGuard {
            shared: self.clone(),
            frame: false,
        })
    }

    fn commit(
        self: &Arc<Self>,
        permit: mpsc::Permit<'_, Outgoing>,
        bytes: Vec<u8>,
        request_id: Option<u32>,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        state.frames += 1;
        // Receiver cleanup publishes terminal state under this lock before closing queues.
        permit.send(Outgoing {
            bytes,
            request_id,
            _incoming: None,
            _frame: WorkGuard {
                shared: self.clone(),
                frame: true,
            },
        });
        Ok(())
    }

    fn terminal(&self) -> Error {
        self.state
            .lock()
            .unwrap()
            .terminal
            .clone()
            .unwrap_or(Error::Closed)
    }

    fn finish(&self, error: Error) {
        self.diagnostics.close();
        let mut state = self.state.lock().unwrap();
        state.terminal = Some(error.clone());
        self.slots.close();
        for (_, reply) in state.pending.drain() {
            let _ = reply.send(Err(error.clone()));
        }
    }
}

struct WorkGuard {
    shared: Arc<Shared>,
    frame: bool,
}
impl Drop for WorkGuard {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        if self.frame {
            state.frames -= 1;
        } else {
            state.operations -= 1;
        }
        drop(state);
        self.shared.changed.notify_one();
    }
}

struct HandlerLease {
    shared: Arc<Shared>,
    active: Arc<AtomicBool>,
}
impl Drop for HandlerLease {
    fn drop(&mut self) {
        let _state = self.shared.state.lock().unwrap();
        self.active.store(false, Ordering::Relaxed);
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
    /// Read fixed diagnostic counters without consuming optional events.
    pub fn diagnostics(&self) -> DiagnosticsSnapshot {
        self.shared.diagnostics.snapshot()
    }

    /// Number of notifications dropped because the shared handler budget was full.
    pub fn dropped_notifications(&self) -> u64 {
        self.diagnostics().dropped_notifications
    }

    /// Configure an immutable registry before attaching a stream.
    pub fn builder() -> PeerBuilder {
        PeerBuilder::default()
    }

    /// Queue a one-way notification with an explicit argument list.
    /// Success means queued locally, without acknowledgment of delivery or execution.
    /// Waits for bounded outgoing capacity; consumes no request ID or pending-call slot.
    pub async fn notify<A: Serialize>(&self, method: &str, args: A) -> Result<(), Error> {
        self.notify_inner(method, args, None).await
    }

    async fn notify_inner<A: Serialize>(
        &self,
        method: &str,
        args: A,
        lease: Option<&AtomicBool>,
    ) -> Result<(), Error> {
        let _operation = self.shared.admit(lease)?;
        let permit = self
            .shared
            .outgoing
            .reserve()
            .await
            .map_err(|_| self.shared.terminal())?;
        let args = wire::encode(&Arguments(args), self.shared.config.max_outgoing_bytes)
            .map_err(|_| Error::InvalidParams)?;
        let params = wire::value(args, &self.shared.config).map_err(|_| Error::InvalidParams)?;
        let frame = wire::encode(
            &(2u8, method, params),
            self.shared.config.max_outgoing_bytes,
        )?;
        self.shared.commit(permit, frame, None)
    }

    /// Send an explicit argument list and decode a typed result.
    /// Use `()` for no arguments and `(value,)` for one argument.
    /// The configured whole-call timeout includes capacity and response waits.
    /// Dropping the future releases its pending entry; started writes may still execute remotely.
    pub async fn request<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
    ) -> Result<R, Error> {
        self.request_with_timeout(method, args, self.shared.config.default_call_timeout)
            .await
    }

    /// Override the whole-call deadline, including capacity waits.
    /// Cancellation is local: a started request may still execute remotely.
    pub async fn request_with_timeout<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
        timeout: Duration,
    ) -> Result<R, Error> {
        self.request_timed(method, args, timeout, None).await
    }

    async fn request_timed<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
        timeout: Duration,
        lease: Option<&AtomicBool>,
    ) -> Result<R, Error> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("call timeout exceeds the clock range"))?;
        tokio::time::timeout_at(deadline, self.request_inner(method, args, lease))
            .await
            .map_err(|_| Error::Timeout)?
    }

    async fn request_inner<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
        lease: Option<&AtomicBool>,
    ) -> Result<R, Error> {
        let _operation = self.shared.admit(lease)?;
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
        let permit = self
            .shared
            .outgoing
            .reserve()
            .await
            .map_err(|_| self.shared.terminal())?;
        let args = wire::encode(&Arguments(args), self.shared.config.max_outgoing_bytes)
            .map_err(|_| Error::InvalidParams)?;
        let params = wire::value(args, &self.shared.config).map_err(|_| Error::InvalidParams)?;
        let frame = wire::encode(
            &(0u8, id, method, params),
            self.shared.config.max_outgoing_bytes,
        )?;
        self.shared.commit(permit, frame, Some(id))?;
        let value = receiver.await.map_err(|_| self.shared.terminal())??;
        wire::decode(value, self.shared.config.max_incoming_bytes)
    }
}

/// Capabilities supplied to a method handler, including reverse RPC requests.
/// Active handlers may initiate work during graceful draining. That privilege expires
/// when their handler finishes, even if a cloned context survives.
#[derive(Clone)]
pub struct HandlerContext {
    peer: RpcPeer,
    active: Arc<AtomicBool>,
}

impl HandlerContext {
    pub(crate) fn config(&self) -> &crate::Config {
        &self.peer.shared.config
    }

    /// Queue a notification while incoming messages continue to be processed.
    pub async fn notify<A: Serialize>(&self, method: &str, args: A) -> Result<(), Error> {
        self.peer
            .notify_inner(method, args, Some(&self.active))
            .await
    }

    /// Issue a request while the connection continues processing responses.
    pub async fn request<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
    ) -> Result<R, Error> {
        self.peer
            .request_timed(
                method,
                args,
                self.peer.shared.config.default_call_timeout,
                Some(&self.active),
            )
            .await
    }

    /// Override the deadline for a reverse RPC call.
    pub async fn request_with_timeout<A: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        args: A,
        timeout: Duration,
    ) -> Result<R, Error> {
        self.peer
            .request_timed(method, args, timeout, Some(&self.active))
            .await
    }
}

/// Sole lifecycle owner of a connection and all its background work.
/// Drop initiates cancellation. Use [`Self::wait`] to observe completed cleanup.
#[must_use = "dropping the connection cancels transport and handlers"]
pub struct Connection {
    shared: Arc<Shared>,
    events: Option<DiagnosticEvents>,
    cancel: Mutex<Option<oneshot::Sender<()>>>,
    task: Option<JoinHandle<Result<(), Error>>>,
}

impl Connection {
    /// Stop new application work and drain accepted work within the configured deadline.
    /// Handler reverse calls remain admissible until their handler finishes.
    pub async fn shutdown(self) -> Result<(), Error> {
        let timeout = self.shared.config.graceful_shutdown_timeout;
        self.shutdown_with_timeout(timeout).await
    }

    /// Drain using a fixed deadline override. Expiration cancels and joins remaining tasks.
    pub async fn shutdown_with_timeout(self, timeout: Duration) -> Result<(), Error> {
        let deadline =
            tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or(Error::InvalidConfig(
                    "shutdown timeout exceeds the clock range",
                ))?;
        {
            let mut state = self.shared.state.lock().unwrap();
            state.drain_deadline = Some(deadline);
        }
        self.shared.changed.notify_one();
        self.wait().await
    }

    /// Take the optional event stream once. Enable it on the builder before startup.
    pub fn take_events(&mut self) -> Option<DiagnosticEvents> {
        self.events.take()
    }

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
    let (outgoing, application) = mpsc::channel(builder.config.application_queue_capacity);
    let (diagnostics, events) = Diagnostics::new(builder.diagnostic_events);
    let shared = Arc::new(Shared {
        diagnostics,
        changed: Notify::new(),
        state: Mutex::new(State {
            next_id: 0,
            pending: HashMap::new(),
            terminal: None,
            drain_deadline: None,
            operations: 0,
            frames: 0,
        }),
        outgoing,
        slots: Semaphore::new(builder.config.max_pending_calls),
        config: builder.config.clone(),
    });
    let peer = RpcPeer { shared };
    let (cancel, canceled) = oneshot::channel();
    let task = runtime.spawn(run(stream, builder, peer.clone(), application, canceled));
    Ok((
        peer.clone(),
        Connection {
            shared: peer.shared.clone(),
            events,
            cancel: Mutex::new(Some(cancel)),
            task: Some(task),
        },
    ))
}

async fn writer<W: AsyncWrite + Unpin>(
    mut writer: W,
    application: &mut mpsc::Receiver<Outgoing>,
    shared: &Shared,
    responses: &mut mpsc::Receiver<Outgoing>,
) -> Error {
    let mut prefer_application = true;
    loop {
        let (frame, application_frame) = if prefer_application {
            tokio::select! { biased;
                Some(frame) = application.recv() => (frame, true),
                Some(frame) = responses.recv() => (frame, false),
                else => return Error::Closed,
            }
        } else {
            tokio::select! { biased;
                Some(frame) = responses.recv() => (frame, false),
                Some(frame) = application.recv() => (frame, true),
                else => return Error::Closed,
            }
        };
        prefer_application = !application_frame;
        if frame
            .request_id
            .is_some_and(|id| !shared.state.lock().unwrap().pending.contains_key(&id))
        {
            continue;
        }
        if let Err(e) = writer.write_all(&frame.bytes).await {
            return Error::Transport(e.to_string());
        }
        if let Err(e) = writer.flush().await {
            return Error::Transport(e.to_string());
        }
    }
}

type HandlerCompletion = Result<(Id, Result<Value, RemoteError>), tokio::task::JoinError>;

enum Event {
    Changed,
    Writer(Error),
    Handler(HandlerCompletion),
    Message(Result<Message, Error>),
}

async fn run<S>(
    stream: S,
    builder: PeerBuilder,
    peer: RpcPeer,
    mut application: mpsc::Receiver<Outgoing>,
    mut canceled: oneshot::Receiver<()>,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, write_half) = tokio::io::split(stream);
    let (incoming_requests, mut response_rx) = IncomingRequests::new(peer.shared.clone());
    let mut handlers = JoinSet::new();
    let mut active: HashMap<Id, Option<IncomingRequest>> = HashMap::new();
    let mut buffer = wire::Decoder::default();
    let terminal = {
        let writing = writer(write_half, &mut application, &peer.shared, &mut response_rx);
        tokio::pin!(writing);
        let terminal = loop {
            let deadline = {
                let mut state = peer.shared.state.lock().unwrap();
                if state.drain_deadline.is_some()
                    && handlers.is_empty()
                    && state.operations == 0
                    && state.frames == 0
                {
                    state.terminal = Some(Error::Closed);
                    break Error::Closed;
                }
                state.drain_deadline
            };
            let expired = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! { biased;
                _ = expired => break Error::ShutdownTimeout,
                _ = &mut canceled => break Error::Closed,
                event = async {
                    tokio::select! {
                        _ = peer.shared.changed.notified() => Event::Changed,
                        error = &mut writing => Event::Writer(error),
                        completed = handlers.join_next_with_id(), if !handlers.is_empty() => Event::Handler(completed.unwrap()),
                        message = wire::read(&mut reader, &mut buffer, &builder.config) => Event::Message(message),
                    }
                } => match event {
                Event::Changed => {},
                Event::Writer(error) => break error,
                Event::Handler(completed) => {
                    let completion = completed;
                    let panicked = completion.is_err();
                    let (task_id, outcome) = match completion {
                        Ok((task_id, outcome)) => (task_id, outcome),
                        Err(error) => {
                            peer.shared.diagnostics.record(DiagnosticEvent::HandlerPanicked);
                            (error.id(), Err(RemoteError::builtin(-32603, "handler failed")))
                        },
                    };
                    let incoming_request = active.remove(&task_id).unwrap();
                    if incoming_request.is_none() && outcome.is_err() && !panicked {
                        peer.shared.diagnostics.record(DiagnosticEvent::NotificationFailed);
                    }
                    if let Some(incoming) = incoming_request {
                        if let Err(error) = incoming.respond(outcome) { break error; }
                    }
                }
                Event::Message(message) => {
                    match message {
                        Err(error) => break error,
                        Ok(Message::Response { id, outcome }) => {
                            if let Some(reply) = peer.shared.state.lock().unwrap().pending.remove(&id) { let _ = reply.send(outcome); }
                        }
                        Ok(Message::Request { id, method, params }) => {
                            let incoming = match incoming_requests.admit(id) {
                                Ok(incoming) => incoming,
                                Err(error) => break error,
                            };
                            let Some(handler) = builder.methods.get(&method).cloned() else {
                                if let Err(error) = incoming.respond(Err(RemoteError::builtin(-32601, "unknown method"))) { break error; }
                                continue;
                            };
                            if handlers.len() >= builder.config.max_handlers {
                                if let Err(error) = incoming.respond(Err(RemoteError::builtin(-32000, "handler capacity exhausted"))) { break error; }
                                continue;
                            }
                            let active_lease = Arc::new(AtomicBool::new(true));
                            let context = HandlerContext { peer: peer.clone(), active: active_lease.clone() };
                            let lease = HandlerLease { shared: peer.shared.clone(), active: active_lease };
                            let task = handlers.spawn(async move { let _lease = lease; handler(context, params).await });
                            active.insert(task.id(), Some(incoming));
                        }
                        Ok(Message::Notification { method, params }) => {
                            if handlers.len() >= builder.config.max_handlers {
                                peer.shared.diagnostics.record(DiagnosticEvent::NotificationDropped);
                                continue;
                            }
                            if let Some(handler) = builder.notifications.get(&method).cloned() {
                                let active_lease = Arc::new(AtomicBool::new(true));
                                let context = HandlerContext { peer: peer.clone(), active: active_lease.clone() };
                                let lease = HandlerLease { shared: peer.shared.clone(), active: active_lease };
                                let task = handlers.spawn(async move { let _lease = lease; handler(context, params).await });
                                active.insert(task.id(), None);
                            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exhausted_ids_fail_without_wrapping() {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        peer.shared.state.lock().unwrap().next_id = u32::MAX as u64;
        let (result, ()) = tokio::join!(
            peer.request_with_timeout::<_, ()>("x", (), Duration::from_millis(10)),
            async {
                use tokio::io::AsyncReadExt;
                let mut bytes = [0; 10];
                raw.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"\x94\x00\xce\xff\xff\xff\xff\xa1x\x90");
            }
        );
        assert_eq!(result, Err(Error::Timeout));
        assert_eq!(
            peer.request::<_, ()>("x", ()).await,
            Err(Error::ResourceLimit("message ID namespace"))
        );
        connection.terminate();
        connection.wait().await.unwrap();
    }
}
