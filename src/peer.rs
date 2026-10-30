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
    sync::{Notify, mpsc, oneshot},
    task::{Id, JoinHandle, JoinSet},
};

use crate::{
    DiagnosticEvent, DiagnosticEvents, DiagnosticsSnapshot, Error, PeerBuilder, RemoteError,
    RpcParams, Value,
    diagnostics::Diagnostics,
    typed,
    wire::{self, Message},
};

mod incoming;
use incoming::{IncomingRequest, IncomingRequests};
mod outgoing;
use outgoing::{OutgoingRequest, PendingRequests};

struct Outgoing {
    bytes: Vec<u8>,
    request_id: Option<u32>,
    flushed: Option<oneshot::Sender<()>>,
    _incoming: Option<IncomingRequest>,
    _frame: WorkGuard,
}

struct State {
    pending: PendingRequests,
    terminal: Option<Error>,
    drain_deadline: Option<tokio::time::Instant>,
    operations: usize,
    frames: usize,
}

struct Shared {
    state: Mutex<State>,
    outgoing: mpsc::Sender<Outgoing>,
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
        flushed: Option<oneshot::Sender<()>>,
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
            flushed,
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
        state.pending.finish(error);
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

/// Cloneable outgoing capabilities. Keeping a handle does not keep a connection alive.
///
/// Requests and notifications can be issued concurrently, and responses are
/// correlated by message ID. Both endpoints count outgoing IDs in independent
/// namespaces, so opposite requests with the same numeric ID do not collide.
/// Outgoing u32 IDs increase without reuse, even after cancellation;
/// exhaustion returns [`Error::ResourceLimit`] with a `message ID namespace`
/// payload instead of wrapping.
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

    /// Queue a one-way notification with typed params.
    ///
    /// A successful await means the notification is queued locally; it does
    /// not confirm remote delivery, execution, or success. Notifications
    /// consume no request ID or pending-call slot and never receive responses.
    /// The remote peer ignores unknown notification names, and invalid typed
    /// params and other notification failures are counted in its own
    /// diagnostics. Waits for bounded outgoing capacity.
    ///
    /// Params follow [`RpcParams`]: `()` for none, `(value,)` for one, and
    /// `(first, second)` for two. Scalars, maps, structs, options, and nil
    /// fail to compile:
    ///
    /// ```compile_fail
    /// use msgpack_rpc_peer::RpcPeer;
    /// async fn demo(peer: RpcPeer) {
    ///     let _ = peer.notify("event", 7u32).await;
    /// }
    /// ```
    pub async fn notify<P: RpcParams + Serialize>(
        &self,
        method: &str,
        params: P,
    ) -> Result<(), Error> {
        self.notify_inner(method, params, None).await
    }

    async fn notify_inner<P: RpcParams + Serialize>(
        &self,
        method: &str,
        params: P,
        lease: Option<&AtomicBool>,
    ) -> Result<(), Error> {
        let _operation = self.shared.admit(lease)?;
        let permit = self
            .shared
            .outgoing
            .reserve()
            .await
            .map_err(|_| self.shared.terminal())?;
        let encoded = typed::encode_params(params, &self.shared.config)?;
        let frame = wire::encode(
            &(2u8, method, encoded),
            self.shared.config.max_outgoing_bytes,
        )?;
        self.shared.commit(permit, frame, None, None)
    }

    /// Send typed params and decode a typed result.
    ///
    /// Params are tuples or sequences: `()` for none, `(value,)` for one,
    /// and `(first, second)` for two. Scalars, maps, structs, options, and
    /// nil do not implement [`RpcParams`] and fail to compile. A structured
    /// value goes inside the list as `(MyParams { ... },)` and encodes
    /// with named fields. A custom [`RpcParams`] implementation that does not
    /// serialize an array still fails at runtime as [`Error::InvalidParams`].
    /// The result can be any supported Serde value; use [`Value`] for
    /// arbitrary MessagePack data.
    ///
    /// The call uses [`Config::default_call_timeout`](crate::Config::default_call_timeout)
    /// (30 seconds by default) as a whole-call deadline. The deadline includes
    /// pending-slot and outgoing-queue waits, serialization, and response
    /// waiting. Synchronous Serde code and handler code cannot be preempted by
    /// an asynchronous deadline.
    ///
    /// Dropping, aborting, or timing out the call removes its pending entry
    /// and releases capacity. A call canceled before its frame starts writing
    /// is skipped; a started frame finishes while the connection remains
    /// active, preserving framing, so local cancellation cannot prevent remote
    /// side effects. Late and unmatched responses are discarded.
    /// [`Error::Timeout`] leaves the connection usable.
    pub async fn request<P: RpcParams + Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R, Error> {
        self.request_with_timeout(method, params, self.shared.config.default_call_timeout)
            .await
    }

    /// Override the whole-call deadline, including capacity waits.
    ///
    /// The deadline cannot preempt synchronous Serde code or handler code.
    /// Cancellation is local: a started request may still execute remotely.
    /// [`Error::Timeout`] leaves the connection usable.
    pub async fn request_with_timeout<P: RpcParams + Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
        timeout: Duration,
    ) -> Result<R, Error> {
        self.request_timed(method, params, timeout, None).await
    }

    async fn request_timed<P: RpcParams + Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
        timeout: Duration,
        lease: Option<&AtomicBool>,
    ) -> Result<R, Error> {
        OutgoingRequest::execute(
            &self.shared,
            timeout,
            lease,
            |id| {
                let encoded = typed::encode_params(params, &self.shared.config)?;
                wire::encode(
                    &(0u8, id, method, encoded),
                    self.shared.config.max_outgoing_bytes,
                )
            },
            |value| typed::decode_result(value, self.shared.config.max_incoming_bytes),
        )
        .await
    }

    /// Call with a dynamic params array and retain the complete dynamic result.
    /// Uses the same deadlines, cancellation, IDs and capacity bounds as typed calls.
    pub async fn request_value(&self, method: &str, params: Value) -> Result<Value, Error> {
        OutgoingRequest::execute(
            &self.shared,
            self.shared.config.default_call_timeout,
            None,
            |id| self.value_frame(vec![0.into(), id.into(), method.into(), params]),
            Ok,
        )
        .await
    }

    /// Send a dynamic params array and wait for this frame's local write and flush.
    /// Uses the configured whole-call deadline, including queue waits. Success
    /// does not confirm remote execution. A canceled started frame may still finish.
    pub async fn notify_value(&self, method: &str, params: Value) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now()
            .checked_add(self.shared.config.default_call_timeout)
            .ok_or(Error::InvalidConfig("call timeout exceeds the clock range"))?;
        tokio::time::timeout_at(deadline, async {
            let _operation = self.shared.admit(None)?;
            let permit = self
                .shared
                .outgoing
                .reserve()
                .await
                .map_err(|_| self.shared.terminal())?;
            let bytes = self.value_frame(vec![2.into(), method.into(), params])?;
            let (flushed, receiver) = oneshot::channel();
            self.shared.commit(permit, bytes, None, Some(flushed))?;
            receiver.await.map_err(|_| self.shared.terminal())
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    fn value_frame(&self, fields: Vec<Value>) -> Result<Vec<u8>, Error> {
        let params = fields.last().unwrap();
        if !params.is_array() {
            return Err(Error::InvalidParams);
        }
        crate::encoding::encode_value(&Value::Array(fields), &self.shared.config)
    }
}

/// Capabilities supplied to a method handler, including reverse RPC requests.
///
/// [`Self::request`] and [`Self::notify`] initiate reverse operations while
/// the receive loop continues processing responses. Requests and
/// notifications share a bounded concurrent handler budget, and handlers can
/// finish out of arrival order.
///
/// Active handlers may initiate work during graceful draining. That privilege
/// expires when their handler finishes, even if a cloned context survives.
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
    pub async fn notify<P: RpcParams + Serialize>(
        &self,
        method: &str,
        params: P,
    ) -> Result<(), Error> {
        self.peer
            .notify_inner(method, params, Some(&self.active))
            .await
    }

    /// Issue a request while the connection continues processing responses.
    pub async fn request<P: RpcParams + Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R, Error> {
        self.peer
            .request_timed(
                method,
                params,
                self.peer.shared.config.default_call_timeout,
                Some(&self.active),
            )
            .await
    }

    /// Override the deadline for a reverse RPC call.
    pub async fn request_with_timeout<P: RpcParams + Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: P,
        timeout: Duration,
    ) -> Result<R, Error> {
        self.peer
            .request_timed(method, params, timeout, Some(&self.active))
            .await
    }
}

/// Sole lifecycle owner of a connection and all its background work.
///
/// Dropping the owner initiates cancellation, even if peer handles or handler
/// contexts survive. A destructor cannot synchronously join tasks, and
/// canceling a [`Self::wait`] or shutdown future also drops the owner and
/// cancels. The supervisor owns the read and single-writer futures and a
/// bounded set of handler tasks; internal locks do not span asynchronous
/// waits.
#[must_use = "dropping the connection cancels transport and handlers"]
pub struct Connection {
    shared: Arc<Shared>,
    events: Option<DiagnosticEvents>,
    cancel: Mutex<Option<oneshot::Sender<()>>>,
    task: Option<JoinHandle<Result<(), Error>>>,
}

impl Connection {
    /// Stop new application work and drain accepted work within the
    /// configured five-second deadline.
    ///
    /// New application calls and notifications return [`Error::Draining`],
    /// while operations already admitted, including capacity waiters, may
    /// finish. Active handlers retain reverse-call admission; cloned contexts
    /// lose it when their handler ends. Incoming work stays admissible within
    /// the same bounds, because standard messages cannot identify reverse
    /// calls. Sustained traffic never extends the fixed deadline.
    ///
    /// Successful draining waits for admitted operations, handlers, queued
    /// frames, and in-flight writes. At the deadline, remaining work is
    /// canceled and joined, and the owner and pending calls receive
    /// [`Error::ShutdownTimeout`].
    pub async fn shutdown(self) -> Result<(), Error> {
        let timeout = self.shared.config.graceful_shutdown_timeout;
        self.shutdown_with_timeout(timeout).await
    }

    /// Drain using a fixed deadline override instead of the configured
    /// [`Config::graceful_shutdown_timeout`](crate::Config::graceful_shutdown_timeout).
    /// Expiration cancels and joins remaining tasks. Semantics match
    /// [`Self::shutdown`].
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

    /// Take the optional event stream once. Enable it on the builder before
    /// startup with [`PeerBuilder::diagnostic_events`](crate::PeerBuilder::diagnostic_events).
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
    ///
    /// Local termination and clean EOF complete the owner successfully, and
    /// outstanding calls fail with [`Error::Closed`]. Malformed traffic,
    /// resource violations, and I/O failures keep their terminal cause, so EOF
    /// can end a drain before all accepted work succeeds.
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

pub(crate) fn start<S>(builder: &PeerBuilder, stream: S) -> Result<(RpcPeer, Connection), Error>
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
            pending: PendingRequests::new(builder.config.max_pending_calls),
            terminal: None,
            drain_deadline: None,
            operations: 0,
            frames: 0,
        }),
        outgoing,
        config: builder.config.clone(),
    });
    let peer = RpcPeer { shared };
    let (cancel, canceled) = oneshot::channel();
    // The task owns a snapshot, so the builder can keep registering and starting.
    let task = runtime.spawn(run(
        stream,
        builder.clone(),
        peer.clone(),
        application,
        canceled,
    ));
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
        let (mut frame, application_frame) = if prefer_application {
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
            .is_some_and(|id| !shared.state.lock().unwrap().pending.is_active(id))
        {
            continue;
        }
        if let Err(e) = writer.write_all(&frame.bytes).await {
            return Error::Transport(e.to_string());
        }
        if let Err(e) = writer.flush().await {
            return Error::Transport(e.to_string());
        }
        if let Some(flushed) = frame.flushed.take() {
            let _ = flushed.send(());
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
                            peer.shared.state.lock().unwrap().pending.complete(id, outcome);
                        }
                        Ok(Message::Request { id, method, params }) => {
                            let incoming = match incoming_requests.admit(id) {
                                Ok(incoming) => incoming,
                                Err(error) => break error,
                            };
                            let Some(handler) = builder.methods.get(&method).cloned().or_else(|| {
                                let fallback = builder.fallback_method.clone()?;
                                let method = method.clone();
                                Some(Arc::new(move |context, params| fallback(context, method.clone(), params)) as crate::registry::Handler)
                            }) else {
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
                            if !builder.notifications.contains_key(&method) {
                                if let Some(sink) = &builder.notification_sink {
                                    if sink.try_send((method, params)).is_err() {
                                        peer.shared.diagnostics.record(DiagnosticEvent::NotificationDropped);
                                    }
                                    continue;
                                }
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
