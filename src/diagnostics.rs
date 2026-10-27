use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;

/// Maximum number of events buffered when observation is enabled. A full or
/// dropped receiver increments `lost_events`.
pub const DIAGNOSTIC_EVENT_CAPACITY: usize = 64;

/// A fixed diagnostic category. Events contain no application-controlled
/// values: no parameters, remote method names, or panic payloads.
///
/// These events also do not cover application logging, application-provided
/// remote errors, custom serializer errors, or the process panic hook, which
/// runs before a panic is caught.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticEvent {
    /// A request or notification handler panicked under unwinding.
    HandlerPanicked,
    /// A notification handler returned an error or could not decode its arguments.
    NotificationFailed,
    /// A notification was dropped because the shared handler budget was exhausted.
    NotificationDropped,
}

/// Fixed counters available even when event observation is disabled. Read
/// them with [`RpcPeer::diagnostics`](crate::RpcPeer::diagnostics); snapshots
/// read counters independently, so concurrent updates may occur during a
/// snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiagnosticsSnapshot {
    /// Request and notification handler panics.
    pub handler_panics: u64,
    /// Notification errors, excluding panics.
    pub notification_failures: u64,
    /// Notifications dropped at handler admission.
    pub dropped_notifications: u64,
    /// Enabled events lost to a full or dropped receiver. Disabled observation loses none.
    pub lost_events: u64,
}

/// Optional bounded event receiver. Keeping it does not keep the connection alive.
///
/// Enable observation with
/// [`PeerBuilder::diagnostic_events`](crate::PeerBuilder::diagnostic_events)
/// before startup and take the stream once with
/// [`Connection::take_events`](crate::Connection::take_events). Disabled
/// observation loses no events, and producers never wait for consumers or
/// spawn extra tasks. The stream closes when the connection terminates, even
/// if handles survive.
pub struct DiagnosticEvents(pub(crate) mpsc::Receiver<DiagnosticEvent>);
impl DiagnosticEvents {
    /// Wait for the next event, or `None` once connection cleanup closes the stream.
    pub async fn recv(&mut self) -> Option<DiagnosticEvent> {
        self.0.recv().await
    }
}

#[derive(Default)]
pub(crate) struct Diagnostics {
    handler_panics: AtomicU64,
    notification_failures: AtomicU64,
    dropped_notifications: AtomicU64,
    lost_events: AtomicU64,
    events: Mutex<Option<mpsc::Sender<DiagnosticEvent>>>,
}

impl Diagnostics {
    pub(crate) fn new(enabled: bool) -> (Self, Option<DiagnosticEvents>) {
        let mut diagnostics = Self::default();
        let events = enabled.then(|| {
            let (sender, receiver) = mpsc::channel(DIAGNOSTIC_EVENT_CAPACITY);
            diagnostics.events = Mutex::new(Some(sender));
            DiagnosticEvents(receiver)
        });
        (diagnostics, events)
    }

    pub(crate) fn record(&self, event: DiagnosticEvent) {
        let counter = match event {
            DiagnosticEvent::HandlerPanicked => &self.handler_panics,
            DiagnosticEvent::NotificationFailed => &self.notification_failures,
            DiagnosticEvent::NotificationDropped => &self.dropped_notifications,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if let Some(sender) = self.events.lock().unwrap().as_ref() {
            if sender.try_send(event).is_err() {
                self.lost_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn close(&self) {
        self.events.lock().unwrap().take();
    }

    pub(crate) fn snapshot(&self) -> DiagnosticsSnapshot {
        DiagnosticsSnapshot {
            handler_panics: self.handler_panics.load(Ordering::Relaxed),
            notification_failures: self.notification_failures.load(Ordering::Relaxed),
            dropped_notifications: self.dropped_notifications.load(Ordering::Relaxed),
            lost_events: self.lost_events.load(Ordering::Relaxed),
        }
    }
}
