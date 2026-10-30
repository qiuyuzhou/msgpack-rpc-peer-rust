use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use tokio::sync::mpsc;

use super::{Outgoing, Shared, WorkGuard};
use crate::{Error, RemoteError, Value, wire};

pub(super) struct IncomingRequests {
    ids: Mutex<HashSet<u32>>,
    responses: mpsc::Sender<Outgoing>,
    shared: Arc<Shared>,
}

impl IncomingRequests {
    pub(super) fn new(shared: Arc<Shared>) -> (Arc<Self>, mpsc::Receiver<Outgoing>) {
        let (responses, receiver) = mpsc::channel(shared.config.response_queue_capacity);
        (
            Arc::new(Self {
                ids: Mutex::new(HashSet::new()),
                responses,
                shared,
            }),
            receiver,
        )
    }

    pub(super) fn admit(self: &Arc<Self>, id: u32) -> Result<IncomingRequest, Error> {
        if !self.ids.lock().unwrap().insert(id) {
            return Err(Error::Protocol("duplicate active request ID"));
        }
        Ok(IncomingRequest {
            requests: self.clone(),
            id,
        })
    }
}

/// Owns the incoming ID until its response has been written and flushed.
pub(super) struct IncomingRequest {
    requests: Arc<IncomingRequests>,
    id: u32,
}

impl IncomingRequest {
    pub(super) fn respond(self, outcome: Result<Value, RemoteError>) -> Result<(), Error> {
        let shared = self.requests.shared.clone();
        let config = &shared.config;
        let bytes = wire::response(self.id, outcome, config).or_else(|_| {
            wire::response(
                self.id,
                Err(RemoteError::builtin(
                    -32603,
                    "response serialization failed",
                )),
                config,
            )
        })?;
        let requests = self.requests.clone();
        shared.state.lock().unwrap().frames += 1;
        let frame = Outgoing {
            bytes,
            request_id: None,
            flushed: None,
            _incoming: Some(self),
            _frame: WorkGuard {
                shared,
                frame: true,
            },
        };
        requests
            .responses
            .try_send(frame)
            .map_err(|_| Error::ResourceLimit("response queue"))
    }
}

impl Drop for IncomingRequest {
    fn drop(&mut self) {
        self.requests.ids.lock().unwrap().remove(&self.id);
    }
}
