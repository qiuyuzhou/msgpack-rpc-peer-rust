use std::{collections::HashMap, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use super::Shared;
use crate::{Error, Value, arguments::Arguments, wire};

type Reply = oneshot::Sender<Result<Value, Error>>;

pub(super) struct PendingRequests {
    next_id: u64,
    replies: HashMap<u32, Reply>,
    slots: Arc<Semaphore>,
}

impl PendingRequests {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            next_id: 0,
            replies: HashMap::new(),
            slots: Arc::new(Semaphore::new(capacity)),
        }
    }

    pub(super) fn is_active(&self, id: u32) -> bool {
        self.replies.contains_key(&id)
    }

    pub(super) fn complete(&mut self, id: u32, outcome: Result<Value, Error>) {
        if let Some(reply) = self.replies.remove(&id) {
            let _ = reply.send(outcome);
        }
    }

    pub(super) fn finish(&mut self, error: Error) {
        self.slots.close();
        for (_, reply) in self.replies.drain() {
            let _ = reply.send(Err(error.clone()));
        }
    }
}

/// Keeps local cancellation and capacity tied to the call, independent of its queued frame.
pub(super) struct OutgoingRequest {
    shared: Arc<Shared>,
    id: u32,
    receiver: oneshot::Receiver<Result<Value, Error>>,
    _slot: OwnedSemaphorePermit,
}

impl OutgoingRequest {
    pub(super) async fn begin(shared: &Arc<Shared>) -> Result<Self, Error> {
        let slots = shared.state.lock().unwrap().pending.slots.clone();
        let slot = slots.acquire_owned().await.map_err(|_| shared.terminal())?;
        let (sender, receiver) = oneshot::channel();
        let mut state = shared.state.lock().unwrap();
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        let id = u32::try_from(state.pending.next_id)
            .map_err(|_| Error::ResourceLimit("message ID namespace"))?;
        state.pending.next_id += 1;
        state.pending.replies.insert(id, sender);
        Ok(Self {
            shared: shared.clone(),
            id,
            receiver,
            _slot: slot,
        })
    }

    pub(super) async fn send<A: Serialize, R: DeserializeOwned>(
        mut self,
        method: &str,
        args: A,
    ) -> Result<R, Error> {
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
            &(0u8, self.id, method, params),
            self.shared.config.max_outgoing_bytes,
        )?;
        self.shared.commit(permit, frame, Some(self.id))?;
        let value = (&mut self.receiver)
            .await
            .map_err(|_| self.shared.terminal())??;
        wire::decode(value, self.shared.config.max_incoming_bytes)
    }
}

impl Drop for OutgoingRequest {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .unwrap()
            .pending
            .replies
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RpcPeer;
    use std::time::Duration;

    #[tokio::test]
    async fn exhausted_ids_fail_without_wrapping() {
        let (stream, mut raw) = tokio::io::duplex(64);
        let (peer, connection) = RpcPeer::builder().start(stream).unwrap();
        peer.shared.state.lock().unwrap().pending.next_id = u32::MAX as u64;
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
