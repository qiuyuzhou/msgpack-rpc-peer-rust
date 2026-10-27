use crate::Error;
use std::time::Duration;

/// Limits on library-owned buffers and work. Custom Serde code and handlers own their allocations.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum bytes in a complete incoming MessagePack object.
    pub max_incoming_bytes: usize,
    /// Maximum bytes in a complete outgoing envelope, enforced during serialization.
    pub max_outgoing_bytes: usize,
    /// Maximum nested MessagePack containers, including the envelope.
    pub max_depth: usize,
    /// Maximum decoded values, including map keys and envelope fields.
    pub max_nodes: usize,
    /// Maximum calls awaiting responses, including queue waits.
    pub max_pending_calls: usize,
    /// Shared concurrent request and notification handler budget.
    pub max_handlers: usize,
    /// Maximum frames queued by outgoing application operations.
    pub application_queue_capacity: usize,
    /// Maximum internally generated responses queued for writing.
    pub response_queue_capacity: usize,
    /// Whole-call deadline, including asynchronous capacity waits.
    pub default_call_timeout: Duration,
    /// Fixed deadline for graceful shutdown, including accepted writes and handler work.
    pub graceful_shutdown_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_pending_calls: 64,
            max_handlers: 64,
            application_queue_capacity: 64,
            response_queue_capacity: 64,
            max_incoming_bytes: 1024 * 1024,
            max_outgoing_bytes: 1024 * 1024,
            max_depth: 64,
            max_nodes: 65_536,
            default_call_timeout: Duration::from_secs(30),
            graceful_shutdown_timeout: Duration::from_secs(5),
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for capacity in [
            self.max_pending_calls,
            self.max_handlers,
            self.application_queue_capacity,
            self.response_queue_capacity,
        ] {
            if capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(crate::Error::InvalidConfig(
                    "capacities must be positive and supported by Tokio",
                ));
            }
        }
        if self.max_incoming_bytes == 0
            || self.max_outgoing_bytes == 0
            || self.max_depth == 0
            || self.max_depth > 1024
            || self.max_nodes == 0
            || self.graceful_shutdown_timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.graceful_shutdown_timeout)
                .is_none()
            || self.default_call_timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.default_call_timeout)
                .is_none()
        {
            return Err(Error::InvalidConfig(
                "wire limits must be positive, depth at most 1024, and timeout nonzero",
            ));
        }
        Ok(())
    }
}
