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
    /// Whole-call deadline, including asynchronous capacity waits.
    pub default_call_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_incoming_bytes: 1024 * 1024,
            max_outgoing_bytes: 1024 * 1024,
            max_depth: 64,
            max_nodes: 65_536,
            default_call_timeout: Duration::from_secs(30),
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.max_incoming_bytes == 0
            || self.max_outgoing_bytes == 0
            || self.max_depth == 0
            || self.max_depth > 1024
            || self.max_nodes == 0
            || self.default_call_timeout.is_zero()
        {
            return Err(Error::InvalidConfig(
                "wire limits must be positive, depth at most 1024, and timeout nonzero",
            ));
        }
        Ok(())
    }
}
