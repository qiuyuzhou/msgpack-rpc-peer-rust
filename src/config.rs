use std::time::Duration;

/// Connection resource and lifecycle configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum calls awaiting responses, including queue waits.
    pub max_pending_calls: usize,
    /// Shared concurrent request and notification handler budget.
    pub max_handlers: usize,
    /// Maximum frames queued by outgoing application operations.
    pub application_queue_capacity: usize,
    /// Maximum internally generated responses queued for writing.
    pub response_queue_capacity: usize,
    /// Deadline covering pending capacity, queue capacity, and the response wait.
    pub default_call_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_pending_calls: 64,
            max_handlers: 64,
            application_queue_capacity: 64,
            response_queue_capacity: 64,
            default_call_timeout: Duration::from_secs(30),
        }
    }
}
impl Config {
    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
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
        if self.default_call_timeout.is_zero() {
            return Err(crate::Error::InvalidConfig("call timeout must be positive"));
        }
        Ok(())
    }
}
