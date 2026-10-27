use crate::Error;
use std::time::Duration;

/// Limits on library-owned buffers and work. Custom Serde code and handlers own their allocations.
///
/// Pass `Config { field: value, ..Config::default() }` to
/// [`PeerBuilder::config`](crate::PeerBuilder::config). Startup validates the
/// configuration before spawning work. Byte and node limits must be positive,
/// depth must be between 1 and 1024, work and queue capacities must be
/// positive and within Tokio's semaphore limit, and deadlines must be
/// positive and representable by the monotonic clock. Violations and
/// unsupported per-operation deadline overrides return
/// [`Error::InvalidConfig`](crate::Error::InvalidConfig).
///
/// Budgets are finite, so overload fails in a bounded way; the library does
/// not guarantee success for arbitrary nesting. Application operations wait
/// for bounded pending or queue capacity. The receive loop never waits for a
/// handler or writer slot: excess requests get an overload error, and excess
/// notifications are dropped and counted. A full response queue terminates
/// the connection with `Error::ResourceLimit("response queue")`. The writer
/// alternates preference between the application and response queues, so
/// response processing stays independent of handler execution and sustained
/// application writes.
///
/// The incremental scanner checks MessagePack headers before allocating
/// decoded containers, and rejects hostile advertised string, binary,
/// extension, array, and map lengths against the byte and node budgets.
/// Encoded buffering is bounded by `max_incoming_bytes`, and fragmented or
/// concatenated objects need no delimiter or length prefix. Params and
/// results also pass depth and node checks before conversion to dynamic
/// values, and the complete outgoing envelope stays within the byte limit
/// during serialization. Unknown-length Serde sequences, maps, and formatted
/// strings obey the same encoding byte limit without unbounded staging
/// buffers. Rejected outgoing calls and notifications emit no partial frame.
/// Oversized or unserializable results and errors use a generic remote
/// failure if it fits; otherwise the connection terminates with
/// [`Error::Encode`](crate::Error::Encode) instead of stranding a remote
/// call.
///
/// Queue slots are reserved before application serialization, so capacity
/// waiters hold no library-encoded frame. Reserved slots, the single active
/// writer, active handlers and their result conversions, and the reader each
/// have separate bounded costs. Active incoming IDs are bounded by admitted
/// handlers, queued responses, and the one in-flight response. These limits
/// do not include allocator overhead or bound total process memory;
/// application-provided arguments, custom Serde implementations, transport
/// implementations, and handlers remain responsible for their own
/// allocations and CPU use.
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum bytes in a complete incoming MessagePack object. Default: 1 MiB.
    pub max_incoming_bytes: usize,
    /// Maximum bytes in a complete outgoing envelope, enforced during
    /// serialization. Default: 1 MiB.
    pub max_outgoing_bytes: usize,
    /// Maximum nested MessagePack containers, including the envelope. Must be
    /// between 1 and 1024. Default: 64.
    pub max_depth: usize,
    /// Maximum decoded values, including map keys and envelope fields.
    /// Default: 65,536.
    pub max_nodes: usize,
    /// Maximum calls awaiting responses, including queue waits. Default: 64.
    pub max_pending_calls: usize,
    /// Shared concurrent request and notification handler budget. Default: 64.
    pub max_handlers: usize,
    /// Maximum frames queued by outgoing application operations. Default: 64.
    pub application_queue_capacity: usize,
    /// Maximum internally generated responses queued for writing. Default: 64.
    pub response_queue_capacity: usize,
    /// Whole-call deadline, including asynchronous capacity waits.
    /// Default: 30 seconds.
    pub default_call_timeout: Duration,
    /// Fixed deadline for graceful shutdown, including accepted writes and
    /// handler work. Default: 5 seconds.
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
