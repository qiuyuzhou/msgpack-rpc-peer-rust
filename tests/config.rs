use std::time::Duration;

use msgpack_rpc_peer::{Config, DIAGNOSTIC_EVENT_CAPACITY};

#[test]
fn public_defaults_match_the_documented_resource_contract() {
    let config = Config::default();
    assert_eq!(config.max_incoming_bytes, 1024 * 1024);
    assert_eq!(config.max_outgoing_bytes, 1024 * 1024);
    assert_eq!(config.max_depth, 64);
    assert_eq!(config.max_nodes, 65_536);
    assert_eq!(config.max_pending_calls, 64);
    assert_eq!(config.max_handlers, 64);
    assert_eq!(config.application_queue_capacity, 64);
    assert_eq!(config.response_queue_capacity, 64);
    assert_eq!(config.default_call_timeout, Duration::from_secs(30));
    assert_eq!(config.graceful_shutdown_timeout, Duration::from_secs(5));
    assert_eq!(DIAGNOSTIC_EVENT_CAPACITY, 64);
}
