#![doc = include_str!("../README.md")]

//! # Wire format
//!
//! Messages follow the
//! [MessagePack-RPC specification](https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md)
//! and travel as consecutive MessagePack objects with no extra framing,
//! handshake, or custom types:
//!
//! - Request: `[0, u32_id, string_method, array_params]`
//! - Response: `[1, u32_id, error, result]`
//! - Notification: `[2, string_method, array_params]`
//!
//! Success carries a nil error and a result; failure carries a non-nil error
//! and a nil result. Named struct fields and explicit argument lists keep the
//! Rust contract usable by an independent peer. Interoperability tests use
//! literal fixtures for requests, responses, and notifications, plus
//! arbitrary remote errors, fragmented headers and payloads, concatenated
//! objects, and round trips with a second MessagePack-RPC library.
//!
//! An incoming request ID stays active from admission until its response is
//! written and flushed, including unknown-method and overload responses.
//! Repeating an active ID in that window is a fatal protocol violation.
//! After the response completes, the remote peer may reuse the ID, though
//! this library's outgoing IDs never repeat.

mod config;
mod encoding;
mod error;
mod params;
mod peer;
mod registry;
mod transport;
mod typed;
mod wire;
pub use transport::SplitIo;

pub use config::Config;
pub use encoding::encode_value;
pub use error::{Error, RemoteError};
pub use params::RpcParams;
pub use peer::{Connection, HandlerContext, RpcPeer};
pub use registry::PeerBuilder;
/// An arbitrary MessagePack value, including remote error payloads.
pub use rmpv::Value;

mod diagnostics;
pub use diagnostics::{
    DIAGNOSTIC_EVENT_CAPACITY, DiagnosticEvent, DiagnosticEvents, DiagnosticsSnapshot,
};
