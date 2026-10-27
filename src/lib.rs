#![doc = include_str!("../README.md")]

mod arguments;
mod config;
mod error;
mod peer;
mod registry;
mod transport;
mod wire;
pub use transport::SplitIo;

pub use config::Config;
pub use error::{Error, RemoteError};
pub use peer::{Connection, HandlerContext, RpcPeer};
pub use registry::PeerBuilder;
/// An arbitrary MessagePack value, including remote error payloads.
pub use rmpv::Value;

mod diagnostics;
pub use diagnostics::{
    DIAGNOSTIC_EVENT_CAPACITY, DiagnosticEvent, DiagnosticEvents, DiagnosticsSnapshot,
};
