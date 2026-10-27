//! Typed MessagePack-RPC between equal peers on a caller-owned Tokio runtime.
//!
//! Register methods with [`RpcPeer::builder`], then retain the [`Connection`]
//! returned by [`PeerBuilder::start`]. Arguments are explicit tuples or sequences.
//! See the crate README and `examples/duplex.rs` for a complete interaction.

mod arguments;
mod error;
mod peer;
mod registry;
mod wire;

pub use error::{Error, RemoteError};
pub use peer::{Connection, HandlerContext, RpcPeer};
pub use registry::PeerBuilder;
/// An arbitrary MessagePack value, including remote error payloads.
pub use rmpv::Value;
