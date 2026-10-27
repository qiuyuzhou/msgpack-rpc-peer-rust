/// The MessagePack-RPC `params` array shape shared by handlers and outgoing
/// calls.
///
/// Implemented for `()`, tuples of 1–16 elements, `Vec<T>`, and arrays. Use
/// `()` for no params, `(T,)` for one, and `(T, U)` for two. Each element can
/// be any owned type, including a struct, map, or option. Builder handlers
/// require elements that implement
/// [`DeserializeOwned`](serde::de::DeserializeOwned), and outgoing
/// [`notify`](crate::RpcPeer::notify) and [`request`](crate::RpcPeer::request)
/// calls require elements that implement [`Serialize`](serde::Serialize).
///
/// Scalars, maps, ordinary structs, options, and nil do not implement
/// `RpcParams`, so they fail at compile time on both sides. Wrap a single
/// value in a one-element tuple.
///
/// Custom types may implement this trait if they convert the entire `params`
/// array. This marker does not verify custom conversion behavior; incoming
/// parameter values and counts are still checked at runtime, and an outgoing
/// implementation that does not serialize an array fails at runtime as
/// [`Error::InvalidParams`](crate::Error::InvalidParams).
///
/// ```
/// use msgpack_rpc_peer::{RpcParams, RpcPeer};
///
/// #[derive(serde::Deserialize)]
/// struct CustomParams(Vec<u32>);
/// impl RpcParams for CustomParams {}
///
/// let builder = RpcPeer::builder()
///     .method("sum", |_, params: CustomParams| async move {
///         Ok(params.0.into_iter().sum::<u32>())
///     }).unwrap()
///     .notification("event", |_, _: [u32; 2]| async { Ok(()) }).unwrap();
/// ```
///
/// The same contract constrains outgoing calls:
///
/// ```compile_fail
/// use msgpack_rpc_peer::RpcPeer;
/// async fn demo(peer: RpcPeer) {
///     let _ = peer.request::<_, ()>("echo", 7u32).await;
/// }
/// ```
///
/// Scalars, maps, structs, and options do not implement `RpcParams` for
/// handlers either:
///
/// ```compile_fail
/// use msgpack_rpc_peer::RpcPeer;
/// RpcPeer::builder().method("scalar", |_, n: u32| async move { Ok(n) });
/// ```
///
/// ```compile_fail
/// use msgpack_rpc_peer::RpcPeer;
/// use std::collections::BTreeMap;
/// RpcPeer::builder().method("map", |_, params: BTreeMap<String, u32>| async move { Ok(params) });
/// ```
///
/// ```compile_fail
/// use msgpack_rpc_peer::RpcPeer;
/// #[derive(serde::Deserialize)]
/// struct Params { n: u32 }
/// RpcPeer::builder().method("struct", |_, params: Params| async move { Ok(params.n) });
/// ```
///
/// ```compile_fail
/// use msgpack_rpc_peer::RpcPeer;
/// RpcPeer::builder().method("option", |_, n: Option<u32>| async move { Ok(n) });
/// ```
///
/// Notification handlers require the same contract:
///
/// ```compile_fail
/// use msgpack_rpc_peer::RpcPeer;
/// RpcPeer::builder().notification("scalar", |_, _: u32| async { Ok(()) });
/// ```
pub trait RpcParams {}

impl RpcParams for () {}
impl<T> RpcParams for Vec<T> {}
impl<T, const N: usize> RpcParams for [T; N] {}

macro_rules! tuple_params {
    ($head:ident $(, $tail:ident)*) => {
        impl<$head, $($tail),*> RpcParams for ($head, $($tail,)*) {}
        tuple_params!($($tail),*);
    };
    () => {};
}

tuple_params!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
