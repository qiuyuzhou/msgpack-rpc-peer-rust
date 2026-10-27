use serde::de::DeserializeOwned;

/// A handler's MessagePack-RPC `params` array.
///
/// Implemented for `()`, tuples of 1–16 elements, `Vec<T>`, and arrays that
/// implement `DeserializeOwned`. Use `()` for no parameters, `(T,)` for one,
/// and `(T, U)` for two. Each element can be any owned deserializable type,
/// including a struct, map, or option.
///
/// Custom types may implement this trait if they deserialize from the entire
/// `params` array. This marker does not verify custom deserialization behavior;
/// incoming parameter values and counts are still checked at runtime.
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
/// Scalars, maps, ordinary structs, and options do not implement `RpcParams`
/// by default. Wrap a single value in a one-element tuple:
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
pub trait RpcParams: DeserializeOwned {}

impl RpcParams for () {}
impl<T: DeserializeOwned> RpcParams for Vec<T> {}
impl<T, const N: usize> RpcParams for [T; N] where [T; N]: DeserializeOwned {}

macro_rules! tuple_args {
    ($head:ident $(, $tail:ident)*) => {
        impl<$head: DeserializeOwned, $($tail: DeserializeOwned),*> RpcParams
            for ($head, $($tail,)*) {}
        tuple_args!($($tail),*);
    };
    () => {};
}

tuple_args!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
