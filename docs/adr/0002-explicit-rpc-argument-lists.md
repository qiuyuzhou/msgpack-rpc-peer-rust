# Express RPC arguments as explicit lists

Calls and notifications take tuple or sequence argument lists to preserve the MessagePack-RPC requirement that params is an array. An empty tuple produces an empty array, a one-element tuple carries one argument, and structs within arguments use named fields for predictable interoperability. Top-level scalar or map params are rejected rather than silently wrapped, so argument count remains explicit for interoperating peers.
