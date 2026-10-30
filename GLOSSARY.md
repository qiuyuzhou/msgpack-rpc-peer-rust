# MessagePack-RPC Peer

Two equal RPC peers exchange requests, responses, and notifications over one bidirectional connection.

## Language

**RPC peer**:
An endpoint that can initiate and receive requests and notifications over the same connection.
_Avoid_: Client, server

**Request**:
A method invocation that requires a response bearing the same message ID.

**Response**:
The successful result or remote error returned for a request.

**Notification**:
A method invocation for which no response is sent.

**Outgoing request**:
A request initiated by the local peer, identified within that peer's message ID namespace.

**Incoming request**:
A request initiated by the remote peer, identified within the remote peer's message ID namespace.

**Reverse RPC call**:
A request initiated by a peer while it handles a request from the other peer.

**Local call cancellation**:
Ending the local wait for a response without promising that the remote operation stops.
_Avoid_: Remote cancellation

**Shared RPC session**:
The target-side session associated with one persistent connection and shared by callers whose own connections may be short-lived.

**Session forwarder**:
An intermediary RPC peer that forwards method invocations from callers through a shared RPC session and returns request outcomes to their originating callers.
