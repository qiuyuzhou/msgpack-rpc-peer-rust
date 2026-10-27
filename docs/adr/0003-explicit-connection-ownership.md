# Separate peer handles from connection ownership

Method registration is frozen before startup, which returns a cloneable RpcPeer handle and a separate connection owner that supervises background tasks and exposes shutdown and completion. Handlers receive a peer handle for reverse calls. Dropping the connection owner initiates task cancellation, so retaining application or handler peer handles does not implicitly keep the connection running.
