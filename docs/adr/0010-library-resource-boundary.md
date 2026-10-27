# Bound resources owned by the library

Resource limits cover library-controlled buffers, pending calls, handler tasks, and decoded values. Incoming size, nesting, and container lengths are checked before allocating decoded containers, and outgoing encoding enforces its byte limit while writing. Application handlers and custom Serde implementations control their own allocations and CPU use; asynchronous timeouts cannot preempt synchronous application code that blocks an executor thread.
