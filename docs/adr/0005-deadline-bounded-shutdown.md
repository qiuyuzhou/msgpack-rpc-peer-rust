# Drain connections within a shutdown deadline

Graceful shutdown rejects new application-initiated calls while allowing admitted handlers and pending calls to finish within a configurable deadline. Existing handlers may initiate reverse calls, and incoming requests remain admissible within resource limits because the protocol does not identify reverse calls separately. At the deadline, remaining work is canceled and the transport is closed; draining never promises completion under sustained incoming traffic.
