# Separate application traffic from internal responses

Application calls and notifications wait for bounded outgoing capacity, while internally generated responses use a separate bounded queue. The writer services both queues fairly, and the receive loop never waits for write capacity. If the response queue fills because the remote peer is not reading, the connection terminates and pending calls fail; finite buffering cannot guarantee delivery to a peer that stops reading.
