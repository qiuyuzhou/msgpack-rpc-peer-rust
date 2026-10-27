# Close on invalid traffic and ignore unmatched responses

Malformed envelopes, duplicate active incoming request IDs, and message size or nesting depth violations terminate the connection. Responses with unknown IDs are discarded because they can legitimately arrive after local cancellation, while valid requests with unknown methods or invalid arguments receive remote errors without terminating the connection. These policies preserve a useful connection after application mistakes while avoiding ambiguous recovery from invalid protocol traffic.
