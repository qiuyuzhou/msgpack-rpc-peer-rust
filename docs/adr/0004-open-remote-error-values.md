# Preserve arbitrary remote error values

Built-in remote failures use a documented map containing code and message, while received non-nil error values are preserved even when they have another structure. MessagePack-RPC defines no common error schema, so requiring our schema would reject interoperable peers; the built-in codes are library conventions rather than protocol extensions.
