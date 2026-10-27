use crate::Value;

/// Local failures. Terminal causes are cloned into every outstanding request.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum Error {
    /// The whole-call deadline expired; remote effects may still occur.
    #[error("request timed out")]
    Timeout,
    /// Startup configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    /// The underlying byte stream failed.
    #[error("transport failure: {0}")]
    Transport(String),
    /// MessagePack bytes could not be decoded.
    #[error("MessagePack decoding failed: {0}")]
    Decode(String),
    /// A complete message violated the RPC envelope contract.
    #[error("invalid RPC envelope: {0}")]
    Protocol(&'static str),
    /// A typed value could not be encoded.
    #[error("MessagePack serialization failed: {0}")]
    Encode(String),
    /// Arguments must be an array and must match the method's declared type.
    #[error("invalid parameters")]
    InvalidParams,
    /// A successful remote result did not match the requested Rust type.
    #[error("result does not match the requested type")]
    InvalidResult,
    /// A remote peer returned an arbitrary non-nil error value.
    #[error("remote method failed")]
    Remote(Value),
    /// A fixed connection resource budget was exceeded.
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(&'static str),
    /// The connection was terminated locally or reached clean EOF.
    #[error("connection closed")]
    Closed,
    /// A method name was registered twice before startup.
    #[error("duplicate method registration")]
    DuplicateMethod,
    /// Startup requires an existing Tokio runtime.
    #[error("startup requires a Tokio runtime")]
    NoRuntime,
}

/// A method failure sent to the remote peer. Its value is always non-nil.
#[derive(Clone, Debug)]
pub struct RemoteError(Value);

impl RemoteError {
    /// Preserve an application-defined error; nil cannot represent RPC failure.
    pub fn new(value: Value) -> Result<Self, Error> {
        if value.is_nil() {
            Err(Error::Protocol("remote error must be non-nil"))
        } else {
            Ok(Self(value))
        }
    }

    /// Return the original error value without imposing a schema.
    pub fn into_value(self) -> Value {
        self.0
    }

    pub(crate) fn builtin(code: i32, message: &'static str) -> Self {
        Self(Value::Map(vec![
            ("code".into(), code.into()),
            ("message".into(), message.into()),
        ]))
    }
}
