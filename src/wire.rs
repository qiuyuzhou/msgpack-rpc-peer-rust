use std::io::{Cursor, ErrorKind};

use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{Error, Value};

pub(crate) const MAX_BYTES: usize = 1024 * 1024;

pub(crate) enum Message {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: u32,
        method: String,
        params: Value,
    },
    Response {
        id: u32,
        outcome: Result<Value, Error>,
    },
}

fn id(value: &Value) -> Result<u32, Error> {
    value
        .as_u64()
        .and_then(|n| n.try_into().ok())
        .ok_or(Error::Protocol("ID must be a u32"))
}

fn parse(value: Value) -> Result<Message, Error> {
    let Value::Array(mut fields) = value else {
        return Err(Error::Protocol("message must be an array"));
    };
    if fields.first().and_then(Value::as_u64) == Some(2) {
        if fields.len() != 3 {
            return Err(Error::Protocol("notification must have three fields"));
        }
        let params = fields.pop().unwrap();
        let method = fields.pop().unwrap();
        let method = method
            .as_str()
            .ok_or(Error::Protocol("method must be a string"))?
            .to_owned();
        if !params.is_array() {
            return Err(Error::Protocol("params must be an array"));
        }
        return Ok(Message::Notification { method, params });
    }
    if fields.len() != 4 {
        return Err(Error::Protocol("request or response must have four fields"));
    }
    let last = fields.pop().unwrap();
    let third = fields.pop().unwrap();
    let id = id(&fields[1])?;
    match fields[0].as_u64() {
        Some(0) => {
            let method = third
                .as_str()
                .ok_or(Error::Protocol("method must be a string"))?
                .to_owned();
            if !last.is_array() {
                return Err(Error::Protocol("params must be an array"));
            }
            Ok(Message::Request {
                id,
                method,
                params: last,
            })
        }
        Some(1) => {
            if !third.is_nil() && !last.is_nil() {
                return Err(Error::Protocol("error response must have nil result"));
            }
            let outcome = if third.is_nil() {
                Ok(last)
            } else {
                Err(Error::Remote(third))
            };
            Ok(Message::Response { id, outcome })
        }
        _ => Err(Error::Protocol("unsupported message type")),
    }
}

// The persistent buffer makes read cancellation safe when another supervisor branch wins.
pub(crate) async fn read<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> Result<Message, Error> {
    loop {
        if !buffer.is_empty() {
            let mut cursor = Cursor::new(buffer.as_slice());
            match rmpv::decode::read_value_with_max_depth(&mut cursor, 64) {
                Ok(value) => {
                    let used = cursor.position() as usize;
                    buffer.drain(..used);
                    return parse(value);
                }
                Err(
                    rmpv::decode::Error::InvalidMarkerRead(ref e)
                    | rmpv::decode::Error::InvalidDataRead(ref e),
                ) if e.kind() == ErrorKind::UnexpectedEof => {}
                Err(e) => return Err(Error::Decode(e.to_string())),
            }
        }
        if buffer.len() >= MAX_BYTES {
            return Err(Error::ResourceLimit("incoming message bytes"));
        }
        let mut chunk = [0; 4096];
        let capacity = chunk.len().min(MAX_BYTES - buffer.len());
        let n = reader
            .read(&mut chunk[..capacity])
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if n == 0 {
            return Err(if buffer.is_empty() {
                Error::Closed
            } else {
                Error::Decode("truncated message at EOF".into())
            });
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
}

struct LimitedBuffer(Vec<u8>);

impl std::io::Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_BYTES - self.0.len() {
            return Err(std::io::Error::other("message byte limit exceeded"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
    let mut buffer = LimitedBuffer(Vec::new());
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut buffer).with_struct_map())
        .map_err(|e| Error::Encode(e.to_string()))?;
    Ok(buffer.0)
}

pub(crate) fn decode<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    rmpv::ext::from_value(value).map_err(|_| Error::InvalidResult)
}

pub(crate) fn response(
    id: u32,
    outcome: Result<Value, crate::RemoteError>,
) -> Result<Vec<u8>, Error> {
    let (error, result) = match outcome {
        Ok(result) => (Value::Nil, result),
        Err(error) => (error.into_value(), Value::Nil),
    };
    encode(&(1u8, id, error, result))
}
