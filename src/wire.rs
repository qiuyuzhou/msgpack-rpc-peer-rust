use std::io::Cursor;

use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{Error, Value};

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

/// Scan boundaries and budgets before rmpv can allocate decoded containers.
#[derive(Default)]
pub(crate) struct Decoder {
    buffer: Vec<u8>,
    offset: usize,
    remaining: Vec<usize>,
    nodes: usize,
}

impl Decoder {
    fn boundary(&mut self, config: &crate::Config) -> Result<Option<usize>, Error> {
        if self.remaining.is_empty() {
            self.remaining.push(1);
        }
        while self.offset < self.buffer.len() {
            let bytes = &self.buffer[self.offset..];
            let marker = bytes[0];
            let (header, payload, children) = match marker {
                0x00..=0x7f | 0xc0 | 0xc2..=0xc3 | 0xe0..=0xff => (1, 0, 0),
                0x80..=0x8f => (1, 0, usize::from(marker & 15) * 2),
                0x90..=0x9f => (1, 0, usize::from(marker & 15)),
                0xa0..=0xbf => (1, usize::from(marker & 31), 0),
                0xc1 => return Err(Error::Decode("reserved MessagePack marker".into())),
                0xc4..=0xc9 | 0xd9..=0xdf => {
                    let width = match marker {
                        0xc4 | 0xc7 | 0xd9 => 1,
                        0xc5 | 0xc8 | 0xda | 0xdc | 0xde => 2,
                        _ => 4,
                    };
                    if bytes.len() < 1 + width {
                        return Ok(None);
                    }
                    let length = bytes[1..1 + width]
                        .iter()
                        .fold(0usize, |n, b| (n << 8) | usize::from(*b));
                    match marker {
                        0xdc | 0xdd => (1 + width, 0, length),
                        0xde | 0xdf => (
                            1 + width,
                            0,
                            length
                                .checked_mul(2)
                                .ok_or(Error::ResourceLimit("decoded value nodes"))?,
                        ),
                        0xc7..=0xc9 => (
                            1 + width,
                            length
                                .checked_add(1)
                                .ok_or(Error::ResourceLimit("incoming message bytes"))?,
                            0,
                        ),
                        _ => (1 + width, length, 0),
                    }
                }
                0xca | 0xce | 0xd2 => (1, 4, 0),
                0xcb | 0xcf | 0xd3 => (1, 8, 0),
                0xcc | 0xd0 => (1, 1, 0),
                0xcd | 0xd1 => (1, 2, 0),
                0xd4 => (1, 2, 0),
                0xd5 => (1, 3, 0),
                0xd6 => (1, 5, 0),
                0xd7 => (1, 9, 0),
                0xd8 => (1, 17, 0),
            };
            let end = self
                .offset
                .checked_add(header)
                .and_then(|n| n.checked_add(payload))
                .ok_or(Error::ResourceLimit("incoming message bytes"))?;
            if end > config.max_incoming_bytes {
                return Err(Error::ResourceLimit("incoming message bytes"));
            }
            let outstanding: usize = self.remaining.iter().sum();
            if self.nodes >= config.max_nodes
                || children > config.max_nodes - self.nodes - outstanding
            {
                return Err(Error::ResourceLimit("decoded value nodes"));
            }
            let container = (0x80..=0x9f).contains(&marker) || (0xdc..=0xdf).contains(&marker);
            if container && self.remaining.len() > config.max_depth {
                return Err(Error::ResourceLimit("MessagePack nesting depth"));
            }
            if end > self.buffer.len() {
                return Ok(None);
            }
            self.offset = end;
            self.nodes += 1;
            *self.remaining.last_mut().unwrap() -= 1;
            if children != 0 {
                self.remaining.push(children);
            }
            while self.remaining.last() == Some(&0) {
                self.remaining.pop();
            }
            if self.remaining.is_empty() {
                return Ok(Some(end));
            }
        }
        Ok(None)
    }
}

// Decoder state survives cancellation when another supervisor branch wins.
pub(crate) async fn read<R: AsyncRead + Unpin>(
    reader: &mut R,
    decoder: &mut Decoder,
    config: &crate::Config,
) -> Result<Message, Error> {
    loop {
        if let Some(used) = decoder.boundary(config)? {
            // rmpv charges twice per container and three times for string/extension leaves.
            let value = rmpv::decode::read_value_with_max_depth(
                &mut Cursor::new(&decoder.buffer[..used]),
                config.max_depth * 2 + 3,
            )
            .map_err(|e| Error::Decode(e.to_string()))?;
            decoder.buffer.drain(..used);
            decoder.offset = 0;
            decoder.nodes = 0;
            return parse(value);
        }
        if decoder.buffer.len() >= config.max_incoming_bytes {
            return Err(Error::ResourceLimit("incoming message bytes"));
        }
        let mut chunk = [0; 4096];
        let capacity = chunk
            .len()
            .min(config.max_incoming_bytes - decoder.buffer.len());
        let n = reader
            .read(&mut chunk[..capacity])
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if n == 0 {
            return Err(if decoder.buffer.is_empty() {
                Error::Closed
            } else {
                Error::Decode("truncated message at EOF".into())
            });
        }
        decoder.buffer.extend_from_slice(&chunk[..n]);
    }
}

pub(crate) use crate::encoding::encode;

pub(crate) fn value(bytes: Vec<u8>, config: &crate::Config) -> Result<Value, Error> {
    // Custom serializers can advertise containers without supplying their elements.
    let mut limits = config.clone();
    limits.max_incoming_bytes = limits.max_outgoing_bytes;
    let mut decoder = Decoder {
        buffer: bytes,
        ..Decoder::default()
    };
    let Some(used) = decoder.boundary(&limits)? else {
        return Err(Error::Encode("incomplete serialized value".into()));
    };
    if used != decoder.buffer.len() {
        return Err(Error::Encode("multiple serialized values".into()));
    }
    rmpv::decode::read_value_with_max_depth(
        &mut decoder.buffer.as_slice(),
        config.max_depth * 2 + 3,
    )
    .map_err(|_| Error::Encode("invalid serialized value".into()))
}

pub(crate) fn decode<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    rmpv::ext::from_value(value).map_err(|_| Error::InvalidResult)
}

pub(crate) fn response(
    id: u32,
    outcome: Result<Value, crate::RemoteError>,
    limit: usize,
) -> Result<Vec<u8>, Error> {
    let (error, result) = match outcome {
        Ok(result) => (Value::Nil, result),
        Err(error) => (error.into_value(), Value::Nil),
    };
    encode(&(1u8, id, error, result), limit)
}
