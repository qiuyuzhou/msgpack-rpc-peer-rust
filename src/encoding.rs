use std::io::Write;

use rmp_serde::encode::Error;
use serde::{Serialize, Serializer, ser};

/// One bounded sink, including unknown-length containers whose headers are backpatched.
struct Buffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other("message byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode<T: Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, crate::Error> {
    let mut buffer = Buffer {
        bytes: Vec::new(),
        limit,
    };
    value.serialize(&mut buffer).map_err(|_| {
        crate::Error::Encode("value cannot be serialized within message limit".into())
    })?;
    Ok(buffer.bytes)
}

impl Buffer {
    fn header(&mut self, map: bool, length: usize) -> Result<(), Error> {
        u32::try_from(length)
            .map_err(|_| Error::InvalidDataModel("container length exceeds u32"))?;
        let mut serializer = rmp_serde::Serializer::new(self);
        if map {
            serializer.serialize_map(Some(length))?;
        } else {
            serializer.serialize_tuple(length)?;
        }
        Ok(())
    }

    fn container(&mut self, map: bool, length: Option<usize>) -> Result<Compound<'_>, Error> {
        let patch = if let Some(length) = length {
            self.header(map, length)?;
            None
        } else {
            let offset = self.bytes.len();
            self.write_all(&[if map { 0xdf } else { 0xdd }, 0, 0, 0, 0])
                .map_err(|_| Error::InvalidDataModel("message byte limit exceeded"))?;
            Some(offset)
        };
        Ok(Compound {
            buffer: self,
            map,
            length,
            patch,
            values: 0,
        })
    }
}

macro_rules! primitive {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, value: $ty) -> Result<(), Error> {
            rmp_serde::Serializer::new(self).$method(value)
        })*
    };
}

impl<'a> Serializer for &'a mut Buffer {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn collect_str<T: std::fmt::Display + ?Sized>(self, value: &T) -> Result<(), Error> {
        struct TextSink<'a>(&'a mut Buffer);
        impl std::fmt::Write for TextSink<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0
                    .write_all(text.as_bytes())
                    .map_err(|_| std::fmt::Error)
            }
        }
        let offset = self.bytes.len();
        self.write_all(&[0xdb, 0, 0, 0, 0])
            .map_err(|_| Error::InvalidDataModel("message byte limit exceeded"))?;
        std::fmt::write(&mut TextSink(self), format_args!("{value}"))
            .map_err(|_| Error::InvalidDataModel("formatted string exceeds message limit"))?;
        let length = u32::try_from(self.bytes.len() - offset - 5)
            .map_err(|_| Error::InvalidDataModel("string length exceeds u32"))?;
        self.bytes[offset + 1..offset + 5].copy_from_slice(&length.to_be_bytes());
        Ok(())
    }

    primitive! {
        serialize_bool(bool), serialize_i8(i8), serialize_i16(i16), serialize_i32(i32),
        serialize_i64(i64), serialize_i128(i128), serialize_u8(u8), serialize_u16(u16),
        serialize_u32(u32), serialize_u64(u64), serialize_u128(u128), serialize_f32(f32),
        serialize_f64(f64), serialize_char(char), serialize_str(&str), serialize_bytes(&[u8])
    }

    fn serialize_none(self) -> Result<(), Error> {
        self.serialize_unit()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        rmp_serde::Serializer::new(self).serialize_unit()
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Error> {
        self.header(false, 0)
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<(), Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        if name == rmp_serde::MSGPACK_EXT_STRUCT_NAME {
            // rmp-serde's extension adapter writes directly to the bounded sink.
            rmp_serde::Serializer::new(self).serialize_newtype_struct(name, value)
        } else {
            value.serialize(self)
        }
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.header(true, 1)?;
        variant.serialize(&mut *self)?;
        value.serialize(self)
    }
    fn serialize_seq(self, length: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        self.container(false, length)
    }
    fn serialize_tuple(self, length: usize) -> Result<Self::SerializeTuple, Error> {
        self.container(false, Some(length))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        self.container(false, Some(length))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        self.header(true, 1)?;
        variant.serialize(&mut *self)?;
        self.container(false, Some(length))
    }
    fn serialize_map(self, length: Option<usize>) -> Result<Self::SerializeMap, Error> {
        self.container(true, length)
    }
    fn serialize_struct(
        self,
        _: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        self.container(true, Some(length))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        self.header(true, 1)?;
        variant.serialize(&mut *self)?;
        self.container(true, Some(length))
    }
}

struct Compound<'a> {
    buffer: &'a mut Buffer,
    map: bool,
    length: Option<usize>,
    patch: Option<usize>,
    values: usize,
}

impl Compound<'_> {
    fn value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut *self.buffer)?;
        self.values = self
            .values
            .checked_add(1)
            .ok_or(Error::InvalidDataModel("container length exceeds u32"))?;
        Ok(())
    }
    fn finish(self) -> Result<(), Error> {
        if self.map && self.values % 2 != 0 {
            return Err(Error::InvalidDataModel("map has unmatched key"));
        }
        let length = if self.map {
            self.values / 2
        } else {
            self.values
        };
        if self.length.is_some_and(|expected| expected != length) {
            return Err(Error::InvalidDataModel("container length mismatch"));
        }
        if let Some(offset) = self.patch {
            let length = u32::try_from(length)
                .map_err(|_| Error::InvalidDataModel("container length exceeds u32"))?;
            self.buffer.bytes[offset + 1..offset + 5].copy_from_slice(&length.to_be_bytes());
        }
        Ok(())
    }
}

macro_rules! sequence {
    ($trait:ident, $method:ident) => {
        impl ser::$trait for Compound<'_> {
            type Ok = ();
            type Error = Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
                self.value(value)
            }
            fn end(self) -> Result<(), Error> {
                self.finish()
            }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);

impl ser::SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.value(key)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.value(value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}

macro_rules! structure {
    ($trait:ident) => {
        impl ser::$trait for Compound<'_> {
            type Ok = ();
            type Error = Error;
            fn serialize_field<T: Serialize + ?Sized>(
                &mut self,
                key: &'static str,
                value: &T,
            ) -> Result<(), Error> {
                self.value(key)?;
                self.value(value)
            }
            fn end(self) -> Result<(), Error> {
                self.finish()
            }
        }
    };
}
structure!(SerializeStruct);
structure!(SerializeStructVariant);
