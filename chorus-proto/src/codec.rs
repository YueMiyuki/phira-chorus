use bytes::{BufMut, Bytes, BytesMut};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    UnexpectedEof,
    BadVarint,
    StringTooLong { max: usize },
    LengthOutOfRange,
    UnknownTag { ty: &'static str, tag: u8 },
    InvalidRoomId,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected end of payload"),
            Self::BadVarint => f.write_str("malformed variable-length integer"),
            Self::StringTooLong { max } => write!(f, "string longer than {max} bytes"),
            Self::LengthOutOfRange => f.write_str("sequence length exceeds payload"),
            Self::UnknownTag { ty, tag } => write!(f, "unknown {ty} tag {tag}"),
            Self::InvalidRoomId => f.write_str("invalid room id"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub type Result<T, E = DecodeError> = std::result::Result<T, E>;

pub trait Encode {
    fn encode(&self, w: &mut BytesMut);
}

pub trait Decode: Sized {
    fn decode(r: &mut Reader<'_>) -> Result<Self>;
}

pub struct Reader<'a> {
    src: &'a Bytes,
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(src: &'a Bytes) -> Self {
        Self { src, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.src.len() - self.pos
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let src: &'a [u8] = self.src.as_ref();
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= src.len())
            .ok_or(DecodeError::UnexpectedEof)?;
        let out = &src[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub fn skip(&mut self, n: usize) -> Result<()> {
        self.take(n).map(drop)
    }

    pub fn slice(&self, start: usize, end: usize) -> Bytes {
        self.src.slice(start..end)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.fixed::<1>()?[0])
    }

    pub fn uleb(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for i in 0..10 {
            let byte = self.u8()?;
            let bits = u64::from(byte & 0x7f);
            if i == 9 && bits > 1 {
                return Err(DecodeError::BadVarint);
            }
            value |= bits << (7 * i);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError::BadVarint)
    }

    /// Rejects counts that cannot fit in the remaining bytes
    pub fn seq_len(&mut self, min_item_len: usize) -> Result<usize> {
        let len = self.uleb()?;
        let max = self.remaining() / min_item_len.max(1);
        if len > max as u64 {
            return Err(DecodeError::LengthOutOfRange);
        }
        Ok(len as usize)
    }

    pub fn str_bytes(&mut self, max: usize) -> Result<&'a [u8]> {
        let len = self.uleb()?;
        if len > max as u64 {
            return Err(DecodeError::StringTooLong { max });
        }
        self.take(len as usize)
    }

    pub fn string(&mut self, max: usize) -> Result<String> {
        Ok(String::from_utf8_lossy(self.str_bytes(max)?).into_owned())
    }

    pub fn read<T: Decode>(&mut self) -> Result<T> {
        T::decode(self)
    }
}

pub fn put_uleb(w: &mut BytesMut, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            w.put_u8(byte);
            return;
        }
        w.put_u8(byte | 0x80);
    }
}

pub fn put_str(w: &mut BytesMut, s: &str) {
    put_uleb(w, s.len() as u64);
    w.put_slice(s.as_bytes());
}

impl Encode for () {
    fn encode(&self, _w: &mut BytesMut) {}
}

impl Decode for () {
    fn decode(_r: &mut Reader<'_>) -> Result<Self> {
        Ok(())
    }
}

impl Encode for u8 {
    fn encode(&self, w: &mut BytesMut) {
        w.put_u8(*self);
    }
}

impl Decode for u8 {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        r.u8()
    }
}

impl Encode for i8 {
    fn encode(&self, w: &mut BytesMut) {
        w.put_i8(*self);
    }
}

impl Decode for i8 {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(r.u8()? as i8)
    }
}

impl Encode for bool {
    fn encode(&self, w: &mut BytesMut) {
        w.put_u8(u8::from(*self));
    }
}

impl Decode for bool {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(r.u8()? == 1)
    }
}

macro_rules! little_endian {
    ($($ty:ty => $put:ident),* $(,)?) => {$(
        impl Encode for $ty {
            fn encode(&self, w: &mut BytesMut) {
                w.$put(*self);
            }
        }

        impl Decode for $ty {
            fn decode(r: &mut Reader<'_>) -> Result<Self> {
                Ok(<$ty>::from_le_bytes(r.fixed()?))
            }
        }
    )*};
}

little_endian! {
    u16 => put_u16_le,
    u32 => put_u32_le,
    u64 => put_u64_le,
    i32 => put_i32_le,
    i64 => put_i64_le,
    f32 => put_f32_le,
}

impl Encode for String {
    fn encode(&self, w: &mut BytesMut) {
        put_str(w, self);
    }
}

impl Decode for String {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        r.string(usize::MAX)
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, w: &mut BytesMut) {
        match self {
            Some(value) => {
                w.put_u8(1);
                value.encode(w);
            }
            None => w.put_u8(0),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(if r.read::<bool>()? {
            Some(r.read()?)
        } else {
            None
        })
    }
}

impl<T: Encode, E: Encode> Encode for std::result::Result<T, E> {
    fn encode(&self, w: &mut BytesMut) {
        match self {
            Ok(value) => {
                w.put_u8(1);
                value.encode(w);
            }
            Err(err) => {
                w.put_u8(0);
                err.encode(w);
            }
        }
    }
}

impl<T: Decode, E: Decode> Decode for std::result::Result<T, E> {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(if r.read::<bool>()? {
            Ok(r.read()?)
        } else {
            Err(r.read()?)
        })
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, w: &mut BytesMut) {
        put_uleb(w, self.len() as u64);
        for item in self {
            item.encode(w);
        }
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let len = r.seq_len(1)?;
        let mut out = Vec::with_capacity(len.min(1024));
        for _ in 0..len {
            out.push(r.read()?);
        }
        Ok(out)
    }
}

impl<A: Encode, B: Encode> Encode for (A, B) {
    fn encode(&self, w: &mut BytesMut) {
        self.0.encode(w);
        self.1.encode(w);
    }
}

impl<A: Decode, B: Decode> Decode for (A, B) {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok((r.read()?, r.read()?))
    }
}
