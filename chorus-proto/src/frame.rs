use crate::codec::Encode;
use bytes::{Buf, BufMut, Bytes, BytesMut};

const PREFIX_MAX: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    BadLength,
    TooLarge(u64),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadLength => f.write_str("malformed frame length"),
            Self::TooLarge(len) => write!(f, "frame of {len} bytes exceeds limit"),
        }
    }
}

impl std::error::Error for FrameError {}

pub fn encode_frame<T: Encode + ?Sized>(msg: &T) -> Bytes {
    encode_frame_with_capacity(msg, 64)
}

pub fn encode_frame_with_capacity<T: Encode + ?Sized>(msg: &T, capacity: usize) -> Bytes {
    // Leave room for the longest prefix, then write the real one right-aligned
    // against the payload so nothing has to move
    let mut buf = BytesMut::with_capacity(PREFIX_MAX + capacity);
    buf.put_bytes(0, PREFIX_MAX);
    msg.encode(&mut buf);
    let mut len = (buf.len() - PREFIX_MAX) as u64;
    let mut prefix = [0u8; PREFIX_MAX];
    let mut n = 0;
    loop {
        let byte = (len & 0x7f) as u8;
        len >>= 7;
        if len == 0 {
            prefix[n] = byte;
            n += 1;
            break;
        }
        prefix[n] = byte | 0x80;
        n += 1;
    }
    let start = PREFIX_MAX - n;
    buf[start..PREFIX_MAX].copy_from_slice(&prefix[..n]);
    buf.advance(start);
    buf.freeze()
}

pub fn split_frame(buf: &mut BytesMut, max_len: usize) -> Result<Option<Bytes>, FrameError> {
    let mut len = 0u64;
    let mut prefix = 0;
    loop {
        let Some(&byte) = buf.get(prefix) else {
            return Ok(None);
        };
        len |= u64::from(byte & 0x7f) << (7 * prefix);
        prefix += 1;
        if byte & 0x80 == 0 {
            break;
        }
        if prefix == PREFIX_MAX {
            return Err(FrameError::BadLength);
        }
    }
    if len > max_len as u64 {
        return Err(FrameError::TooLarge(len));
    }
    let total = prefix + len as usize;
    if buf.len() < total {
        buf.reserve(total - buf.len());
        return Ok(None);
    }
    buf.advance(prefix);
    Ok(Some(buf.split_to(len as usize).freeze()))
}
