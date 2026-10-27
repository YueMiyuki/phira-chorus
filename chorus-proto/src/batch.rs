use crate::codec::{Decode, DecodeError, Encode, Reader, Result, put_uleb};
use crate::model::{JudgeEvent, Judgement, TouchFrame};
use bytes::{BufMut, Bytes, BytesMut};
use std::{fmt, marker::PhantomData};

#[derive(Clone, PartialEq, Eq)]
pub struct TouchBatch {
    raw: Bytes,
    len: usize,
}

#[derive(Clone, PartialEq, Eq)]
pub struct JudgeBatch {
    raw: Bytes,
    len: usize,
}

fn encode_items<T: Encode>(items: &[T], hint: usize) -> Bytes {
    let mut w = BytesMut::with_capacity(5 + items.len() * hint);
    put_uleb(&mut w, items.len() as u64);
    for item in items {
        item.encode(&mut w);
    }
    w.freeze()
}

impl TouchBatch {
    pub fn new(frames: &[TouchFrame]) -> Self {
        Self {
            raw: encode_items(
                frames,
                TouchFrame::MIN_ENCODED_LEN + TouchFrame::POINT_ENCODED_LEN,
            ),
            len: frames.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn raw(&self) -> &Bytes {
        &self.raw
    }

    pub fn iter(&self) -> Items<'_, TouchFrame> {
        Items::new(&self.raw, self.len)
    }
}

impl JudgeBatch {
    pub fn new(events: &[JudgeEvent]) -> Self {
        Self {
            raw: encode_items(events, JudgeEvent::ENCODED_LEN),
            len: events.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn raw(&self) -> &Bytes {
        &self.raw
    }

    pub fn iter(&self) -> Items<'_, JudgeEvent> {
        Items::new(&self.raw, self.len)
    }
}

impl Encode for TouchBatch {
    fn encode(&self, w: &mut BytesMut) {
        w.put_slice(&self.raw);
    }
}

impl Encode for JudgeBatch {
    fn encode(&self, w: &mut BytesMut) {
        w.put_slice(&self.raw);
    }
}

impl Decode for TouchBatch {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let start = r.position();
        let len = r.seq_len(TouchFrame::MIN_ENCODED_LEN)?;
        for _ in 0..len {
            r.skip(4)?;
            let points = r.seq_len(TouchFrame::POINT_ENCODED_LEN)?;
            r.skip(points * TouchFrame::POINT_ENCODED_LEN)?;
        }
        Ok(Self {
            raw: r.slice(start, r.position()),
            len,
        })
    }
}

impl Decode for JudgeBatch {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let start = r.position();
        let len = r.seq_len(JudgeEvent::ENCODED_LEN)?;
        for _ in 0..len {
            r.skip(JudgeEvent::ENCODED_LEN - 1)?;
            let tag = r.u8()?;
            if Judgement::from_u8(tag).is_none() {
                return Err(DecodeError::UnknownTag {
                    ty: "Judgement",
                    tag,
                });
            }
        }
        Ok(Self {
            raw: r.slice(start, r.position()),
            len,
        })
    }
}

impl fmt::Debug for TouchBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TouchBatch")
            .field("frames", &self.len)
            .field("bytes", &self.raw.len())
            .finish()
    }
}

impl fmt::Debug for JudgeBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JudgeBatch")
            .field("events", &self.len)
            .field("bytes", &self.raw.len())
            .finish()
    }
}

pub struct Items<'a, T> {
    raw: &'a Bytes,
    pos: usize,
    left: usize,
    _item: PhantomData<T>,
}

impl<'a, T> Items<'a, T> {
    fn new(raw: &'a Bytes, len: usize) -> Self {
        let mut r = Reader::new(raw);
        let left = r.uleb().map_or(0, |_| len);
        Self {
            raw,
            pos: r.position(),
            left,
            _item: PhantomData,
        }
    }
}

impl<T: Decode> Iterator for Items<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.left == 0 {
            return None;
        }
        let mut r = Reader::new(self.raw);
        r.skip(self.pos).ok()?;
        let item = r.read().ok();
        self.pos = r.position();
        self.left = if item.is_some() { self.left - 1 } else { 0 };
        item
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.left))
    }
}
