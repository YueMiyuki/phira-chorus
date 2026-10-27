use crate::codec::{Decode, DecodeError, Encode, Reader, Result, put_str, put_uleb};
use bytes::{BufMut, BytesMut};
use half::f16;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompactPos {
    x: f16,
    y: f16,
}

impl CompactPos {
    pub const ENCODED_LEN: usize = 4;

    pub fn new(x: f32, y: f32) -> Self {
        Self {
            x: f16::from_f32(x),
            y: f16::from_f32(y),
        }
    }

    pub fn x(&self) -> f32 {
        self.x.to_f32()
    }

    pub fn y(&self) -> f32 {
        self.y.to_f32()
    }
}

impl Encode for CompactPos {
    fn encode(&self, w: &mut BytesMut) {
        w.put_u16_le(self.x.to_bits());
        w.put_u16_le(self.y.to_bits());
    }
}

impl Decode for CompactPos {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            x: f16::from_bits(r.read()?),
            y: f16::from_bits(r.read()?),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TouchFrame {
    pub time: f32,
    pub points: Vec<(i8, CompactPos)>,
}

impl TouchFrame {
    pub const MIN_ENCODED_LEN: usize = 5;
    pub const POINT_ENCODED_LEN: usize = 1 + CompactPos::ENCODED_LEN;
}

impl Encode for TouchFrame {
    fn encode(&self, w: &mut BytesMut) {
        self.time.encode(w);
        self.points.encode(w);
    }
}

impl Decode for TouchFrame {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            time: r.read()?,
            points: r.read()?,
        })
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Judgement {
    Perfect = 0,
    Good = 1,
    Bad = 2,
    Miss = 3,
    HoldPerfect = 4,
    HoldGood = 5,
}

impl Judgement {
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::Perfect,
            1 => Self::Good,
            2 => Self::Bad,
            3 => Self::Miss,
            4 => Self::HoldPerfect,
            5 => Self::HoldGood,
            _ => return None,
        })
    }
}

impl Encode for Judgement {
    fn encode(&self, w: &mut BytesMut) {
        w.put_u8(*self as u8);
    }
}

impl Decode for Judgement {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let tag = r.u8()?;
        Self::from_u8(tag).ok_or(DecodeError::UnknownTag {
            ty: "Judgement",
            tag,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct JudgeEvent {
    pub time: f32,
    pub line_id: u32,
    pub note_id: u32,
    pub judgement: Judgement,
}

impl JudgeEvent {
    pub const ENCODED_LEN: usize = 13;
}

impl Encode for JudgeEvent {
    fn encode(&self, w: &mut BytesMut) {
        self.time.encode(w);
        self.line_id.encode(w);
        self.note_id.encode(w);
        self.judgement.encode(w);
    }
}

impl Decode for JudgeEvent {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            time: r.read()?,
            line_id: r.read()?,
            note_id: r.read()?,
            judgement: r.read()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoomId(Box<str>);

impl RoomId {
    pub const MAX_LEN: usize = 20;

    fn is_valid(bytes: &[u8]) -> bool {
        !bytes.is_empty()
            && bytes.len() <= Self::MAX_LEN
            && bytes
                .iter()
                .all(|&b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for RoomId {
    type Err = DecodeError;

    fn from_str(value: &str) -> Result<Self> {
        if !Self::is_valid(value.as_bytes()) {
            return Err(DecodeError::InvalidRoomId);
        }
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for RoomId {
    type Error = DecodeError;

    fn try_from(value: String) -> Result<Self> {
        if !Self::is_valid(value.as_bytes()) {
            return Err(DecodeError::InvalidRoomId);
        }
        Ok(Self(value.into_boxed_str()))
    }
}

impl fmt::Display for RoomId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Encode for RoomId {
    fn encode(&self, w: &mut BytesMut) {
        put_str(w, &self.0);
    }
}

impl Decode for RoomId {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let bytes = r.str_bytes(Self::MAX_LEN)?;
        if !Self::is_valid(bytes) {
            return Err(DecodeError::InvalidRoomId);
        }
        Ok(Self(String::from_utf8_lossy(bytes).into()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserInfo {
    pub id: i32,
    pub name: String,
    pub monitor: bool,
}

impl Encode for UserInfo {
    fn encode(&self, w: &mut BytesMut) {
        self.id.encode(w);
        put_str(w, &self.name);
        self.monitor.encode(w);
    }
}

impl Decode for UserInfo {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            id: r.read()?,
            name: r.read()?,
            monitor: r.read()?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoomState {
    SelectChart(Option<i32>),
    WaitingForReady,
    Playing,
}

impl Default for RoomState {
    fn default() -> Self {
        Self::SelectChart(None)
    }
}

impl Encode for RoomState {
    fn encode(&self, w: &mut BytesMut) {
        match self {
            Self::SelectChart(chart) => {
                w.put_u8(0);
                chart.encode(w);
            }
            Self::WaitingForReady => w.put_u8(1),
            Self::Playing => w.put_u8(2),
        }
    }
}

impl Decode for RoomState {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            0 => Self::SelectChart(r.read()?),
            1 => Self::WaitingForReady,
            2 => Self::Playing,
            tag => {
                return Err(DecodeError::UnknownTag {
                    ty: "RoomState",
                    tag,
                });
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientRoomState {
    pub id: RoomId,
    pub state: RoomState,
    pub live: bool,
    pub locked: bool,
    pub cycle: bool,
    pub is_host: bool,
    pub is_ready: bool,
    pub users: Vec<UserInfo>,
}

impl Encode for ClientRoomState {
    fn encode(&self, w: &mut BytesMut) {
        self.id.encode(w);
        self.state.encode(w);
        self.live.encode(w);
        self.locked.encode(w);
        self.cycle.encode(w);
        self.is_host.encode(w);
        self.is_ready.encode(w);
        put_uleb(w, self.users.len() as u64);
        for user in &self.users {
            user.id.encode(w);
            user.encode(w);
        }
    }
}

impl Decode for ClientRoomState {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            id: r.read()?,
            state: r.read()?,
            live: r.read()?,
            locked: r.read()?,
            cycle: r.read()?,
            is_host: r.read()?,
            is_ready: r.read()?,
            users: {
                let len = r.seq_len(1)?;
                let mut users = Vec::with_capacity(len.min(64));
                for _ in 0..len {
                    let _key: i32 = r.read()?;
                    users.push(r.read()?);
                }
                users
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinRoomResponse {
    pub state: RoomState,
    pub users: Vec<UserInfo>,
    pub live: bool,
}

impl Encode for JoinRoomResponse {
    fn encode(&self, w: &mut BytesMut) {
        self.state.encode(w);
        self.users.encode(w);
        self.live.encode(w);
    }
}

impl Decode for JoinRoomResponse {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            state: r.read()?,
            users: r.read()?,
            live: r.read()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    Chat {
        user: i32,
        content: String,
    },
    CreateRoom {
        user: i32,
    },
    JoinRoom {
        user: i32,
        name: String,
    },
    LeaveRoom {
        user: i32,
        name: String,
    },
    NewHost {
        user: i32,
    },
    SelectChart {
        user: i32,
        name: String,
        id: i32,
    },
    GameStart {
        user: i32,
    },
    Ready {
        user: i32,
    },
    CancelReady {
        user: i32,
    },
    CancelGame {
        user: i32,
    },
    StartPlaying,
    Played {
        user: i32,
        score: i32,
        accuracy: f32,
        full_combo: bool,
    },
    GameEnd,
    Abort {
        user: i32,
    },
    LockRoom {
        lock: bool,
    },
    CycleRoom {
        cycle: bool,
    },
}

impl Encode for Message {
    fn encode(&self, w: &mut BytesMut) {
        match self {
            Self::Chat { user, content } => {
                w.put_u8(0);
                user.encode(w);
                put_str(w, content);
            }
            Self::CreateRoom { user } => {
                w.put_u8(1);
                user.encode(w);
            }
            Self::JoinRoom { user, name } => {
                w.put_u8(2);
                user.encode(w);
                put_str(w, name);
            }
            Self::LeaveRoom { user, name } => {
                w.put_u8(3);
                user.encode(w);
                put_str(w, name);
            }
            Self::NewHost { user } => {
                w.put_u8(4);
                user.encode(w);
            }
            Self::SelectChart { user, name, id } => {
                w.put_u8(5);
                user.encode(w);
                put_str(w, name);
                id.encode(w);
            }
            Self::GameStart { user } => {
                w.put_u8(6);
                user.encode(w);
            }
            Self::Ready { user } => {
                w.put_u8(7);
                user.encode(w);
            }
            Self::CancelReady { user } => {
                w.put_u8(8);
                user.encode(w);
            }
            Self::CancelGame { user } => {
                w.put_u8(9);
                user.encode(w);
            }
            Self::StartPlaying => w.put_u8(10),
            Self::Played {
                user,
                score,
                accuracy,
                full_combo,
            } => {
                w.put_u8(11);
                user.encode(w);
                score.encode(w);
                accuracy.encode(w);
                full_combo.encode(w);
            }
            Self::GameEnd => w.put_u8(12),
            Self::Abort { user } => {
                w.put_u8(13);
                user.encode(w);
            }
            Self::LockRoom { lock } => {
                w.put_u8(14);
                lock.encode(w);
            }
            Self::CycleRoom { cycle } => {
                w.put_u8(15);
                cycle.encode(w);
            }
        }
    }
}

impl Decode for Message {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            0 => Self::Chat {
                user: r.read()?,
                content: r.read()?,
            },
            1 => Self::CreateRoom { user: r.read()? },
            2 => Self::JoinRoom {
                user: r.read()?,
                name: r.read()?,
            },
            3 => Self::LeaveRoom {
                user: r.read()?,
                name: r.read()?,
            },
            4 => Self::NewHost { user: r.read()? },
            5 => Self::SelectChart {
                user: r.read()?,
                name: r.read()?,
                id: r.read()?,
            },
            6 => Self::GameStart { user: r.read()? },
            7 => Self::Ready { user: r.read()? },
            8 => Self::CancelReady { user: r.read()? },
            9 => Self::CancelGame { user: r.read()? },
            10 => Self::StartPlaying,
            11 => Self::Played {
                user: r.read()?,
                score: r.read()?,
                accuracy: r.read()?,
                full_combo: r.read()?,
            },
            12 => Self::GameEnd,
            13 => Self::Abort { user: r.read()? },
            14 => Self::LockRoom { lock: r.read()? },
            15 => Self::CycleRoom { cycle: r.read()? },
            tag => return Err(DecodeError::UnknownTag { ty: "Message", tag }),
        })
    }
}
