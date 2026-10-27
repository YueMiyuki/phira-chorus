use crate::batch::{JudgeBatch, TouchBatch};
use crate::codec::{Decode, DecodeError, Encode, Reader, Result, put_str};
use crate::model::{ClientRoomState, JoinRoomResponse, Message, RoomId, RoomState, UserInfo};
use bytes::{BufMut, Bytes, BytesMut};

pub type Reply<T = ()> = std::result::Result<T, String>;

#[derive(Clone, Debug, PartialEq)]
/// Every variant but `Ping`, `Touches` and `Judges` gets exactly one reply
pub enum ClientCommand {
    Ping,
    Authenticate { token: String },
    Chat { message: String },
    Touches { frames: TouchBatch },
    Judges { judges: JudgeBatch },
    CreateRoom { id: RoomId },
    JoinRoom { id: RoomId, monitor: bool },
    LeaveRoom,
    LockRoom { lock: bool },
    CycleRoom { cycle: bool },
    SelectChart { id: i32 },
    RequestStart,
    Ready,
    CancelReady,
    Played { id: i32 },
    Abort,
}

impl ClientCommand {
    pub const MAX_TOKEN_LEN: usize = 32;
    pub const MAX_CHAT_LEN: usize = 200;

    /// Unknown tags give `Ok(None)`, so callers can skip them
    pub fn from_payload(payload: &Bytes) -> Result<Option<Self>> {
        if payload.first().is_some_and(|&tag| tag > 15) {
            return Ok(None);
        }
        Reader::new(payload).read().map(Some)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Ping => "Ping",
            Self::Authenticate { .. } => "Authenticate",
            Self::Chat { .. } => "Chat",
            Self::Touches { .. } => "Touches",
            Self::Judges { .. } => "Judges",
            Self::CreateRoom { .. } => "CreateRoom",
            Self::JoinRoom { .. } => "JoinRoom",
            Self::LeaveRoom => "LeaveRoom",
            Self::LockRoom { .. } => "LockRoom",
            Self::CycleRoom { .. } => "CycleRoom",
            Self::SelectChart { .. } => "SelectChart",
            Self::RequestStart => "RequestStart",
            Self::Ready => "Ready",
            Self::CancelReady => "CancelReady",
            Self::Played { .. } => "Played",
            Self::Abort => "Abort",
        }
    }
}

impl Encode for ClientCommand {
    fn encode(&self, w: &mut BytesMut) {
        match self {
            Self::Ping => w.put_u8(0),
            Self::Authenticate { token } => {
                w.put_u8(1);
                put_str(w, token);
            }
            Self::Chat { message } => {
                w.put_u8(2);
                put_str(w, message);
            }
            Self::Touches { frames } => {
                w.put_u8(3);
                frames.encode(w);
            }
            Self::Judges { judges } => {
                w.put_u8(4);
                judges.encode(w);
            }
            Self::CreateRoom { id } => {
                w.put_u8(5);
                id.encode(w);
            }
            Self::JoinRoom { id, monitor } => {
                w.put_u8(6);
                id.encode(w);
                monitor.encode(w);
            }
            Self::LeaveRoom => w.put_u8(7),
            Self::LockRoom { lock } => {
                w.put_u8(8);
                lock.encode(w);
            }
            Self::CycleRoom { cycle } => {
                w.put_u8(9);
                cycle.encode(w);
            }
            Self::SelectChart { id } => {
                w.put_u8(10);
                id.encode(w);
            }
            Self::RequestStart => w.put_u8(11),
            Self::Ready => w.put_u8(12),
            Self::CancelReady => w.put_u8(13),
            Self::Played { id } => {
                w.put_u8(14);
                id.encode(w);
            }
            Self::Abort => w.put_u8(15),
        }
    }
}

impl Decode for ClientCommand {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            0 => Self::Ping,
            1 => Self::Authenticate {
                token: r.string(Self::MAX_TOKEN_LEN)?,
            },
            2 => Self::Chat {
                message: r.string(Self::MAX_CHAT_LEN)?,
            },
            3 => Self::Touches { frames: r.read()? },
            4 => Self::Judges { judges: r.read()? },
            5 => Self::CreateRoom { id: r.read()? },
            6 => Self::JoinRoom {
                id: r.read()?,
                monitor: r.read()?,
            },
            7 => Self::LeaveRoom,
            8 => Self::LockRoom { lock: r.read()? },
            9 => Self::CycleRoom { cycle: r.read()? },
            10 => Self::SelectChart { id: r.read()? },
            11 => Self::RequestStart,
            12 => Self::Ready,
            13 => Self::CancelReady,
            14 => Self::Played { id: r.read()? },
            15 => Self::Abort,
            tag => {
                return Err(DecodeError::UnknownTag {
                    ty: "ClientCommand",
                    tag,
                });
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ServerCommand {
    Pong,
    Authenticate(Reply<(UserInfo, Option<ClientRoomState>)>),
    Chat(Reply),
    Touches { player: i32, frames: TouchBatch },
    Judges { player: i32, judges: JudgeBatch },
    Message(Message),
    ChangeState(RoomState),
    ChangeHost(bool),
    CreateRoom(Reply),
    JoinRoom(Reply<JoinRoomResponse>),
    OnJoinRoom(UserInfo),
    LeaveRoom(Reply),
    LockRoom(Reply),
    CycleRoom(Reply),
    SelectChart(Reply),
    RequestStart(Reply),
    Ready(Reply),
    CancelReady(Reply),
    Played(Reply),
    Abort(Reply),
}

impl Encode for ServerCommand {
    fn encode(&self, w: &mut BytesMut) {
        match self {
            Self::Pong => w.put_u8(0),
            Self::Authenticate(reply) => {
                w.put_u8(1);
                reply.encode(w);
            }
            Self::Chat(reply) => {
                w.put_u8(2);
                reply.encode(w);
            }
            Self::Touches { player, frames } => {
                w.put_u8(3);
                player.encode(w);
                frames.encode(w);
            }
            Self::Judges { player, judges } => {
                w.put_u8(4);
                player.encode(w);
                judges.encode(w);
            }
            Self::Message(msg) => {
                w.put_u8(5);
                msg.encode(w);
            }
            Self::ChangeState(state) => {
                w.put_u8(6);
                state.encode(w);
            }
            Self::ChangeHost(is_host) => {
                w.put_u8(7);
                is_host.encode(w);
            }
            Self::CreateRoom(reply) => {
                w.put_u8(8);
                reply.encode(w);
            }
            Self::JoinRoom(reply) => {
                w.put_u8(9);
                reply.encode(w);
            }
            Self::OnJoinRoom(user) => {
                w.put_u8(10);
                user.encode(w);
            }
            Self::LeaveRoom(reply) => {
                w.put_u8(11);
                reply.encode(w);
            }
            Self::LockRoom(reply) => {
                w.put_u8(12);
                reply.encode(w);
            }
            Self::CycleRoom(reply) => {
                w.put_u8(13);
                reply.encode(w);
            }
            Self::SelectChart(reply) => {
                w.put_u8(14);
                reply.encode(w);
            }
            Self::RequestStart(reply) => {
                w.put_u8(15);
                reply.encode(w);
            }
            Self::Ready(reply) => {
                w.put_u8(16);
                reply.encode(w);
            }
            Self::CancelReady(reply) => {
                w.put_u8(17);
                reply.encode(w);
            }
            Self::Played(reply) => {
                w.put_u8(18);
                reply.encode(w);
            }
            Self::Abort(reply) => {
                w.put_u8(19);
                reply.encode(w);
            }
        }
    }
}

impl Decode for ServerCommand {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            0 => Self::Pong,
            1 => Self::Authenticate(r.read()?),
            2 => Self::Chat(r.read()?),
            3 => Self::Touches {
                player: r.read()?,
                frames: r.read()?,
            },
            4 => Self::Judges {
                player: r.read()?,
                judges: r.read()?,
            },
            5 => Self::Message(r.read()?),
            6 => Self::ChangeState(r.read()?),
            7 => Self::ChangeHost(r.read()?),
            8 => Self::CreateRoom(r.read()?),
            9 => Self::JoinRoom(r.read()?),
            10 => Self::OnJoinRoom(r.read()?),
            11 => Self::LeaveRoom(r.read()?),
            12 => Self::LockRoom(r.read()?),
            13 => Self::CycleRoom(r.read()?),
            14 => Self::SelectChart(r.read()?),
            15 => Self::RequestStart(r.read()?),
            16 => Self::Ready(r.read()?),
            17 => Self::CancelReady(r.read()?),
            18 => Self::Played(r.read()?),
            19 => Self::Abort(r.read()?),
            tag => {
                return Err(DecodeError::UnknownTag {
                    ty: "ServerCommand",
                    tag,
                });
            }
        })
    }
}
