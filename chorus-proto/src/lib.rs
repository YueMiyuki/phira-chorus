//! Wire protocol between Phira clients and the multiplayer server
//!
//! The client opens with one byte, its protocol version; after that both sides
//! exchange frames: a ULEB128 length (at most five bytes), then the payload
//!
//! Integers and floats are little-endian; strings and sequences are a ULEB128
//! length and their contents; enums are a one-byte tag and the variant's
//! fields; `bool` is one byte (only `1` is true); `Option` and `Result` are a
//! `bool` and the payload; maps are a length and key/value pairs

mod batch;
mod codec;
mod command;
mod frame;
mod model;

pub use batch::{Items, JudgeBatch, TouchBatch};
pub use codec::{Decode, DecodeError, Encode, Reader, put_str, put_uleb};
pub use command::{ClientCommand, Reply, ServerCommand};
pub use frame::{FrameError, encode_frame, encode_frame_with_capacity, split_frame};
pub use model::{
    ClientRoomState, CompactPos, JoinRoomResponse, JudgeEvent, Judgement, Message, RoomId,
    RoomState, TouchFrame, UserInfo,
};

use std::time::Duration;

pub const PROTOCOL_VERSION: u8 = 1;

pub const MAX_FRAME_LEN: usize = 2 * 1024 * 1024;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

pub const HEARTBEAT_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);
