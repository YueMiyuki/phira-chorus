use bytes::{Bytes, BytesMut};
use chorus_proto::*;

fn payload<T: Encode>(msg: &T) -> Vec<u8> {
    let mut w = BytesMut::new();
    msg.encode(&mut w);
    w.to_vec()
}

fn decode<T: Decode>(bytes: &[u8]) -> Result<T, DecodeError> {
    let bytes = Bytes::copy_from_slice(bytes);
    Reader::new(&bytes).read()
}

fn user(id: i32, name: &str, monitor: bool) -> UserInfo {
    UserInfo {
        id,
        name: name.to_owned(),
        monitor,
    }
}

fn sample_touches() -> Vec<TouchFrame> {
    vec![TouchFrame {
        time: 1.0,
        points: vec![(3, CompactPos::new(0.5, -1.0))],
    }]
}

const TOUCH_RAW: [u8; 11] = [1, 0, 0, 0x80, 0x3f, 1, 3, 0x00, 0x38, 0x00, 0xbc];

fn sample_judges() -> Vec<JudgeEvent> {
    vec![JudgeEvent {
        time: 2.0,
        line_id: 1,
        note_id: 258,
        judgement: Judgement::HoldGood,
    }]
}

const JUDGE_RAW: [u8; 14] = [1, 0, 0, 0, 0x40, 1, 0, 0, 0, 2, 1, 0, 0, 5];

#[test]
fn client_commands() {
    assert_eq!(payload(&ClientCommand::Ping), [0]);
    assert_eq!(
        payload(&ClientCommand::Authenticate {
            token: "abc".into()
        }),
        [1, 3, b'a', b'b', b'c']
    );
    assert_eq!(
        payload(&ClientCommand::JoinRoom {
            id: "ab".parse().unwrap(),
            monitor: true
        }),
        [6, 2, b'a', b'b', 1]
    );
    assert_eq!(
        payload(&ClientCommand::SelectChart { id: 258 }),
        [10, 2, 1, 0, 0]
    );
    assert_eq!(payload(&ClientCommand::Abort), [15]);

    let mut touches = vec![3];
    touches.extend_from_slice(&TOUCH_RAW);
    assert_eq!(
        payload(&ClientCommand::Touches {
            frames: TouchBatch::new(&sample_touches())
        }),
        touches
    );
}

#[test]
fn server_commands() {
    assert_eq!(&encode_frame(&ServerCommand::Pong)[..], [1, 0]);
    assert_eq!(
        payload(&ServerCommand::Authenticate(Ok((
            user(7, "x", false),
            None
        )))),
        [1, 1, 7, 0, 0, 0, 1, b'x', 0, 0]
    );
    assert_eq!(
        payload(&ServerCommand::Authenticate(Err("no".into()))),
        [1, 0, 2, b'n', b'o']
    );
    assert_eq!(
        payload(&ServerCommand::Message(Message::Played {
            user: 1,
            score: 2,
            accuracy: 0.5,
            full_combo: true
        })),
        [5, 11, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0x3f, 1]
    );
    assert_eq!(
        payload(&ServerCommand::ChangeState(RoomState::SelectChart(Some(5)))),
        [6, 0, 1, 5, 0, 0, 0]
    );
    assert_eq!(
        payload(&ServerCommand::ChangeState(RoomState::Playing)),
        [6, 2]
    );
    assert_eq!(payload(&ServerCommand::Abort(Ok(()))), [19, 1]);

    let mut touches = vec![3, 9, 0, 0, 0];
    touches.extend_from_slice(&TOUCH_RAW);
    assert_eq!(
        payload(&ServerCommand::Touches {
            player: 9,
            frames: TouchBatch::new(&sample_touches())
        }),
        touches
    );
    let mut judges = vec![4, 9, 0, 0, 0];
    judges.extend_from_slice(&JUDGE_RAW);
    assert_eq!(
        payload(&ServerCommand::Judges {
            player: 9,
            judges: JudgeBatch::new(&sample_judges())
        }),
        judges
    );
}

#[test]
fn room_state_snapshot_encodes_users_as_map() {
    let state = ClientRoomState {
        id: "r".parse().unwrap(),
        state: RoomState::Playing,
        live: true,
        locked: false,
        cycle: true,
        is_host: false,
        is_ready: true,
        users: vec![user(3, "a", true)],
    };
    let expected = [
        1, b'r', 2, 1, 0, 1, 0, 1, // header fields
        1, // one entry
        3, 0, 0, 0, // key
        3, 0, 0, 0, 1, b'a', 1, // value
    ];
    assert_eq!(payload(&state), expected);
    assert_eq!(decode::<ClientRoomState>(&expected).unwrap(), state);
}

#[test]
fn frame_prefix_is_uleb128() {
    let frame = encode_frame(&ClientCommand::Chat {
        message: "a".repeat(197),
    });
    assert_eq!(frame.len(), 202);
    assert_eq!(&frame[..5], [0xc8, 0x01, 2, 0xc5, 0x01]);

    let mut buf = BytesMut::from(&frame[..]);
    let payload = split_frame(&mut buf, MAX_FRAME_LEN).unwrap().unwrap();
    assert_eq!(payload.len(), 200);
    assert!(buf.is_empty());
}

#[test]
fn split_frame_handles_partial_input() {
    let frame = encode_frame(&ClientCommand::Chat {
        message: "hello".into(),
    });
    let mut buf = BytesMut::new();
    for (i, byte) in frame.iter().enumerate() {
        assert_eq!(
            split_frame(&mut buf, MAX_FRAME_LEN).unwrap(),
            None,
            "byte {i}"
        );
        buf.extend_from_slice(&[*byte]);
    }
    let payload = split_frame(&mut buf, MAX_FRAME_LEN).unwrap().unwrap();
    assert_eq!(
        ClientCommand::from_payload(&payload).unwrap(),
        Some(ClientCommand::Chat {
            message: "hello".into()
        })
    );

    let mut two = BytesMut::new();
    two.extend_from_slice(&encode_frame(&ClientCommand::Ping));
    two.extend_from_slice(&encode_frame(&ClientCommand::Ready));
    assert_eq!(
        &split_frame(&mut two, MAX_FRAME_LEN).unwrap().unwrap()[..],
        [0]
    );
    assert_eq!(
        &split_frame(&mut two, MAX_FRAME_LEN).unwrap().unwrap()[..],
        [12]
    );
    assert_eq!(split_frame(&mut two, MAX_FRAME_LEN).unwrap(), None);
}

#[test]
fn split_frame_rejects_bad_lengths() {
    let mut big = BytesMut::from(&[0x81, 0x80, 0x80, 0x01][..]); // 2 MiB + 1
    assert_eq!(
        split_frame(&mut big, MAX_FRAME_LEN),
        Err(FrameError::TooLarge(2 * 1024 * 1024 + 1))
    );
    let mut endless = BytesMut::from(&[0x80; 5][..]);
    assert_eq!(
        split_frame(&mut endless, MAX_FRAME_LEN),
        Err(FrameError::BadLength)
    );
}

#[test]
fn decoding_enforces_limits() {
    let mut token = vec![1, 33];
    token.extend_from_slice(&[b'a'; 33]);
    assert_eq!(
        decode::<ClientCommand>(&token),
        Err(DecodeError::StringTooLong { max: 32 })
    );

    assert_eq!(
        decode::<ClientCommand>(&[5, 3, b'a', b' ', b'b']),
        Err(DecodeError::InvalidRoomId)
    );
    assert_eq!(
        decode::<ClientCommand>(&[5, 0]),
        Err(DecodeError::InvalidRoomId)
    );
    let mut long_id = vec![5, 21];
    long_id.extend_from_slice(&[b'a'; 21]);
    assert_eq!(
        decode::<ClientCommand>(&long_id),
        Err(DecodeError::StringTooLong { max: 20 })
    );

    assert_eq!(
        decode::<ClientCommand>(&[3, 0xe8, 0x07, 0, 0, 0, 0, 0]),
        Err(DecodeError::LengthOutOfRange)
    );
    let mut bad_judgement = vec![4];
    bad_judgement.extend_from_slice(&JUDGE_RAW);
    *bad_judgement.last_mut().unwrap() = 6;
    assert_eq!(
        decode::<ClientCommand>(&bad_judgement),
        Err(DecodeError::UnknownTag {
            ty: "Judgement",
            tag: 6
        })
    );

    assert_eq!(
        ClientCommand::from_payload(&Bytes::from_static(&[16, 1, 2])),
        Ok(None)
    );
    assert!(ClientCommand::from_payload(&Bytes::new()).is_err());
    assert_eq!(
        Reader::new(&Bytes::from_static(&[0xff; 9])).uleb(),
        Err(DecodeError::UnexpectedEof)
    );
    let overlong = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
    assert_eq!(
        Reader::new(&Bytes::copy_from_slice(&overlong)).uleb(),
        Err(DecodeError::BadVarint)
    );
}

#[test]
fn decoding_is_lenient_where_clients_are() {
    assert_eq!(decode::<bool>(&[2]), Ok(false));
    assert_eq!(
        decode::<ClientCommand>(&[2, 2, 0xff, b'a']),
        Ok(ClientCommand::Chat {
            message: "\u{fffd}a".into()
        })
    );
    assert_eq!(
        decode::<ClientCommand>(&[12, 99, 99]),
        Ok(ClientCommand::Ready)
    );
}

#[test]
fn batches_relay_only_the_validated_bytes() {
    let mut body = vec![3];
    body.extend_from_slice(&TOUCH_RAW);
    body.extend_from_slice(b"trailing");
    let payload = Bytes::from(body);
    let Some(ClientCommand::Touches { frames }) = ClientCommand::from_payload(&payload).unwrap()
    else {
        panic!("expected touches");
    };
    assert_eq!(&frames.raw()[..], TOUCH_RAW);
    assert_eq!(frames.iter().collect::<Vec<_>>(), sample_touches());
    assert_eq!(
        JudgeBatch::new(&sample_judges()).iter().collect::<Vec<_>>(),
        sample_judges()
    );
}

#[test]
fn server_commands_round_trip() {
    let state = ClientRoomState {
        id: "room-1".parse().unwrap(),
        state: RoomState::SelectChart(None),
        live: false,
        locked: true,
        cycle: false,
        is_host: true,
        is_ready: false,
        users: vec![user(1, "one", false), user(2, "two", true)],
    };
    let commands = vec![
        ServerCommand::Pong,
        ServerCommand::Authenticate(Ok((user(1, "one", false), Some(state)))),
        ServerCommand::Chat(Err("no room".into())),
        ServerCommand::Touches {
            player: 4,
            frames: TouchBatch::new(&sample_touches()),
        },
        ServerCommand::Judges {
            player: 4,
            judges: JudgeBatch::new(&sample_judges()),
        },
        ServerCommand::Message(Message::SelectChart {
            user: 1,
            name: "chart".into(),
            id: 7,
        }),
        ServerCommand::Message(Message::CycleRoom { cycle: true }),
        ServerCommand::ChangeState(RoomState::WaitingForReady),
        ServerCommand::ChangeHost(true),
        ServerCommand::CreateRoom(Ok(())),
        ServerCommand::JoinRoom(Ok(JoinRoomResponse {
            state: RoomState::Playing,
            users: vec![user(5, "five", false)],
            live: true,
        })),
        ServerCommand::OnJoinRoom(user(6, "six", true)),
        ServerCommand::LeaveRoom(Ok(())),
        ServerCommand::LockRoom(Ok(())),
        ServerCommand::CycleRoom(Ok(())),
        ServerCommand::SelectChart(Err("x".into())),
        ServerCommand::RequestStart(Ok(())),
        ServerCommand::Ready(Ok(())),
        ServerCommand::CancelReady(Ok(())),
        ServerCommand::Played(Ok(())),
        ServerCommand::Abort(Ok(())),
    ];
    for cmd in commands {
        let mut buf = BytesMut::from(&encode_frame(&cmd)[..]);
        let payload = split_frame(&mut buf, MAX_FRAME_LEN).unwrap().unwrap();
        assert_eq!(Reader::new(&payload).read::<ServerCommand>().unwrap(), cmd);
    }
}
