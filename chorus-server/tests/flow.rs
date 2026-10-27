use chorus_bench::mock_api::MockApi;
use chorus_client::Connection;
use chorus_proto::*;
use chorus_server::{Config, Shared, bind, serve_with};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    time,
};

const MONITOR: i32 = 900;

struct TestServer {
    addr: SocketAddr,
    shared: Arc<Shared>,
    api: MockApi,
    _stop: oneshot::Sender<()>,
}

async fn start(api_latency: Duration, tweak: impl FnOnce(&mut Config)) -> TestServer {
    let api = MockApi::spawn(([127, 0, 0, 1], 0).into(), api_latency)
        .await
        .unwrap();
    let mut config = Config {
        api_url: api.url(),
        monitors: vec![MONITOR],
        reconnect_grace_secs: 1,
        playing_grace_secs: 1,
        stats_interval_secs: 0,
        login_store: String::new(),
        ..Config::default()
    };
    tweak(&mut config);
    let listener = bind(([127, 0, 0, 1], 0).into()).unwrap();
    let addr = listener.local_addr().unwrap();
    let shared = Shared::new(config).unwrap();
    let (stop, stopped) = oneshot::channel();
    tokio::spawn(serve_with(listener, Arc::clone(&shared), async {
        let _ = stopped.await;
    }));
    TestServer {
        addr,
        shared,
        api,
        _stop: stop,
    }
}

async fn server() -> TestServer {
    start(Duration::ZERO, |_| {}).await
}

struct Client {
    conn: Connection,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        Self {
            conn: Connection::connect(addr).await.unwrap(),
        }
    }

    async fn login(addr: SocketAddr, id: i32) -> Self {
        let mut client = Self::connect(addr).await;
        let reply = client.request(auth(id)).await;
        assert!(
            matches!(&reply[..], [ServerCommand::Authenticate(Ok((info, None)))] if info.id == id),
            "{reply:?}"
        );
        client
    }

    async fn send(&mut self, cmd: ClientCommand) {
        self.conn.send(&cmd).await.unwrap();
    }

    async fn next(&mut self) -> ServerCommand {
        loop {
            let cmd = time::timeout(Duration::from_secs(5), self.conn.recv())
                .await
                .expect("timed out waiting for a frame")
                .expect("connection failed");
            if cmd != ServerCommand::Pong {
                return cmd;
            }
        }
    }

    async fn request(&mut self, cmd: ClientCommand) -> Vec<ServerCommand> {
        self.send(cmd).await;
        let mut frames = Vec::new();
        loop {
            let frame = self.next().await;
            let done = is_reply(&frame);
            frames.push(frame);
            if done {
                return frames;
            }
        }
    }

    async fn expect(&mut self, expected: &[ServerCommand]) {
        for (i, want) in expected.iter().enumerate() {
            let got = self.next().await;
            assert_eq!(&got, want, "frame {i}");
        }
    }

    async fn expect_quiet(&mut self) {
        let res = time::timeout(Duration::from_millis(300), self.next()).await;
        assert!(res.is_err(), "unexpected frame {res:?}");
    }

    async fn expect_closed(&mut self) {
        let res = time::timeout(Duration::from_secs(5), async {
            loop {
                match self.conn.recv().await {
                    Ok(_) => continue,
                    Err(err) => return err,
                }
            }
        })
        .await
        .expect("connection stayed open");
        assert_eq!(res.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}

fn is_reply(cmd: &ServerCommand) -> bool {
    use ServerCommand::*;
    matches!(
        cmd,
        Authenticate(_)
            | Chat(_)
            | CreateRoom(_)
            | JoinRoom(_)
            | LeaveRoom(_)
            | LockRoom(_)
            | CycleRoom(_)
            | SelectChart(_)
            | RequestStart(_)
            | Ready(_)
            | CancelReady(_)
            | Played(_)
            | Abort(_)
    )
}

fn auth(id: i32) -> ClientCommand {
    ClientCommand::Authenticate {
        token: format!("u{id}"),
    }
}

fn room(id: &str) -> RoomId {
    id.parse().unwrap()
}

fn info(id: i32, monitor: bool) -> UserInfo {
    UserInfo {
        id,
        name: format!("user{id}"),
        monitor,
    }
}

fn msg(m: Message) -> ServerCommand {
    ServerCommand::Message(m)
}

fn joined(id: i32) -> Message {
    Message::JoinRoom {
        user: id,
        name: format!("user{id}"),
    }
}

fn left(id: i32) -> Message {
    Message::LeaveRoom {
        user: id,
        name: format!("user{id}"),
    }
}

fn played(id: i32) -> Message {
    Message::Played {
        user: id,
        score: 900_000 + id,
        accuracy: 0.995,
        full_combo: id % 2 == 0,
    }
}

async fn create(client: &mut Client, id: i32, name: &str) {
    assert_eq!(
        client
            .request(ClientCommand::CreateRoom { id: room(name) })
            .await,
        [
            msg(Message::CreateRoom { user: id }),
            ServerCommand::CreateRoom(Ok(()))
        ]
    );
}

async fn join(joiner: &mut Client, id: i32, name: &str, monitor: bool, others: &mut [&mut Client]) {
    let reply = joiner
        .request(ClientCommand::JoinRoom {
            id: room(name),
            monitor,
        })
        .await;
    assert_eq!(
        reply[..2],
        [
            ServerCommand::OnJoinRoom(info(id, monitor)),
            msg(joined(id))
        ]
    );
    assert!(
        matches!(reply[2], ServerCommand::JoinRoom(Ok(_))),
        "{reply:?}"
    );
    for other in others {
        other
            .expect(&[
                ServerCommand::OnJoinRoom(info(id, monitor)),
                msg(joined(id)),
            ])
            .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn full_round() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;

    create(&mut a, 1, "r1").await;
    let reply = b
        .request(ClientCommand::JoinRoom {
            id: room("r1"),
            monitor: false,
        })
        .await;
    assert_eq!(
        reply,
        [
            ServerCommand::OnJoinRoom(info(2, false)),
            msg(joined(2)),
            ServerCommand::JoinRoom(Ok(JoinRoomResponse {
                state: RoomState::SelectChart(None),
                users: vec![info(1, false), info(2, false)],
                live: false,
            })),
        ]
    );
    a.expect(&[ServerCommand::OnJoinRoom(info(2, false)), msg(joined(2))])
        .await;

    assert_eq!(
        b.request(ClientCommand::RequestStart).await,
        [ServerCommand::RequestStart(Err(
            "only host can do this".into()
        ))]
    );
    assert_eq!(
        a.request(ClientCommand::RequestStart).await,
        [ServerCommand::RequestStart(Err("No chart selected".into()))]
    );

    let selected = [
        msg(Message::SelectChart {
            user: 1,
            name: "Chart 7".into(),
            id: 7,
        }),
        ServerCommand::ChangeState(RoomState::SelectChart(Some(7))),
    ];
    let mut reply = selected.to_vec();
    reply.push(ServerCommand::SelectChart(Ok(())));
    assert_eq!(a.request(ClientCommand::SelectChart { id: 7 }).await, reply);
    b.expect(&selected).await;

    let waiting = [
        msg(Message::GameStart { user: 1 }),
        ServerCommand::ChangeState(RoomState::WaitingForReady),
    ];
    let mut reply = waiting.to_vec();
    reply.push(ServerCommand::RequestStart(Ok(())));
    assert_eq!(a.request(ClientCommand::RequestStart).await, reply);
    b.expect(&waiting).await;

    let started = [
        msg(Message::Ready { user: 2 }),
        msg(Message::StartPlaying),
        ServerCommand::ChangeState(RoomState::Playing),
    ];
    let mut reply = started.to_vec();
    reply.push(ServerCommand::Ready(Ok(())));
    assert_eq!(b.request(ClientCommand::Ready).await, reply);
    a.expect(&started).await;

    assert_eq!(
        b.request(ClientCommand::Played { id: 1 }).await,
        [ServerCommand::Played(Err("invalid record".into()))]
    );
    assert_eq!(
        a.request(ClientCommand::Played { id: 1 }).await,
        [msg(played(1)), ServerCommand::Played(Ok(()))]
    );
    b.expect(&[msg(played(1))]).await;
    assert_eq!(
        a.request(ClientCommand::Played { id: 1 }).await,
        [ServerCommand::Played(Err("already uploaded".into()))]
    );

    let ended = [
        msg(played(2)),
        msg(Message::GameEnd),
        ServerCommand::ChangeState(RoomState::SelectChart(Some(7))),
    ];
    let mut reply = ended.to_vec();
    reply.push(ServerCommand::Played(Ok(())));
    assert_eq!(b.request(ClientCommand::Played { id: 2 }).await, reply);
    a.expect(&ended).await;

    assert_eq!(
        b.request(ClientCommand::Chat {
            message: "gg".into()
        })
        .await,
        [
            msg(Message::Chat {
                user: 2,
                content: "gg".into()
            }),
            ServerCommand::Chat(Ok(()))
        ]
    );
    a.expect(&[msg(Message::Chat {
        user: 2,
        content: "gg".into(),
    })])
    .await;

    assert_eq!(
        a.request(ClientCommand::LockRoom { lock: true }).await,
        [
            msg(Message::LockRoom { lock: true }),
            ServerCommand::LockRoom(Ok(()))
        ]
    );
    b.expect(&[msg(Message::LockRoom { lock: true })]).await;
    let mut c = Client::login(srv.addr, 3).await;
    assert_eq!(
        c.request(ClientCommand::JoinRoom {
            id: room("r1"),
            monitor: false
        })
        .await,
        [ServerCommand::JoinRoom(Err("Room is locked".into()))]
    );

    assert_eq!(
        b.request(ClientCommand::LeaveRoom).await,
        [msg(left(2)), ServerCommand::LeaveRoom(Ok(()))]
    );
    a.expect(&[msg(left(2))]).await;
    assert_eq!(
        b.request(ClientCommand::LeaveRoom).await,
        [ServerCommand::LeaveRoom(Err("no room".into()))]
    );
    assert_eq!(
        a.request(ClientCommand::LeaveRoom).await,
        [msg(left(1)), ServerCommand::LeaveRoom(Ok(()))]
    );
    assert_eq!(srv.shared.room_count(), 0);
    assert_eq!(
        c.request(ClientCommand::JoinRoom {
            id: room("r1"),
            monitor: false
        })
        .await,
        [ServerCommand::JoinRoom(Err("room not found".into()))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_ready_and_abort() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    let mut c = Client::login(srv.addr, 3).await;
    create(&mut a, 1, "r").await;
    join(&mut b, 2, "r", false, &mut [&mut a]).await;
    join(&mut c, 3, "r", false, &mut [&mut a, &mut b]).await;
    a.request(ClientCommand::SelectChart { id: 1 }).await;
    b.expect(&[
        msg(Message::SelectChart {
            user: 1,
            name: "Chart 1".into(),
            id: 1,
        }),
        ServerCommand::ChangeState(RoomState::SelectChart(Some(1))),
    ])
    .await;
    c.next().await;
    c.next().await;

    a.request(ClientCommand::RequestStart).await;
    for x in [&mut b, &mut c] {
        x.expect(&[
            msg(Message::GameStart { user: 1 }),
            ServerCommand::ChangeState(RoomState::WaitingForReady),
        ])
        .await;
    }
    assert_eq!(
        b.request(ClientCommand::CancelReady).await,
        [ServerCommand::CancelReady(Err("not ready".into()))]
    );
    b.request(ClientCommand::Ready).await;
    assert_eq!(
        b.request(ClientCommand::Ready).await,
        [ServerCommand::Ready(Err("already ready".into()))]
    );
    assert_eq!(
        b.request(ClientCommand::CancelReady).await,
        [
            msg(Message::CancelReady { user: 2 }),
            ServerCommand::CancelReady(Ok(()))
        ]
    );
    a.expect(&[
        msg(Message::Ready { user: 2 }),
        msg(Message::CancelReady { user: 2 }),
    ])
    .await;
    c.expect(&[
        msg(Message::Ready { user: 2 }),
        msg(Message::CancelReady { user: 2 }),
    ])
    .await;
    let cancelled = [
        msg(Message::CancelGame { user: 1 }),
        ServerCommand::ChangeState(RoomState::SelectChart(Some(1))),
    ];
    let mut reply = cancelled.to_vec();
    reply.push(ServerCommand::CancelReady(Ok(())));
    assert_eq!(a.request(ClientCommand::CancelReady).await, reply);
    b.expect(&cancelled).await;
    c.expect(&cancelled).await;

    a.request(ClientCommand::RequestStart).await;
    b.request(ClientCommand::Ready).await;
    c.request(ClientCommand::Ready).await;
    for x in [&mut a, &mut b] {
        loop {
            if x.next().await == ServerCommand::ChangeState(RoomState::Playing) {
                break;
            }
        }
    }
    assert_eq!(
        b.request(ClientCommand::Abort).await,
        [
            msg(Message::Abort { user: 2 }),
            ServerCommand::Abort(Ok(()))
        ]
    );
    assert_eq!(
        b.request(ClientCommand::Abort).await,
        [ServerCommand::Abort(Err("aborted".into()))]
    );
    assert_eq!(
        b.request(ClientCommand::Played { id: 2 }).await,
        [ServerCommand::Played(Err("aborted".into()))]
    );
    c.request(ClientCommand::Played { id: 3 }).await;
    let ended = [
        msg(Message::Abort { user: 1 }),
        msg(Message::GameEnd),
        ServerCommand::ChangeState(RoomState::SelectChart(Some(1))),
    ];
    let mut expected = ended.to_vec();
    expected.push(ServerCommand::Abort(Ok(())));
    let reply = a.request(ClientCommand::Abort).await;
    assert_eq!(reply[reply.len() - 4..], expected[..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn cycle_passes_host_in_join_order() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    create(&mut a, 1, "cyc").await;
    join(&mut b, 2, "cyc", false, &mut [&mut a]).await;
    assert_eq!(
        b.request(ClientCommand::CycleRoom { cycle: true }).await,
        [ServerCommand::CycleRoom(
            Err("only host can do this".into())
        )]
    );
    assert_eq!(
        a.request(ClientCommand::CycleRoom { cycle: true }).await,
        [
            msg(Message::CycleRoom { cycle: true }),
            ServerCommand::CycleRoom(Ok(()))
        ]
    );
    b.next().await;
    a.request(ClientCommand::SelectChart { id: 3 }).await;
    a.request(ClientCommand::RequestStart).await;
    b.request(ClientCommand::Ready).await;
    a.request(ClientCommand::Played { id: 1 }).await;
    let reply = b.request(ClientCommand::Played { id: 2 }).await;
    assert_eq!(
        reply[reply.len() - 6..],
        [
            msg(played(2)),
            msg(Message::GameEnd),
            msg(Message::NewHost { user: 2 }),
            ServerCommand::ChangeHost(true),
            ServerCommand::ChangeState(RoomState::SelectChart(Some(3))),
            ServerCommand::Played(Ok(())),
        ]
    );
    let mut seen = Vec::new();
    loop {
        let frame = a.next().await;
        let done = matches!(frame, ServerCommand::ChangeState(RoomState::SelectChart(_)));
        seen.push(frame);
        if done {
            break;
        }
    }
    assert_eq!(
        seen[seen.len() - 4..],
        [
            msg(Message::GameEnd),
            msg(Message::NewHost { user: 2 }),
            ServerCommand::ChangeHost(false),
            ServerCommand::ChangeState(RoomState::SelectChart(Some(3))),
        ]
    );
    assert_eq!(
        a.request(ClientCommand::SelectChart { id: 4 }).await,
        [ServerCommand::SelectChart(Err(
            "only host can do this".into()
        ))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn host_leaving_hands_over_host() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    create(&mut a, 1, "h").await;
    join(&mut b, 2, "h", false, &mut [&mut a]).await;
    assert_eq!(
        a.request(ClientCommand::LeaveRoom).await,
        [msg(left(1)), ServerCommand::LeaveRoom(Ok(()))]
    );
    b.expect(&[
        msg(left(1)),
        msg(Message::NewHost { user: 2 }),
        ServerCommand::ChangeHost(true),
    ])
    .await;
    assert_eq!(
        b.request(ClientCommand::LockRoom { lock: false }).await,
        [
            msg(Message::LockRoom { lock: false }),
            ServerCommand::LockRoom(Ok(()))
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn monitor_receives_live_data() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut m = Client::login(srv.addr, MONITOR).await;
    let mut x = Client::login(srv.addr, 4).await;
    create(&mut a, 1, "live").await;

    assert_eq!(
        x.request(ClientCommand::JoinRoom {
            id: room("live"),
            monitor: true
        })
        .await,
        [ServerCommand::JoinRoom(Err(
            "Permission denied. You can't monitor this room.".into()
        ))]
    );

    let frames = TouchBatch::new(&[TouchFrame {
        time: 1.5,
        points: vec![(0, CompactPos::new(0.25, 0.75))],
    }]);
    a.send(ClientCommand::Touches {
        frames: frames.clone(),
    })
    .await;
    // Not live yet, so nothing is relayed; the no-op request makes sure the
    // server handled it before the monitor joins
    assert_eq!(
        a.request(ClientCommand::CancelReady).await,
        [ServerCommand::CancelReady(Ok(()))]
    );

    let reply = m
        .request(ClientCommand::JoinRoom {
            id: room("live"),
            monitor: true,
        })
        .await;
    assert_eq!(
        reply.last().unwrap(),
        &ServerCommand::JoinRoom(Ok(JoinRoomResponse {
            state: RoomState::SelectChart(None),
            users: vec![info(1, false), info(MONITOR, true)],
            live: true,
        }))
    );
    a.expect(&[
        ServerCommand::OnJoinRoom(info(MONITOR, true)),
        msg(joined(MONITOR)),
    ])
    .await;

    a.send(ClientCommand::Touches {
        frames: frames.clone(),
    })
    .await;
    let judges = JudgeBatch::new(&[JudgeEvent {
        time: 1.6,
        line_id: 2,
        note_id: 9,
        judgement: Judgement::Good,
    }]);
    a.send(ClientCommand::Judges {
        judges: judges.clone(),
    })
    .await;
    m.expect(&[
        ServerCommand::Touches { player: 1, frames },
        ServerCommand::Judges { player: 1, judges },
    ])
    .await;
    a.expect_quiet().await;

    a.request(ClientCommand::SelectChart { id: 2 }).await;
    a.request(ClientCommand::RequestStart).await;
    m.next().await;
    m.next().await;
    m.expect(&[
        msg(Message::GameStart { user: 1 }),
        ServerCommand::ChangeState(RoomState::WaitingForReady),
    ])
    .await;
    assert_eq!(
        m.request(ClientCommand::Ready).await,
        [
            msg(Message::Ready { user: MONITOR }),
            msg(Message::StartPlaying),
            ServerCommand::ChangeState(RoomState::Playing),
            ServerCommand::Ready(Ok(())),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_within_grace_restores_room() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    create(&mut a, 1, "re").await;
    join(&mut b, 2, "re", false, &mut [&mut a]).await;
    a.request(ClientCommand::SelectChart { id: 5 }).await;
    a.request(ClientCommand::RequestStart).await;
    drop(b);
    time::sleep(Duration::from_millis(200)).await;

    let mut b = Client::connect(srv.addr).await;
    assert_eq!(
        b.request(auth(2)).await,
        [ServerCommand::Authenticate(Ok((
            info(2, false),
            Some(ClientRoomState {
                id: room("re"),
                state: RoomState::WaitingForReady,
                live: false,
                locked: false,
                cycle: false,
                is_host: false,
                is_ready: false,
                users: vec![info(1, false), info(2, false)],
            })
        )))]
    );
    time::sleep(Duration::from_millis(1500)).await;
    a.expect_quiet().await;
    assert_eq!(
        b.request(ClientCommand::Ready).await.last().unwrap(),
        &ServerCommand::Ready(Ok(()))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn seat_released_after_grace() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    create(&mut a, 1, "gr").await;
    join(&mut b, 2, "gr", false, &mut [&mut a]).await;
    drop(b);
    a.expect_quiet().await;
    a.expect(&[msg(left(2))]).await;
    assert_eq!(srv.shared.online_users(), 1);

    let mut b = Client::login(srv.addr, 2).await;
    b.expect_quiet().await;
}

async fn game_in_progress(srv: &TestServer, name: &str) -> (Client, Client) {
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    create(&mut a, 1, name).await;
    join(&mut b, 2, name, false, &mut [&mut a]).await;
    a.request(ClientCommand::SelectChart { id: 5 }).await;
    a.request(ClientCommand::RequestStart).await;
    b.request(ClientCommand::Ready).await;
    for _ in 0..3 {
        a.next().await;
    }
    (a, b)
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_while_playing() {
    let srv = start(Duration::ZERO, |c| c.reconnect_grace_secs = 30).await;
    let (mut a, b) = game_in_progress(&srv, "pl").await;
    drop(b);
    a.expect_quiet().await;
    a.expect(&[msg(left(2))]).await;
    let reply = a.request(ClientCommand::Played { id: 1 }).await;
    assert_eq!(
        reply,
        [
            msg(played(1)),
            msg(Message::GameEnd),
            ServerCommand::ChangeState(RoomState::SelectChart(Some(5))),
            ServerCommand::Played(Ok(())),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_while_playing_without_grace() {
    let srv = start(Duration::ZERO, |c| c.playing_grace_secs = 0).await;
    let (mut a, b) = game_in_progress(&srv, "pl0").await;
    drop(b);
    time::timeout(Duration::from_millis(300), a.expect(&[msg(left(2))]))
        .await
        .expect("player removed at once");
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_during_game_keeps_seat() {
    let srv = start(Duration::ZERO, |c| c.playing_grace_secs = 2).await;
    let (mut a, b) = game_in_progress(&srv, "mid").await;
    drop(b);
    time::sleep(Duration::from_millis(500)).await;

    let mut b = Client::connect(srv.addr).await;
    let reply = b.request(auth(2)).await;
    assert!(
        matches!(&reply[..], [ServerCommand::Authenticate(Ok((_, Some(state))))]
            if state.state == RoomState::Playing && state.users.len() == 2),
        "{reply:?}"
    );
    time::sleep(Duration::from_millis(2500)).await;
    a.expect_quiet().await;
    a.request(ClientCommand::Played { id: 1 }).await;
    let reply = b.request(ClientCommand::Played { id: 2 }).await;
    assert_eq!(
        reply[reply.len() - 4..],
        [
            msg(played(2)),
            msg(Message::GameEnd),
            ServerCommand::ChangeState(RoomState::SelectChart(Some(5))),
            ServerCommand::Played(Ok(())),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn new_login_takes_over_room_events() {
    let srv = server().await;
    let mut old = Client::login(srv.addr, 1).await;
    create(&mut old, 1, "two").await;

    let mut new = Client::connect(srv.addr).await;
    let reply = new.request(auth(1)).await;
    assert!(
        matches!(&reply[..], [ServerCommand::Authenticate(Ok((_, Some(state))))] if state.is_host && state.id == room("two")),
        "{reply:?}"
    );

    let mut b = Client::login(srv.addr, 2).await;
    join(&mut b, 2, "two", false, &mut [&mut new]).await;
    old.expect_quiet().await;

    old.send(ClientCommand::Ping).await;
    assert_eq!(
        time::timeout(Duration::from_secs(2), old.conn.recv())
            .await
            .unwrap()
            .unwrap(),
        ServerCommand::Pong
    );
    assert_eq!(
        old.request(ClientCommand::Chat {
            message: "hi".into()
        })
        .await,
        [ServerCommand::Chat(Err(
            "Your account is now connected from another session".into()
        ))]
    );
    drop(old);
    time::sleep(Duration::from_millis(1500)).await;
    b.expect_quiet().await;
    assert_eq!(
        new.request(ClientCommand::LockRoom { lock: true }).await,
        [
            msg(Message::LockRoom { lock: true }),
            ServerCommand::LockRoom(Ok(()))
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn login_rules() {
    let srv = server().await;
    let mut bad = Client::connect(srv.addr).await;
    bad.send(ClientCommand::CreateRoom { id: room("x") }).await;
    bad.expect_quiet().await;
    assert_eq!(
        bad.request(ClientCommand::Authenticate {
            token: "nope".into()
        })
        .await,
        [ServerCommand::Authenticate(Err(
            "failed to fetch info".into()
        ))]
    );
    bad.expect_closed().await;

    let mut a = Client::login(srv.addr, 1).await;
    assert_eq!(
        a.request(auth(1)).await,
        [ServerCommand::Authenticate(Err(
            "repeated authenticate".into()
        ))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_are_localized() {
    let srv = server().await;
    let mut a = Client::login(srv.addr, 1).await;
    create(&mut a, 1, "taken").await;
    assert_eq!(
        a.request(ClientCommand::CreateRoom { id: room("other") })
            .await,
        [ServerCommand::CreateRoom(Err("already in room".into()))]
    );

    let mut zh = Client::connect(srv.addr).await;
    zh.request(ClientCommand::Authenticate {
        token: "u5-zh-CN".into(),
    })
    .await;
    assert_eq!(
        zh.request(ClientCommand::CreateRoom { id: room("taken") })
            .await,
        [ServerCommand::CreateRoom(Err("房间 ID 已被占用".into()))]
    );
    let mut ja = Client::connect(srv.addr).await;
    ja.request(ClientCommand::Authenticate {
        token: "u6-ja-JP".into(),
    })
    .await;
    assert_eq!(
        ja.request(ClientCommand::CreateRoom { id: room("taken") })
            .await,
        [ServerCommand::CreateRoom(Err("Room ID is occupied".into()))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn room_capacity() {
    let srv = start(Duration::ZERO, |c| c.room_max_users = 2).await;
    let mut a = Client::login(srv.addr, 1).await;
    let mut b = Client::login(srv.addr, 2).await;
    let mut c = Client::login(srv.addr, 3).await;
    let mut m = Client::login(srv.addr, MONITOR).await;
    create(&mut a, 1, "cap").await;
    join(&mut b, 2, "cap", false, &mut [&mut a]).await;
    assert_eq!(
        c.request(ClientCommand::JoinRoom {
            id: room("cap"),
            monitor: false
        })
        .await,
        [ServerCommand::JoinRoom(Err("Room is full".into()))]
    );
    join(&mut m, MONITOR, "cap", true, &mut [&mut a, &mut b]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pings_answered_while_api_is_slow() {
    let srv = start(Duration::from_millis(600), |_| {}).await;
    let mut a = Client::connect(srv.addr).await;
    a.send(auth(1)).await;
    a.send(ClientCommand::Ping).await;
    let first = time::timeout(Duration::from_millis(300), a.conn.recv())
        .await
        .expect("pong held back by the login")
        .unwrap();
    assert_eq!(first, ServerCommand::Pong);
    assert!(matches!(a.next().await, ServerCommand::Authenticate(Ok(_))));

    a.send(ClientCommand::CreateRoom { id: room("slow") }).await;
    a.send(ClientCommand::SelectChart { id: 8 }).await;
    a.send(ClientCommand::RequestStart).await;
    a.send(ClientCommand::Ping).await;
    let mut before_pong = Vec::new();
    time::timeout(Duration::from_millis(300), async {
        loop {
            match a.conn.recv().await.unwrap() {
                ServerCommand::Pong => return,
                other => before_pong.push(other),
            }
        }
    })
    .await
    .expect("pong held back by the chart lookup");
    assert_eq!(
        before_pong,
        [
            msg(Message::CreateRoom { user: 1 }),
            ServerCommand::CreateRoom(Ok(()))
        ]
    );
    let mut rest = Vec::new();
    while rest.len() < 8 {
        rest.push(a.next().await);
    }
    assert_eq!(
        rest,
        [
            msg(Message::SelectChart {
                user: 1,
                name: "Chart 8".into(),
                id: 8
            }),
            ServerCommand::ChangeState(RoomState::SelectChart(Some(8))),
            ServerCommand::SelectChart(Ok(())),
            msg(Message::GameStart { user: 1 }),
            ServerCommand::ChangeState(RoomState::WaitingForReady),
            // Alone in the room, the host is everyone, so the game starts
            msg(Message::StartPlaying),
            ServerCommand::ChangeState(RoomState::Playing),
            ServerCommand::RequestStart(Ok(())),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_limits() {
    let srv = start(Duration::ZERO, |c| c.max_connections = 1).await;
    let _first = Client::login(srv.addr, 1).await;
    let mut second = Client::connect(srv.addr).await;
    assert_eq!(
        second.request(auth(2)).await,
        [ServerCommand::Authenticate(Err("server is full".into()))]
    );
    second.expect_closed().await;
}

async fn raw(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&[PROTOCOL_VERSION]).await.unwrap();
    stream
}

async fn read_some(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = [0u8; 64];
    let n = time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("no response")
        .unwrap();
    buf[..n].to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_input_is_handled() {
    let srv = server().await;

    let mut stream = raw(srv.addr).await;
    stream.write_all(&[2, 40, 0, 1, 0]).await.unwrap();
    assert_eq!(read_some(&mut stream).await, [1, 0]);

    let mut stream = raw(srv.addr).await;
    stream.write_all(&[3, 5, 1, b' ']).await.unwrap();
    assert!(read_some(&mut stream).await.is_empty());

    let mut stream = raw(srv.addr).await;
    stream.write_all(&[0xff, 0xff, 0xff, 0x0f]).await.unwrap();
    assert!(read_some(&mut stream).await.is_empty());

    let mut stream = TcpStream::connect(srv.addr).await.unwrap();
    stream.write_all(&[1, 1, 0, 1, 0]).await.unwrap();
    let mut pongs = Vec::new();
    while pongs.len() < 4 {
        pongs.extend(read_some(&mut stream).await);
    }
    assert_eq!(pongs, [1, 0, 1, 0]);
}

fn me_calls(srv: &TestServer) -> u64 {
    srv.api
        .counters
        .me
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn stat(counter: &std::sync::atomic::AtomicU64) -> u64 {
    counter.load(std::sync::atomic::Ordering::Relaxed)
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_login_waits_for_the_api() {
    let srv = start(Duration::from_millis(1500), |c| c.api_wait_secs = 1).await;
    let mut a = Client::connect(srv.addr).await;
    let mut b = Client::connect(srv.addr).await;
    a.send(auth(1)).await;
    b.send(auth(1)).await;
    for client in [&mut a, &mut b] {
        assert!(matches!(
            client.next().await,
            ServerCommand::Authenticate(Ok((info, None))) if info.id == 1
        ));
    }
    assert_eq!(me_calls(&srv), 1);

    let mut c = Client::connect(srv.addr).await;
    c.send(auth(2)).await;
    time::sleep(Duration::from_millis(300)).await;
    drop(c);
    time::sleep(Duration::from_millis(1500)).await;
    let started = time::Instant::now();
    let _c = Client::login(srv.addr, 2).await;
    assert!(started.elapsed() < Duration::from_millis(300));
    assert_eq!(me_calls(&srv), 2);
    assert_eq!(stat(&srv.shared.stats.abandoned_logins), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn login_retries_api_failures() {
    let srv = server().await;
    srv.api.fail_next(2, 503);
    let _a = Client::login(srv.addr, 1).await;
    assert_eq!(me_calls(&srv), 3);
    srv.api.fail_next(2, 401);
    let _b = Client::login(srv.addr, 2).await;
    assert_eq!(me_calls(&srv), 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn login_keeps_trying_past_the_retry_budget() {
    let srv = start(Duration::ZERO, |c| c.api_retry_secs = 1).await;
    srv.api.fail_next(4, 503);
    let _a = Client::login(srv.addr, 1).await;
    assert_eq!(me_calls(&srv), 5);
}

fn recheck_always(c: &mut Config) {
    c.login_cache_secs = 0;
    c.login_remember_days = 1;
}

#[tokio::test(flavor = "multi_thread")]
async fn remembered_login_gets_in_while_api_is_down() {
    let srv = start(Duration::ZERO, |c| {
        recheck_always(c);
        c.login_timeout_secs = 1;
    })
    .await;
    drop(Client::login(srv.addr, 1).await);
    srv.api.set_latency(Duration::from_secs(30));
    let started = time::Instant::now();
    let mut a = Client::login(srv.addr, 1).await;
    assert!(started.elapsed() < Duration::from_millis(300));
    create(&mut a, 1, "down").await;
    let mut b = Client::connect(srv.addr).await;
    b.send(auth(2)).await;
    time::sleep(Duration::from_secs(2)).await;
    b.send(ClientCommand::Ping).await;
    let pong = time::timeout(Duration::from_secs(1), b.conn.recv())
        .await
        .expect("connection still served")
        .unwrap();
    assert_eq!(pong, ServerCommand::Pong);
    b.expect_quiet().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn revoked_token_is_closed_on_recheck() {
    let srv = start(Duration::ZERO, recheck_always).await;
    drop(Client::login(srv.addr, 1).await);
    srv.api.revoke(1);
    let mut a = Client::login(srv.addr, 1).await;
    a.expect_closed().await;
    assert_eq!(stat(&srv.shared.stats.revoked_logins), 1);
    let mut again = Client::connect(srv.addr).await;
    assert_eq!(
        again.request(auth(1)).await,
        [ServerCommand::Authenticate(Err(
            "failed to fetch info".into()
        ))]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn recheck_failures_do_not_close() {
    let srv = start(Duration::ZERO, recheck_always).await;
    drop(Client::login(srv.addr, 1).await);
    srv.api.fail_next(1000, 500);
    let mut a = Client::login(srv.addr, 1).await;
    time::sleep(Duration::from_secs(3)).await;
    // One try, then backing off
    assert_eq!(me_calls(&srv), 2);
    create(&mut a, 1, "flaky").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn remembered_logins_survive_restart() {
    let path = std::env::temp_dir().join(format!("chorus-logins-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let store = path.to_str().unwrap().to_owned();
    {
        let store = store.clone();
        let srv = start(Duration::ZERO, move |c| c.login_store = store).await;
        drop(Client::login(srv.addr, 7).await);
        srv.shared.save_logins().await;
    }
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        text.contains("\"user7\"") && !text.contains("\"u7\""),
        "{text}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    let srv = start(Duration::from_secs(30), move |c| c.login_store = store).await;
    let started = time::Instant::now();
    let _a = Client::login(srv.addr, 7).await;
    assert!(started.elapsed() < Duration::from_millis(300));
    std::fs::remove_file(&path).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn late_result_retries_while_the_game_awaits_it() {
    let srv = start(Duration::ZERO, |c| {
        c.api_wait_secs = 1;
        c.api_retry_secs = 1;
    })
    .await;
    let (mut a, mut b) = game_in_progress(&srv, "flaky").await;
    srv.api.fail_next(4, 503);
    assert_eq!(
        a.request(ClientCommand::Played { id: 1 }).await,
        [ServerCommand::Played(Err(
            "failed to fetch record: HTTP 503".into()
        ))]
    );
    a.expect(&[msg(played(1))]).await;
    b.expect(&[msg(played(1))]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn late_result_still_counts() {
    let srv = start(Duration::ZERO, |c| c.api_wait_secs = 1).await;
    let (mut a, mut b) = game_in_progress(&srv, "late").await;
    srv.api.set_latency(Duration::from_millis(1500));
    assert_eq!(
        a.request(ClientCommand::Played { id: 1 }).await,
        [ServerCommand::Played(Err(
            "failed to fetch record: request timed out".into()
        ))]
    );
    a.expect(&[msg(played(1))]).await;
    b.expect(&[msg(played(1))]).await;

    assert_eq!(
        b.request(ClientCommand::Played { id: 2 }).await,
        [ServerCommand::Played(Err(
            "failed to fetch record: request timed out".into()
        ))]
    );
    let end = [
        msg(Message::Abort { user: 2 }),
        msg(Message::GameEnd),
        ServerCommand::ChangeState(RoomState::SelectChart(Some(5))),
    ];
    let mut reply = b.request(ClientCommand::Abort).await;
    assert_eq!(reply.pop(), Some(ServerCommand::Abort(Ok(()))));
    assert_eq!(reply, end);
    a.expect(&end).await;
    time::sleep(Duration::from_secs(1)).await;
    a.expect_quiet().await;
    b.expect_quiet().await;
}
