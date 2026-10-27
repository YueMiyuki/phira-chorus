use anyhow::Result;
use bytes::Bytes;
use chorus_client::{Connection, FrameReader, FrameWriter};
use chorus_proto::{
    ClientCommand, ClientRoomState, CompactPos, JudgeBatch, JudgeEvent, Judgement, Message, RoomId,
    RoomState, ServerCommand, TouchBatch, TouchFrame, encode_frame,
};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{self, Instant, MissedTickBehavior},
};

#[derive(Clone, Debug)]
pub struct LoadConfig {
    pub server: SocketAddr,
    pub players: usize,
    pub room_size: usize,
    pub monitors: bool,
    pub id_base: i32,
    pub monitor_id_base: i32,
    pub ramp: Duration,
    pub duration: Duration,
    pub song: Duration,
    pub pause: Duration,
    pub touch_rate: f64,
    pub judge_rate: f64,
    pub reconnect: bool,
    pub progress: Duration,
}

const REPLY_TIMEOUT: Duration = Duration::from_secs(7);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

impl LoadConfig {
    pub fn rooms(&self) -> usize {
        self.players.div_ceil(self.room_size)
    }

    pub fn monitor_ids(&self) -> Vec<i32> {
        (0..self.rooms() as i32)
            .map(|g| self.monitor_id_base + g)
            .collect()
    }
}

#[derive(Default)]
struct Metrics {
    connected: AtomicU64,
    logged_in: AtomicU64,
    joined: AtomicU64,
    online: AtomicI64,
    seated: AtomicI64,
    reconnects: AtomicU64,
    connect_errors: AtomicU64,
    login_errors: AtomicU64,
    join_errors: AtomicU64,
    error_replies: AtomicU64,
    disconnects: AtomicU64,
    stalled: AtomicU64,
    rounds: AtomicU64,
    games_started: AtomicU64,
    games_ended: AtomicU64,
    frames_in: AtomicU64,
    live_frames_out: AtomicU64,
    live_frames_in: AtomicU64,
    ping_us: Mutex<Vec<u32>>,
    login_us: Mutex<Vec<u32>>,
    rpc_us: Mutex<Vec<u32>>,
    error_kinds: Mutex<HashMap<String, u64>>,
}

impl Metrics {
    fn error(&self, counter: &AtomicU64, msg: String) {
        counter.fetch_add(1, Relaxed);
        let kind: String = msg
            .chars()
            .map(|c| if c.is_ascii_digit() { '#' } else { c })
            .collect();
        *self.error_kinds.lock().unwrap().entry(kind).or_default() += 1;
    }
}

pub struct Report {
    config: LoadConfig,
    elapsed: Duration,
    metrics: Metrics,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Host,
    Player,
    Monitor,
}

struct Gauge<'a>(&'a AtomicI64);

impl<'a> Gauge<'a> {
    fn up(gauge: &'a AtomicI64) -> Self {
        gauge.fetch_add(1, Relaxed);
        Self(gauge)
    }
}

impl Drop for Gauge<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

struct Timer {
    sleep: Pin<Box<time::Sleep>>,
    armed: bool,
}

impl Timer {
    fn new() -> Self {
        Self {
            sleep: Box::pin(time::sleep(Duration::from_secs(86_400))),
            armed: false,
        }
    }

    fn at(&mut self, when: Instant) {
        self.sleep.as_mut().reset(when);
        self.armed = true;
    }

    async fn fired(&mut self) {
        self.sleep.as_mut().await;
        self.armed = false;
    }
}

enum Exit {
    Done,
    Retry,
}

pub async fn run(config: LoadConfig) -> Report {
    let config = Arc::new(config);
    let metrics = Arc::new(Metrics::default());
    let started = Instant::now();
    let total = config.players + if config.monitors { config.rooms() } else { 0 };
    let step = config.ramp.div_f64(total.max(1) as f64);
    let deadline = started + config.ramp + config.duration;

    let progress = (!config.progress.is_zero()).then(|| {
        let (config, metrics) = (Arc::clone(&config), Arc::clone(&metrics));
        tokio::spawn(async move {
            let mut ticker = time::interval(config.progress);
            loop {
                ticker.tick().await;
                let get = |it: &AtomicU64| it.load(Relaxed);
                eprintln!(
                    "[{:>5.1}s] online {}  seated {}  logins {}  login errors {}  reconnects {}  rounds {}",
                    started.elapsed().as_secs_f64(),
                    metrics.online.load(Relaxed),
                    metrics.seated.load(Relaxed),
                    get(&metrics.logged_in),
                    get(&metrics.login_errors),
                    get(&metrics.reconnects),
                    get(&metrics.rounds),
                );
            }
        })
    });

    let mut tasks = Vec::with_capacity(total);
    let mut index = 0;
    for room in 0..config.rooms() {
        let (created_tx, created_rx) = watch::channel(false);
        let created_tx = Arc::new(created_tx);
        let first = room * config.room_size;
        let last = (first + config.room_size).min(config.players);
        let mut members = Vec::new();
        for slot in first..last {
            let role = if slot == first {
                Role::Host
            } else {
                Role::Player
            };
            members.push((role, config.id_base + slot as i32));
        }
        if config.monitors {
            members.push((Role::Monitor, config.monitor_id_base + room as i32));
        }
        let expected = members.len();
        for (role, id) in members {
            let start_at = started + step * index as u32;
            index += 1;
            let client = Client {
                config: Arc::clone(&config),
                metrics: Arc::clone(&metrics),
                creator: role == Role::Host,
                role,
                id,
                room: format!("lt{}-{room}", config.id_base)
                    .parse()
                    .expect("valid room id"),
                expected,
                created_tx: Arc::clone(&created_tx),
                created_rx: created_rx.clone(),
            };
            tasks.push(tokio::spawn(client.run(start_at, deadline)));
        }
    }
    for task in tasks {
        let _ = task.await;
    }
    if let Some(progress) = progress {
        progress.abort();
        let _ = progress.await;
    }
    Report {
        config: (*config).clone(),
        elapsed: started.elapsed(),
        metrics: Arc::into_inner(metrics).expect("all clients finished"),
    }
}

struct Client {
    config: Arc<LoadConfig>,
    metrics: Arc<Metrics>,
    creator: bool,
    role: Role,
    id: i32,
    room: RoomId,
    expected: usize,
    created_tx: Arc<watch::Sender<bool>>,
    created_rx: watch::Receiver<bool>,
}

#[derive(Default)]
struct InFlight {
    pings: VecDeque<Instant>,
    requests: VecDeque<Instant>,
}

struct Seat {
    live: bool,
    members: usize,
    state: RoomState,
    ready: bool,
}

impl Seat {
    fn from_state(state: &ClientRoomState) -> Self {
        Self {
            live: state.live,
            members: state.users.len(),
            state: state.state,
            ready: state.is_ready,
        }
    }
}

impl Client {
    async fn run(mut self, start_at: Instant, deadline: Instant) {
        time::sleep_until(start_at).await;
        let m = Arc::clone(&self.metrics);
        let id = self.id;
        let mut first = true;
        loop {
            let session = self.play(deadline, first);
            let retry = match time::timeout_at(deadline + Duration::from_secs(5), session).await {
                Ok(Ok(Exit::Done)) => false,
                Ok(Ok(Exit::Retry)) => true,
                Ok(Err(err)) => {
                    m.error(&m.disconnects, format!("user {id}: {err}"));
                    true
                }
                Err(_) => {
                    m.error(
                        &m.stalled,
                        format!("user {id}: stalled until the end of the run"),
                    );
                    false
                }
            };
            first = false;
            let backoff = Duration::from_millis(1000 + fastrand::u64(..2000));
            if !retry || !self.config.reconnect || Instant::now() + backoff >= deadline {
                return;
            }
            m.reconnects.fetch_add(1, Relaxed);
            time::sleep(backoff).await;
        }
    }

    async fn play(&mut self, deadline: Instant, first: bool) -> Result<Exit> {
        let m = Arc::clone(&self.metrics);
        let conn =
            match time::timeout(CONNECT_TIMEOUT, Connection::connect(self.config.server)).await {
                Ok(Ok(conn)) => conn,
                Ok(Err(err)) => {
                    m.error(&m.connect_errors, format!("connect: {err}"));
                    return Ok(Exit::Retry);
                }
                Err(_) => {
                    m.error(&m.connect_errors, "connect: timed out".to_owned());
                    return Ok(Exit::Retry);
                }
            };
        m.connected.fetch_add(1, Relaxed);
        let (mut rd, mut wr) = conn.into_split();
        let mut inflight = InFlight::default();

        let sent = Instant::now();
        wr.send(&ClientCommand::Authenticate {
            token: format!("u{}", self.id),
        })
        .await?;
        let login = time::timeout(REPLY_TIMEOUT, async {
            loop {
                if let ServerCommand::Authenticate(reply) = rd.recv().await? {
                    return std::io::Result::Ok(reply);
                }
            }
        });
        let restored = match login.await {
            Ok(Ok(Ok((_, room)))) => room,
            Ok(Ok(Err(err))) => {
                m.error(&m.login_errors, format!("login: {err}"));
                return Ok(Exit::Retry);
            }
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => {
                m.error(&m.login_errors, "login: timed out".to_owned());
                return Ok(Exit::Retry);
            }
        };
        push_sample(&m.login_us, sent);
        m.logged_in.fetch_add(1, Relaxed);
        let _online = Gauge::up(&m.online);

        let seat = match restored {
            Some(state) => {
                self.role = if state.is_host {
                    Role::Host
                } else if self.role == Role::Monitor {
                    Role::Monitor
                } else {
                    Role::Player
                };
                Seat::from_state(&state)
            }
            None => match self.enter_room(&mut rd, &mut wr, first).await? {
                Some(seat) => seat,
                None => return Ok(Exit::Done),
            },
        };
        m.joined.fetch_add(1, Relaxed);
        let _seated = Gauge::up(&m.seated);
        let mut live = seat.live;
        let mut members = seat.members;

        let touches = touch_frame();
        let judges = judge_frame();
        let mut ping = time::interval(chorus_proto::HEARTBEAT_INTERVAL);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut touch = interval_from_rate(self.config.touch_rate);
        let mut judge = interval_from_rate(self.config.judge_rate);
        let mut song = Timer::new();
        let mut in_game = false;
        let wait_for_members = if first {
            self.config.ramp + Duration::from_secs(10)
        } else {
            Duration::from_secs(15)
        };
        let mut round = Timer::new();
        if self.role == Role::Host {
            round.at(Instant::now() + wait_for_members);
        }
        match seat.state {
            RoomState::Playing if self.role != Role::Monitor => {
                in_game = true;
                self.request(&mut wr, &mut inflight, &ClientCommand::Abort)
                    .await?;
            }
            RoomState::Playing => in_game = true,
            RoomState::WaitingForReady if self.role != Role::Host && !seat.ready => {
                self.request(&mut wr, &mut inflight, &ClientCommand::Ready)
                    .await?;
            }
            _ => {}
        }
        let end = time::sleep_until(deadline);
        tokio::pin!(end);

        loop {
            tokio::select! {
                _ = &mut end => break,
                cmd = rd.recv() => {
                    let cmd = match cmd {
                        Ok(cmd) => cmd,
                        Err(err) => {
                            m.error(&m.disconnects, format!("user {}: {err}", self.id));
                            return Ok(Exit::Retry);
                        }
                    };
                    m.frames_in.fetch_add(1, Relaxed);
                    match cmd {
                        ServerCommand::Pong => {
                            if let Some(at) = inflight.pings.pop_front() {
                                push_sample(&m.ping_us, at);
                            }
                        }
                        ServerCommand::Touches { .. } | ServerCommand::Judges { .. } => {
                            m.live_frames_in.fetch_add(1, Relaxed);
                        }
                        ServerCommand::OnJoinRoom(user) => {
                            members += 1;
                            live |= user.monitor;
                            if self.role == Role::Host && members >= self.expected && !in_game {
                                round.at(Instant::now());
                            }
                        }
                        ServerCommand::Message(Message::LeaveRoom { .. }) => members -= 1,
                        ServerCommand::Message(Message::GameEnd) => {
                            m.games_ended.fetch_add(1, Relaxed);
                        }
                        ServerCommand::ChangeState(RoomState::WaitingForReady) => {
                            if self.role != Role::Host {
                                self.request(&mut wr, &mut inflight, &ClientCommand::Ready).await?;
                            }
                        }
                        ServerCommand::ChangeState(RoomState::Playing) => {
                            in_game = true;
                            if self.role != Role::Monitor {
                                m.games_started.fetch_add(1, Relaxed);
                                song.at(Instant::now() + self.config.song);
                            }
                        }
                        ServerCommand::ChangeState(RoomState::SelectChart(_)) => {
                            if in_game {
                                in_game = false;
                                if self.role == Role::Host {
                                    m.rounds.fetch_add(1, Relaxed);
                                    round.at(Instant::now() + self.config.pause);
                                }
                            }
                        }
                        ServerCommand::ChangeHost(is_host) => {
                            self.role = match (is_host, self.role) {
                                (true, _) => Role::Host,
                                (false, Role::Host) => Role::Player,
                                (false, role) => role,
                            };
                            if is_host && !in_game && !round.armed {
                                round.at(Instant::now() + self.config.pause);
                            }
                        }
                        reply => {
                            if let Some(err) = reply_error(&reply) {
                                m.error(&m.error_replies, format!("user {}: {err}", self.id));
                            }
                            if let Some(at) = inflight.requests.pop_front() {
                                push_sample(&m.rpc_us, at);
                            }
                            if matches!(reply, ServerCommand::SelectChart(Ok(()))) {
                                self.request(&mut wr, &mut inflight, &ClientCommand::RequestStart)
                                    .await?;
                            }
                        }
                    }
                }
                _ = ping.tick() => {
                    inflight.pings.push_back(Instant::now());
                    wr.send(&ClientCommand::Ping).await?;
                }
                _ = touch.tick(), if live && song.armed => {
                    m.live_frames_out.fetch_add(1, Relaxed);
                    wr.send_raw(&touches).await?;
                }
                _ = judge.tick(), if live && song.armed => {
                    m.live_frames_out.fetch_add(1, Relaxed);
                    wr.send_raw(&judges).await?;
                }
                _ = song.fired(), if song.armed => {
                    self.request(&mut wr, &mut inflight, &ClientCommand::Played { id: self.id }).await?;
                }
                _ = round.fired(), if round.armed => {
                    if self.role == Role::Host && !in_game {
                        let chart = 1 + fastrand::i32(0..50);
                        self.request(&mut wr, &mut inflight, &ClientCommand::SelectChart { id: chart }).await?;
                    }
                }
            }
        }
        let _ = wr.shutdown().await;
        Ok(Exit::Done)
    }

    async fn enter_room(
        &mut self,
        rd: &mut FrameReader,
        wr: &mut FrameWriter,
        first: bool,
    ) -> Result<Option<Seat>> {
        let m = Arc::clone(&self.metrics);
        if self.creator {
            wr.send(&ClientCommand::CreateRoom {
                id: self.room.clone(),
            })
            .await?;
            let reply = loop {
                if let ServerCommand::CreateRoom(reply) = rd.recv().await? {
                    break reply;
                }
            };
            match reply {
                Ok(()) => {
                    self.role = Role::Host;
                    let _ = self.created_tx.send(true);
                    return Ok(Some(Seat {
                        live: false,
                        members: 1,
                        state: RoomState::SelectChart(None),
                        ready: false,
                    }));
                }
                Err(_) if !first => {}
                Err(err) => {
                    m.error(&m.join_errors, format!("create: {err}"));
                    return Ok(None);
                }
            }
        } else if first {
            let _ = self.created_rx.wait_for(|created| *created).await;
        }
        let monitor = self.role == Role::Monitor;
        let mut attempts = 0;
        loop {
            wr.send(&ClientCommand::JoinRoom {
                id: self.room.clone(),
                monitor,
            })
            .await?;
            let reply = loop {
                if let ServerCommand::JoinRoom(reply) = rd.recv().await? {
                    break reply;
                }
            };
            match reply {
                Ok(resp) => {
                    if !monitor {
                        self.role = Role::Player;
                    }
                    return Ok(Some(Seat {
                        live: resp.live,
                        members: resp.users.len(),
                        state: resp.state,
                        ready: false,
                    }));
                }
                Err(err) => {
                    attempts += 1;
                    if attempts >= 60 {
                        m.error(&m.join_errors, format!("join: {err}"));
                        return Ok(None);
                    }
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn request(
        &self,
        wr: &mut FrameWriter,
        inflight: &mut InFlight,
        cmd: &ClientCommand,
    ) -> std::io::Result<()> {
        inflight.requests.push_back(Instant::now());
        wr.send(cmd).await
    }
}

fn reply_error(cmd: &ServerCommand) -> Option<&str> {
    let err = match cmd {
        ServerCommand::Chat(Err(err))
        | ServerCommand::CreateRoom(Err(err))
        | ServerCommand::LeaveRoom(Err(err))
        | ServerCommand::LockRoom(Err(err))
        | ServerCommand::CycleRoom(Err(err))
        | ServerCommand::SelectChart(Err(err))
        | ServerCommand::RequestStart(Err(err))
        | ServerCommand::Ready(Err(err))
        | ServerCommand::CancelReady(Err(err))
        | ServerCommand::Played(Err(err))
        | ServerCommand::Abort(Err(err)) => err,
        ServerCommand::JoinRoom(Err(err)) => err,
        _ => return None,
    };
    Some(err)
}

fn interval_from_rate(rate: f64) -> time::Interval {
    let period = if rate > 0. {
        Duration::from_secs_f64(1. / rate)
    } else {
        Duration::from_secs(86_400)
    };
    let mut interval = time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}

fn push_sample(samples: &Mutex<Vec<u32>>, since: Instant) {
    let us = since.elapsed().as_micros().min(u32::MAX as u128) as u32;
    samples.lock().unwrap().push(us);
}

fn touch_frame() -> Bytes {
    let frames: Vec<_> = (0..4)
        .map(|i| TouchFrame {
            time: i as f32 * 0.05,
            points: vec![
                (0, CompactPos::new(0.1 * i as f32, 0.5)),
                (1, CompactPos::new(-0.3, 0.2 * i as f32)),
            ],
        })
        .collect();
    encode_frame(&ClientCommand::Touches {
        frames: TouchBatch::new(&frames),
    })
}

fn judge_frame() -> Bytes {
    let events: Vec<_> = (0..6)
        .map(|i| JudgeEvent {
            time: i as f32 * 0.1,
            line_id: i % 3,
            note_id: i,
            judgement: Judgement::Perfect,
        })
        .collect();
    encode_frame(&ClientCommand::Judges {
        judges: JudgeBatch::new(&events),
    })
}

fn percentiles(samples: &mut [u32]) -> Option<[f64; 5]> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_unstable();
    let at = |p: f64| {
        let index = ((samples.len() - 1) as f64 * p).round() as usize;
        f64::from(samples[index]) / 1000.
    };
    Some([at(0.5), at(0.9), at(0.99), at(0.999), at(1.)])
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let m = &self.metrics;
        let c = &self.config;
        let get = |counter: &AtomicU64| counter.load(Relaxed);
        let monitors = if c.monitors { c.rooms() } else { 0 };
        writeln!(
            f,
            "{} players + {} monitors in {} rooms, ran {:.1}s",
            c.players,
            monitors,
            c.rooms(),
            self.elapsed.as_secs_f64()
        )?;
        writeln!(
            f,
            "connections {}  logins {}  room entries {}  reconnects {}",
            get(&m.connected),
            get(&m.logged_in),
            get(&m.joined),
            get(&m.reconnects)
        )?;
        writeln!(
            f,
            "errors: connect {}  login {}  join {}  error replies {}  disconnects {}  stalled {}",
            get(&m.connect_errors),
            get(&m.login_errors),
            get(&m.join_errors),
            get(&m.error_replies),
            get(&m.disconnects),
            get(&m.stalled)
        )?;
        writeln!(
            f,
            "rounds completed {}  games started {}  games ended {}",
            get(&m.rounds),
            get(&m.games_started),
            get(&m.games_ended)
        )?;
        let secs = self.elapsed.as_secs_f64();
        writeln!(
            f,
            "frames received {} ({:.0}/s)  live frames sent {}  relayed to monitors {}",
            get(&m.frames_in),
            get(&m.frames_in) as f64 / secs,
            get(&m.live_frames_out),
            get(&m.live_frames_in)
        )?;
        for (name, samples) in [
            ("ping rtt", &m.ping_us),
            ("login latency", &m.login_us),
            ("request latency", &m.rpc_us),
        ] {
            let mut samples = samples.lock().unwrap();
            let n = samples.len();
            match percentiles(&mut samples) {
                Some([p50, p90, p99, p999, max]) => writeln!(
                    f,
                    "{name:>15} (n={n}): p50 {p50:.2}ms  p90 {p90:.2}ms  p99 {p99:.2}ms  p99.9 {p999:.2}ms  max {max:.2}ms"
                )?,
                None => writeln!(f, "{name:>15}: no samples")?,
            }
        }
        let kinds = m.error_kinds.lock().unwrap();
        let mut kinds: Vec<_> = kinds.iter().collect();
        kinds.sort_by(|a, b| b.1.cmp(a.1));
        if !kinds.is_empty() {
            writeln!(f, "errors by kind:")?;
            for (kind, count) in kinds.iter().take(12) {
                writeln!(f, "  {count:>6}  {kind}")?;
            }
        }
        Ok(())
    }
}
