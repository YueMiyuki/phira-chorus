use crate::{
    api::{ApiError, Chart, Lookup, Profile, Record},
    logins::{Admission, Logins},
    outbox::Outbox,
    room::{Room, respond},
    server::Shared,
    stats::Stats,
    user::User,
};
use bytes::{Buf, Bytes, BytesMut};
use chorus_proto::{
    ClientCommand, HEARTBEAT_DISCONNECT_TIMEOUT, MAX_FRAME_LEN, Reply, ServerCommand, split_frame,
};
use std::{
    collections::VecDeque, future::Future, net::SocketAddr, pin::Pin, sync::Arc,
    sync::atomic::Ordering::Relaxed, time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpStream, tcp::OwnedReadHalf},
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

const READ_BUF: usize = 4096;
const MAX_BACKLOG: usize = 64;
const FLUSH_TIMEOUT: Duration = Duration::from_secs(3);
const PONG: Bytes = Bytes::from_static(&[1, 0]);
const RETRY_PAUSE: Duration = Duration::from_secs(1);
const RECHECK_PAUSE: Duration = Duration::from_secs(30);
const MAX_RECHECK_PAUSE: Duration = Duration::from_secs(600);

type Pending = Pin<Box<dyn Future<Output = Completion> + Send>>;

enum Completion {
    Login(Result<Arc<Profile>, ApiError>),
    Chart {
        room: Arc<Room>,
        result: Option<Result<Arc<Chart>, ApiError>>,
    },
    Record {
        id: i32,
        room: Arc<Room>,
        round: u64,
        lookup: Lookup<Record>,
        result: Option<Result<Record, ApiError>>,
    },
}

enum Close {
    Eof,
    Io(std::io::Error),
    Idle,
    LoginTimeout,
    LoginFailed,
    Full,
    Killed,
    Malformed(String),
    Flood,
}

pub(crate) async fn run(shared: Arc<Shared>, stream: TcpStream, peer: SocketAddr, full: bool) {
    let _ = stream.set_nodelay(true);
    let (mut rd, wr) = stream.into_split();
    let close = CancellationToken::new();
    let config = &shared.config;
    let (outbox, writer) = Outbox::new(
        config.outbox_lossy_limit,
        config.outbox_limit,
        close.clone(),
        Arc::clone(&shared.stats),
    );
    let mut writer = tokio::spawn(writer.run(wr));
    let mut session = Session {
        id: shared.next_session_id(),
        limiter: RateLimiter::new(config.command_rate, config.command_burst),
        shared,
        peer,
        outbox,
        close: close.clone(),
        user: None,
        full,
    };
    let reason = session.serve(&mut rd).await;
    session.log_close(reason);
    drop(session);
    let _ = time::timeout(FLUSH_TIMEOUT, &mut writer).await;
    close.cancel();
}

struct Session {
    id: u64,
    shared: Arc<Shared>,
    peer: SocketAddr,
    outbox: Outbox,
    close: CancellationToken,
    user: Option<Arc<User>>,
    full: bool,
    limiter: RateLimiter,
}

impl Session {
    async fn serve(&mut self, rd: &mut OwnedReadHalf) -> Close {
        let close = self.close.clone();
        let mut buf = BytesMut::with_capacity(READ_BUF);
        let mut handshaken = false;
        let mut pending: Option<Pending> = None;
        let mut backlog = VecDeque::new();
        let mut last_recv = Instant::now();
        let idle = time::sleep(HEARTBEAT_DISCONNECT_TIMEOUT);
        let login_deadline = time::sleep(self.shared.config.login_timeout());
        tokio::pin!(idle, login_deadline);

        loop {
            tokio::select! {
                biased;
                _ = close.cancelled() => return Close::Killed,
                _ = &mut idle => {
                    let deadline = last_recv + HEARTBEAT_DISCONNECT_TIMEOUT;
                    if Instant::now() >= deadline {
                        return Close::Idle;
                    }
                    idle.as_mut().reset(deadline);
                }
                _ = &mut login_deadline, if self.user.is_none() && pending.is_none() => {
                    return Close::LoginTimeout;
                }
                done = wait(&mut pending) => {
                    pending = None;
                    if let Err(close) = self.complete(done) {
                        return close;
                    }
                    while pending.is_none() {
                        let Some(cmd) = backlog.pop_front() else { break };
                        if let Err(close) = self.dispatch(cmd, &mut pending) {
                            return close;
                        }
                    }
                }
                res = rd.read_buf(&mut buf) => {
                    match res {
                        Ok(0) => return self.closed(Close::Eof, &pending),
                        Ok(n) => Stats::add(&self.shared.stats.bytes_in, n as u64),
                        Err(err) => return self.closed(Close::Io(err), &pending),
                    }
                    last_recv = Instant::now();
                    if !handshaken {
                        let version = buf.get_u8();
                        trace!(peer = %self.peer, version, "connection handshake");
                        handshaken = true;
                    }
                    loop {
                        match split_frame(&mut buf, MAX_FRAME_LEN) {
                            Ok(Some(payload)) => {
                                if let Err(close) = self.on_frame(payload, &mut pending, &mut backlog) {
                                    return close;
                                }
                            }
                            Ok(None) => break,
                            Err(err) => return Close::Malformed(err.to_string()),
                        }
                    }
                    if buf.is_empty() && buf.capacity() > 16 * READ_BUF {
                        buf = BytesMut::with_capacity(READ_BUF);
                    } else if buf.capacity() - buf.len() < READ_BUF / 4 {
                        buf.reserve(READ_BUF);
                    }
                }
            }
        }
    }

    fn on_frame(
        &mut self,
        payload: Bytes,
        pending: &mut Option<Pending>,
        backlog: &mut VecDeque<ClientCommand>,
    ) -> Result<(), Close> {
        Stats::add(&self.shared.stats.frames_in, 1);
        let cmd = match ClientCommand::from_payload(&payload) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => {
                debug!(peer = %self.peer, tag = payload[0], "ignoring unknown command");
                return Ok(());
            }
            Err(err) => return Err(Close::Malformed(err.to_string())),
        };
        if matches!(cmd, ClientCommand::Ping) {
            self.outbox.send(PONG);
            return Ok(());
        }
        if !self.limiter.allow() {
            return Err(Close::Flood);
        }
        if pending.is_some() {
            let relay = matches!(
                cmd,
                ClientCommand::Touches { .. } | ClientCommand::Judges { .. }
            );
            if relay && self.user.is_some() {
                return self.dispatch(cmd, pending);
            }
            if backlog.len() >= MAX_BACKLOG {
                return Err(Close::Flood);
            }
            backlog.push_back(cmd);
            return Ok(());
        }
        self.dispatch(cmd, pending)
    }

    fn dispatch(&mut self, cmd: ClientCommand, pending: &mut Option<Pending>) -> Result<(), Close> {
        let Some(user) = self.user.clone() else {
            return self.dispatch_anonymous(cmd, pending);
        };
        trace!(user = user.id, cmd = cmd.name(), "command");
        let (session, out, shared) = (self.id, &self.outbox, &self.shared);
        let in_room = |variant: fn(Reply) -> ServerCommand, f: &dyn Fn(Arc<Room>)| match user
            .room_for(session)
        {
            Ok(room) => f(room),
            Err(err) => respond(out, variant, Err(err)),
        };
        match cmd {
            ClientCommand::Ping => out.send(PONG),
            ClientCommand::Authenticate { .. } => respond(
                out,
                ServerCommand::Authenticate,
                Err("repeated authenticate".to_owned()),
            ),
            ClientCommand::Chat { message } => match user.room_for(session) {
                Ok(room) => room.chat(&user, message, out),
                Err(err) => respond(out, ServerCommand::Chat, Err(err)),
            },
            ClientCommand::Touches { frames } => {
                let hint = 5 + frames.raw().len();
                self.relay(
                    &user,
                    hint,
                    ServerCommand::Touches {
                        player: user.id,
                        frames,
                    },
                );
            }
            ClientCommand::Judges { judges } => {
                let hint = 5 + judges.raw().len();
                self.relay(
                    &user,
                    hint,
                    ServerCommand::Judges {
                        player: user.id,
                        judges,
                    },
                );
            }
            ClientCommand::CreateRoom { id } => shared.create_room(&user, session, id, out),
            ClientCommand::JoinRoom { id, monitor } => {
                shared.join_room(&user, session, &id, monitor, out)
            }
            ClientCommand::LeaveRoom => in_room(ServerCommand::LeaveRoom, &|room| {
                if room.leave(&user, Some(out)) {
                    shared.unregister(&room);
                }
            }),
            ClientCommand::LockRoom { lock } => in_room(ServerCommand::LockRoom, &|room| {
                room.set_locked(&user, lock, out)
            }),
            ClientCommand::CycleRoom { cycle } => in_room(ServerCommand::CycleRoom, &|room| {
                room.set_cycle(&user, cycle, out)
            }),
            ClientCommand::SelectChart { id } => {
                let room = user
                    .room_for(session)
                    .and_then(|room| room.can_select_chart(user.id).map(|()| room));
                match room {
                    Ok(room) => {
                        let lookup = shared.api.chart(id);
                        let wait = shared.config.api_wait();
                        *pending = Some(Box::pin(async move {
                            let result = lookup.wait_up_to(wait).await;
                            Completion::Chart { room, result }
                        }));
                    }
                    Err(err) => respond(out, ServerCommand::SelectChart, Err(err)),
                }
            }
            ClientCommand::RequestStart => in_room(ServerCommand::RequestStart, &|room| {
                room.request_start(&user, out)
            }),
            ClientCommand::Ready => in_room(ServerCommand::Ready, &|room| room.ready(&user, out)),
            ClientCommand::CancelReady => in_room(ServerCommand::CancelReady, &|room| {
                room.cancel_ready(&user, out)
            }),
            ClientCommand::Played { id } => match user.room_for(session) {
                Ok(room) => {
                    let round = room.round();
                    let lookup = shared.api.record(id);
                    let wait = shared.config.api_wait();
                    *pending = Some(Box::pin(async move {
                        let result = lookup.clone().wait_up_to(wait).await;
                        Completion::Record {
                            id,
                            room,
                            round,
                            lookup,
                            result,
                        }
                    }));
                }
                Err(err) => respond(out, ServerCommand::Played, Err(err)),
            },
            ClientCommand::Abort => in_room(ServerCommand::Abort, &|room| room.abort(&user, out)),
        }
        Ok(())
    }

    fn dispatch_anonymous(
        &mut self,
        cmd: ClientCommand,
        pending: &mut Option<Pending>,
    ) -> Result<(), Close> {
        match cmd {
            ClientCommand::Authenticate { token } => {
                if self.full {
                    respond(
                        &self.outbox,
                        ServerCommand::Authenticate,
                        Err("server is full".to_owned()),
                    );
                    return Err(Close::Full);
                }
                match self.shared.logins.admit(&token) {
                    Admission::Known(profile) => {
                        self.log_in(&profile);
                    }
                    Admission::Recheck(profile, check) => {
                        let user = self.log_in(&profile);
                        self.watch_token(user, token, check);
                    }
                    Admission::Lookup(lookup) => {
                        let logins = Arc::clone(&self.shared.logins);
                        *pending = Some(Box::pin(async move {
                            Completion::Login(settle_login(&logins, &token, lookup, false).await)
                        }));
                    }
                }
            }
            other => {
                debug!(peer = %self.peer, cmd = other.name(), "ignoring command before login");
            }
        }
        Ok(())
    }

    fn log_in(&mut self, profile: &Profile) -> i32 {
        let user = self.shared.login(profile, self.id, &self.outbox);
        Stats::add(&self.shared.stats.logins, 1);
        info!(peer = %self.peer, user = user.id, name = %user.name, "user logged in");
        let id = user.id;
        self.user = Some(user);
        id
    }

    fn watch_token(&self, user: i32, token: String, check: Lookup<Arc<Profile>>) {
        let close = self.close.clone();
        let logins = Arc::clone(&self.shared.logins);
        let stats = Arc::clone(&self.shared.stats);
        let peer = self.peer;
        tokio::spawn(async move {
            let verdict = tokio::select! {
                _ = close.cancelled() => return,
                verdict = settle_login(&logins, &token, check, true) => verdict,
            };
            let revoked = !matches!(verdict, Ok(profile) if profile.id == user);
            if revoked {
                Stats::add(&stats.revoked_logins, 1);
                warn!(%peer, user, "login token is no longer valid, closing");
                close.cancel();
            }
        });
    }

    fn complete(&mut self, done: Completion) -> Result<(), Close> {
        let out = &self.outbox;
        match done {
            Completion::Login(Ok(profile)) => {
                self.log_in(&profile);
            }
            Completion::Login(Err(err)) => {
                Stats::add(&self.shared.stats.login_failures, 1);
                info!(peer = %self.peer, %err, "login refused");
                respond(
                    out,
                    ServerCommand::Authenticate,
                    Err("failed to fetch info".to_owned()),
                );
                return Err(Close::LoginFailed);
            }
            Completion::Chart { room, result } => {
                let Some(user) = &self.user else {
                    return Ok(());
                };
                match result.unwrap_or(Err(ApiError::Timeout)) {
                    Ok(chart) => room.select_chart(user, chart, out),
                    Err(err) => respond(
                        out,
                        ServerCommand::SelectChart,
                        Err(format!("failed to fetch chart: {err}")),
                    ),
                }
            }
            Completion::Record {
                id,
                room,
                round,
                lookup,
                result,
            } => {
                let Some(user) = &self.user else {
                    return Ok(());
                };
                let err = match result {
                    Some(Err(err)) if err.is_transient() => err,
                    None => ApiError::Timeout,
                    Some(result) => {
                        self.played(user, result);
                        return Ok(());
                    }
                };
                respond(
                    out,
                    ServerCommand::Played,
                    Err(format!("failed to fetch record: {err}")),
                );
                // The client will not upload again and the game cannot end without
                // this result, so keep trying while the game awaits it
                let api = Arc::clone(&self.shared.api);
                let user = Arc::clone(user);
                tokio::spawn(async move {
                    let mut lookup = lookup;
                    loop {
                        match lookup.wait().await {
                            Ok(record) => {
                                if record.player == user.id
                                    && room.played_late(&user, &record, round)
                                {
                                    debug!(
                                        user = user.id,
                                        record = record.id,
                                        "late result counted"
                                    );
                                }
                                return;
                            }
                            Err(err)
                                if err.is_transient() && room.awaits_result(user.id, round) =>
                            {
                                time::sleep(RETRY_PAUSE).await;
                                lookup = api.record(id);
                            }
                            Err(_) => return,
                        }
                    }
                });
            }
        }
        Ok(())
    }

    fn played(&self, user: &Arc<User>, result: Result<Record, ApiError>) {
        let out = &self.outbox;
        let reply = match result {
            Ok(record) if record.player != user.id => Err("invalid record".to_owned()),
            Ok(record) => user.room_for(self.id).map(|room| (room, record)),
            Err(err) => Err(format!("failed to fetch record: {err}")),
        };
        match reply {
            Ok((room, record)) => {
                debug!(user = user.id, record = record.id, "result uploaded");
                room.played(user, &record, out);
            }
            Err(err) => respond(out, ServerCommand::Played, Err(err)),
        }
    }

    fn closed(&self, reason: Close, pending: &Option<Pending>) -> Close {
        if self.user.is_none() && pending.is_some() {
            Stats::add(&self.shared.stats.abandoned_logins, 1);
            debug!(peer = %self.peer, "client gave up waiting for its login");
        }
        reason
    }

    fn relay(&self, user: &User, size_hint: usize, cmd: ServerCommand) {
        match user.room_for(self.id) {
            Ok(room) => {
                if !room.relay(&cmd, size_hint) {
                    trace!(user = user.id, "live data for a room that is not live");
                }
            }
            Err(_) => trace!(user = user.id, "live data outside of a room"),
        }
    }

    fn log_close(&self, reason: Close) {
        let peer = self.peer;
        let user = self.user.as_ref().map(|it| it.id);
        match reason {
            Close::Eof => debug!(%peer, ?user, "connection closed"),
            Close::Io(err) => debug!(%peer, ?user, %err, "connection error"),
            Close::Idle => info!(%peer, ?user, "connection timed out"),
            Close::LoginTimeout => debug!(%peer, "no login in time"),
            Close::LoginFailed | Close::Full => {}
            Close::Killed if self.outbox.overflowed() => {
                warn!(%peer, ?user, "connection fell too far behind, closed")
            }
            Close::Killed => debug!(%peer, ?user, "connection closed by server"),
            Close::Malformed(err) => warn!(%peer, ?user, %err, "malformed data, closing"),
            Close::Flood => {
                Stats::add(&self.shared.stats.kicked, 1);
                warn!(%peer, ?user, "too many requests, closing")
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(user) = self.user.take() {
            self.shared.disconnect(&user, self.id);
        }
        self.shared.stats.connections.fetch_sub(1, Relaxed);
    }
}

/// Background re-checks try once per round and back off, so a crowd of
/// remembered players does not flood a struggling API
async fn settle_login(
    logins: &Arc<Logins>,
    token: &str,
    mut lookup: Lookup<Arc<Profile>>,
    background: bool,
) -> Result<Arc<Profile>, ApiError> {
    let mut pause = if background {
        RECHECK_PAUSE
    } else {
        RETRY_PAUSE
    };
    loop {
        match lookup.wait().await {
            Err(err) if err != ApiError::Unauthenticated => {
                debug!(%err, "token lookup gave up, asking again");
                time::sleep(pause).await;
                if background {
                    pause = (pause * 2).min(MAX_RECHECK_PAUSE);
                }
                lookup = logins.check(token, !background);
            }
            res => return res,
        }
    }
}

async fn wait(pending: &mut Option<Pending>) -> Completion {
    match pending {
        Some(op) => op.await,
        None => std::future::pending().await,
    }
}

struct RateLimiter {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(rate: u32, burst: u32) -> Self {
        let burst = f64::from(burst.max(1));
        Self {
            rate: f64::from(rate),
            burst,
            tokens: burst,
            last: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        if self.rate == 0. {
            return true;
        }
        let now = Instant::now();
        let refill = now.duration_since(self.last).as_secs_f64() * self.rate;
        self.tokens = (self.tokens + refill).min(self.burst);
        self.last = now;
        if self.tokens >= 1. {
            self.tokens -= 1.;
            true
        } else {
            false
        }
    }
}
