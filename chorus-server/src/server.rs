use crate::{
    api::{Api, Profile},
    config::Config,
    http::{RoomStatus, serve_http},
    l10n::{Lang, Text},
    logins::Logins,
    outbox::Outbox,
    room::{ROOM_NOT_FOUND, Room, respond},
    session,
    stats::{Snapshot, Stats},
    user::{SessionLink, User},
};
use anyhow::{Context, Result};
use chorus_proto::{Message, Reply, RoomId, ServerCommand, encode_frame};
use dashmap::{DashMap, mapref::entry::Entry};
use std::{
    collections::HashSet,
    future::Future,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{net::TcpListener, time};
use tracing::{debug, info, warn};

const SAVE_LOGINS_EVERY: Duration = Duration::from_secs(30);

pub struct Shared {
    pub config: Config,
    pub stats: Arc<Stats>,
    pub(crate) api: Arc<Api>,
    pub(crate) logins: Arc<Logins>,
    monitors: HashSet<i32>,
    users: DashMap<i32, Arc<User>>,
    rooms: DashMap<RoomId, Arc<Room>>,
    next_session: AtomicU64,
}

impl Shared {
    pub fn new(config: Config) -> Result<Arc<Self>> {
        let stats = Arc::new(Stats::default());
        let api = Arc::new(Api::new(&config, Arc::clone(&stats))?);
        let logins = Logins::new(&config, Arc::clone(&api), Arc::clone(&stats));
        Ok(Arc::new(Self {
            api,
            logins: Arc::new(logins),
            monitors: config.monitor_set(),
            config,
            stats,
            users: DashMap::new(),
            rooms: DashMap::new(),
            next_session: AtomicU64::new(1),
        }))
    }

    pub fn online_users(&self) -> usize {
        self.users.len()
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// Every room, ordered by id
    pub(crate) fn room_statuses(&self) -> Vec<RoomStatus> {
        // No room is locked while the map is
        let mut rooms: Vec<_> = self.rooms.iter().map(|it| Arc::clone(&it)).collect();
        rooms.sort_unstable_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        rooms.iter().filter_map(|it| it.status()).collect()
    }

    pub(crate) fn next_session_id(&self) -> u64 {
        self.next_session.fetch_add(1, Relaxed)
    }

    pub async fn save_logins(&self) {
        let logins = Arc::clone(&self.logins);
        match tokio::task::spawn_blocking(move || logins.save()).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(%err, "could not save remembered logins"),
            Err(err) => warn!(%err, "saving remembered logins failed"),
        }
    }

    pub(crate) fn login(&self, profile: &Profile, session: u64, out: &Outbox) -> Arc<User> {
        loop {
            let user = Arc::clone(&*self.users.entry(profile.id).or_insert_with(|| {
                Arc::new(User::new(
                    profile.id,
                    profile.name.clone(),
                    Lang::from_tag(&profile.language),
                    self.monitors.contains(&profile.id),
                ))
            }));
            if attach(&user, session, out) {
                return user;
            }
            self.users
                .remove_if(&profile.id, |_, it| Arc::ptr_eq(it, &user));
        }
    }

    pub(crate) fn disconnect(self: &Arc<Self>, user: &Arc<User>, session: u64) {
        let (room, epoch) = {
            let mut state = user.state();
            if !state.is_current(session) {
                return;
            }
            state.session = None;
            state.epoch += 1;
            (state.room.clone(), state.epoch)
        };
        let grace = match &room {
            Some(room) if room.is_playing() => {
                info!(user = user.id, "lost connection while playing");
                self.config.playing_grace()
            }
            Some(_) => {
                debug!(user = user.id, "user disconnected, holding room seat");
                self.config.reconnect_grace()
            }
            None => Duration::ZERO,
        };
        let runtime = tokio::runtime::Handle::try_current();
        match runtime {
            Ok(runtime) if !grace.is_zero() => {
                let shared = Arc::clone(self);
                let user = Arc::clone(user);
                runtime.spawn(async move {
                    time::sleep(grace).await;
                    shared.retire(&user, epoch);
                });
            }
            _ => self.retire(user, epoch),
        }
    }

    fn retire(&self, user: &Arc<User>, epoch: u64) {
        let room = {
            let mut state = user.state();
            if state.epoch != epoch || state.retired {
                return;
            }
            state.retired = true;
            state.room.clone()
        };
        self.users
            .remove_if(&user.id, |_, it| Arc::ptr_eq(it, user));
        debug!(user = user.id, "user removed");
        if let Some(room) = room
            && room.leave(user, None)
        {
            self.unregister(&room);
        }
    }

    pub(crate) fn unregister(&self, room: &Arc<Room>) {
        if self
            .rooms
            .remove_if(&room.id, |_, it| Arc::ptr_eq(it, room))
            .is_some()
        {
            debug!(room = %room.id, "room removed");
        }
    }

    pub(crate) fn create_room(&self, user: &Arc<User>, session: u64, id: RoomId, out: &Outbox) {
        if let Err(err) = self.try_create_room(user, session, id, out) {
            respond(out, ServerCommand::CreateRoom, Err(err));
        }
    }

    fn try_create_room(&self, user: &Arc<User>, session: u64, id: RoomId, out: &Outbox) -> Reply {
        user.check_can_enter(session)?;
        let Entry::Vacant(slot) = self.rooms.entry(id.clone()) else {
            return Err(user.lang.text(Text::RoomIdTaken).to_owned());
        };
        let room = Arc::new(Room::new(id, Arc::clone(user)));
        {
            let mut state = user.state();
            if !state.is_current(session) {
                return Err(user.replaced_error());
            }
            if state.room.is_some() {
                return Err(crate::room::ALREADY_IN_ROOM.to_owned());
            }
            state.room = Some(Arc::clone(&room));
            state.monitor = false;
        }
        out.send(encode_frame(&ServerCommand::Message(Message::CreateRoom {
            user: user.id,
        })));
        respond(out, ServerCommand::CreateRoom, Ok(()));
        info!(room = %room.id, user = user.id, "room created");
        slot.insert(room);
        Ok(())
    }

    pub(crate) fn join_room(
        &self,
        user: &Arc<User>,
        session: u64,
        id: &RoomId,
        monitor: bool,
        out: &Outbox,
    ) {
        let room = user.check_can_enter(session).and_then(|()| {
            self.rooms
                .get(id)
                .map(|it| Arc::clone(&it))
                .ok_or_else(|| ROOM_NOT_FOUND.to_owned())
        });
        match room {
            Ok(room) => room.join(user, session, monitor, self.config.room_max_users, out),
            Err(err) => respond(out, ServerCommand::JoinRoom, Err(err)),
        }
    }
}

/// Under the room lock, so the login reply precedes any room event and its
/// snapshot is what later events build on. Returns `false` if the user was retired
fn attach(user: &Arc<User>, session: u64, out: &Outbox) -> bool {
    loop {
        let room = {
            let state = user.state();
            if state.retired {
                return false;
            }
            state.room.clone()
        };
        let link = SessionLink {
            id: session,
            outbox: out.clone(),
        };
        let attached = match &room {
            None => {
                let mut state = user.state();
                if state.retired {
                    return false;
                }
                if state.room.is_some() {
                    continue;
                }
                replace_session(user, &mut state.session, link);
                state.epoch += 1;
                let info = user.info(state.monitor);
                respond(out, ServerCommand::Authenticate, Ok((info, None)));
                Some(true)
            }
            Some(room) => room.with_snapshot(user.id, |snapshot| {
                let mut state = user.state();
                if state.retired {
                    return Some(false);
                }
                if !state.room.as_ref().is_some_and(|it| Arc::ptr_eq(it, room)) {
                    return None;
                }
                replace_session(user, &mut state.session, link);
                state.epoch += 1;
                let info = user.info(state.monitor);
                respond(out, ServerCommand::Authenticate, Ok((info, Some(snapshot))));
                Some(true)
            }),
        };
        if let Some(attached) = attached {
            return attached;
        }
    }
}

fn replace_session(user: &User, slot: &mut Option<SessionLink>, link: SessionLink) {
    if let Some(previous) = slot.replace(link) {
        info!(
            user = user.id,
            previous = previous.id,
            "user connected from a new session"
        );
    }
}

pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    if addr.is_ipv6() {
        let _ = socket.set_only_v6(false);
    }
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(4096)?;
    TcpListener::from_std(socket.into())
}

pub async fn serve(
    listener: TcpListener,
    config: Config,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    serve_with(listener, Shared::new(config)?, shutdown).await
}

pub async fn serve_with(
    listener: TcpListener,
    shared: Arc<Shared>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let http = if let Some(addr) = shared.config.http_addr(listener.local_addr()?.ip())? {
        let listener = bind(addr).with_context(|| format!("could not listen on {addr}"))?;
        info!(%addr, "HTTP API listening");
        let shared = Arc::clone(&shared);
        Some(tokio::spawn(async move {
            if let Err(err) = serve_http(listener, shared).await {
                warn!(%err, "HTTP API stopped");
            }
        }))
    } else {
        None
    };
    let max = shared.config.max_connections;
    let hard_max = max + max / 10 + 64;
    let reporter = (shared.config.stats_interval_secs > 0).then(|| {
        tokio::spawn(report_stats(
            Arc::clone(&shared),
            Duration::from_secs(shared.config.stats_interval_secs),
        ))
    });
    let saver = shared.logins.persistent().then(|| {
        let shared = Arc::clone(&shared);
        tokio::spawn(async move {
            let mut ticker = time::interval(SAVE_LOGINS_EVERY);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                shared.save_logins().await;
            }
        })
    });
    tokio::pin!(shutdown);
    loop {
        let (stream, peer) = tokio::select! {
            _ = &mut shutdown => break,
            res = listener.accept() => match res {
                Ok(conn) => conn,
                Err(err) => {
                    warn!(%err, "accept failed");
                    if is_resource_exhaustion(&err) {
                        time::sleep(Duration::from_millis(100)).await;
                    }
                    continue;
                }
            },
        };
        Stats::add(&shared.stats.accepted, 1);
        let active = shared.stats.connections.fetch_add(1, Relaxed) + 1;
        if active as usize > hard_max {
            shared.stats.connections.fetch_sub(1, Relaxed);
            continue;
        }
        let full = active as usize > max;
        tokio::spawn(session::run(Arc::clone(&shared), stream, peer, full));
    }
    if let Some(http) = http {
        http.abort();
    }
    if let Some(reporter) = reporter {
        reporter.abort();
    }
    if let Some(saver) = saver {
        saver.abort();
        shared.save_logins().await;
    }
    Ok(())
}

fn is_resource_exhaustion(err: &io::Error) -> bool {
    #[cfg(unix)]
    if let Some(code) = err.raw_os_error() {
        return [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM].contains(&code);
    }
    let _ = err;
    false
}

async fn report_stats(shared: Arc<Shared>, every: Duration) {
    let mut ticker = time::interval(every);
    ticker.tick().await;
    let mut prev = shared.stats.snapshot();
    let mut last = None;
    loop {
        ticker.tick().await;
        let now = shared.stats.snapshot();
        let delta = now.since(&prev);
        prev = now;
        let secs = every.as_secs_f64();
        let per_sec = |n: u64, unit: f64| (n as f64 / unit / secs).round();
        let (connections, users, rooms) = (
            shared.stats.connections.load(Relaxed),
            shared.online_users(),
            shared.room_count(),
        );
        let traffic = [
            per_sec(delta.frames_in, 1.),
            per_sec(delta.frames_out, 1.),
            per_sec(delta.bytes_in, 1024.),
            per_sec(delta.bytes_out, 1024.),
        ];
        // Skip a line that would read the same as the last one, as an idle
        // server's do. Traffic is compared as shown, rounded
        let shown = (
            (connections, users, rooms),
            Snapshot {
                frames_in: 0,
                bytes_in: 0,
                frames_out: 0,
                bytes_out: 0,
                ..delta
            },
            traffic,
        );
        if last.as_ref() == Some(&shown) {
            continue;
        }
        last = Some(shown);
        let [
            frames_in_per_sec,
            frames_out_per_sec,
            kib_in_per_sec,
            kib_out_per_sec,
        ] = traffic;
        info!(
            connections,
            users,
            rooms,
            accepted = delta.accepted,
            logins = delta.logins,
            remembered_logins = delta.remembered_logins,
            abandoned_logins = delta.abandoned_logins,
            login_failures = delta.login_failures,
            revoked_logins = delta.revoked_logins,
            frames_in_per_sec,
            frames_out_per_sec,
            kib_in_per_sec,
            kib_out_per_sec,
            dropped_frames = delta.dropped_frames,
            kicked = delta.kicked,
            api_calls = delta.api_calls,
            api_errors = delta.api_errors,
            "stats"
        );
    }
}
