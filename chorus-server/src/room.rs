use crate::{
    api::{Chart, Record},
    l10n::Text,
    outbox::Outbox,
    user::User,
};
use chorus_proto::{
    ClientRoomState, JoinRoomResponse, Message, Reply, RoomId, RoomState, ServerCommand, UserInfo,
    encode_frame, encode_frame_with_capacity,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{debug, info};

pub(crate) const NO_ROOM: &str = "no room";
pub(crate) const ROOM_NOT_FOUND: &str = "room not found";
pub(crate) const ALREADY_IN_ROOM: &str = "already in room";
const INVALID_STATE: &str = "invalid state";
const NOT_HOST: &str = "only host can do this";

pub(crate) fn respond<T>(out: &Outbox, variant: fn(Reply<T>) -> ServerCommand, reply: Reply<T>) {
    out.send(encode_frame(&variant(reply)));
}

pub struct Room {
    pub id: RoomId,
    inner: Mutex<RoomInner>,
}

struct RoomInner {
    host: i32,
    players: Vec<Arc<User>>,
    monitors: Vec<Arc<User>>,
    phase: Phase,
    round: u64,
    chart: Option<Arc<Chart>>,
    locked: bool,
    cycle: bool,
    live: bool,
    disbanded: bool,
}

enum Phase {
    SelectChart,
    WaitForReady {
        ready: Vec<i32>,
    },
    Playing {
        finished: Vec<i32>,
        aborted: Vec<i32>,
    },
}

impl Room {
    pub(crate) fn new(id: RoomId, host: Arc<User>) -> Self {
        Self {
            id,
            inner: Mutex::new(RoomInner {
                host: host.id,
                players: vec![host],
                monitors: Vec::new(),
                phase: Phase::SelectChart,
                round: 0,
                chart: None,
                locked: false,
                cycle: false,
                live: false,
                disbanded: false,
            }),
        }
    }

    pub(crate) fn is_playing(&self) -> bool {
        matches!(self.inner.lock().phase, Phase::Playing { .. })
    }

    pub(crate) fn round(&self) -> u64 {
        self.inner.lock().round
    }

    pub(crate) fn with_snapshot<R>(&self, user: i32, f: impl FnOnce(ClientRoomState) -> R) -> R {
        let inner = self.inner.lock();
        f(inner.snapshot(&self.id, user))
    }

    pub(crate) fn join(
        self: &Arc<Self>,
        user: &Arc<User>,
        session: u64,
        monitor: bool,
        max_players: usize,
        out: &Outbox,
    ) {
        let mut inner = self.inner.lock();
        let reply = self.join_locked(&mut inner, user, session, monitor, max_players);
        respond(out, ServerCommand::JoinRoom, reply);
    }

    fn join_locked(
        self: &Arc<Self>,
        inner: &mut RoomInner,
        user: &Arc<User>,
        session: u64,
        monitor: bool,
        max_players: usize,
    ) -> Reply<JoinRoomResponse> {
        if inner.disbanded {
            return Err(ROOM_NOT_FOUND.to_owned());
        }
        if inner.locked {
            return Err(user.lang.text(Text::RoomLocked).to_owned());
        }
        if !matches!(inner.phase, Phase::SelectChart) {
            return Err(user.lang.text(Text::GameInProgress).to_owned());
        }
        if monitor && !user.can_monitor {
            return Err(user.lang.text(Text::CannotMonitor).to_owned());
        }
        if !monitor && inner.players.len() >= max_players {
            return Err(user.lang.text(Text::RoomFull).to_owned());
        }
        {
            let mut state = user.state();
            if !state.is_current(session) {
                return Err(user.replaced_error());
            }
            if state.room.is_some() {
                return Err(ALREADY_IN_ROOM.to_owned());
            }
            state.room = Some(Arc::clone(self));
            state.monitor = monitor;
        }
        if monitor {
            inner.monitors.push(Arc::clone(user));
            if !inner.live {
                inner.live = true;
                info!(room = %self.id, "room goes live");
            }
        } else {
            inner.players.push(Arc::clone(user));
        }
        info!(room = %self.id, user = user.id, monitor, "user joined room");
        inner.broadcast(&ServerCommand::OnJoinRoom(user.info(monitor)));
        inner.broadcast_message(Message::JoinRoom {
            user: user.id,
            name: user.name.clone(),
        });
        Ok(JoinRoomResponse {
            state: inner.client_state(),
            users: inner.member_infos(),
            live: inner.live,
        })
    }

    #[must_use]
    /// Returns whether the room lost its last player and should be unregistered
    pub(crate) fn leave(&self, user: &User, out: Option<&Outbox>) -> bool {
        let mut inner = self.inner.lock();
        // The leaver gets this too; its client still thinks it is in the room
        inner.broadcast_message(Message::LeaveRoom {
            user: user.id,
            name: user.name.clone(),
        });
        inner.players.retain(|it| it.id != user.id);
        inner.monitors.retain(|it| it.id != user.id);
        {
            let mut state = user.state();
            if state
                .room
                .as_ref()
                .is_some_and(|room| std::ptr::eq(&**room, self))
            {
                state.room = None;
                state.monitor = false;
            }
        }
        info!(room = %self.id, user = user.id, "user left room");

        let mut disband = false;
        if inner.host == user.id && !inner.disbanded {
            if inner.players.is_empty() {
                inner.disbanded = true;
                disband = true;
                info!(room = %self.id, "last player left, disbanding room");
            } else {
                let next = Arc::clone(&inner.players[fastrand::usize(..inner.players.len())]);
                debug!(room = %self.id, user = next.id, "host reassigned");
                inner.host = next.id;
                inner.broadcast_message(Message::NewHost { user: next.id });
                next.send(&encode_frame(&ServerCommand::ChangeHost(true)));
            }
        }
        if !inner.disbanded {
            inner.advance(&self.id);
        }
        if let Some(out) = out {
            respond(out, ServerCommand::LeaveRoom, Ok(()));
        }
        disband
    }

    pub(crate) fn chat(&self, user: &User, content: String, out: &Outbox) {
        let inner = self.inner.lock();
        inner.broadcast_message(Message::Chat {
            user: user.id,
            content,
        });
        respond(out, ServerCommand::Chat, Ok(()));
    }

    pub(crate) fn set_locked(&self, user: &User, lock: bool, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.check_host(user.id).map(|()| {
            info!(room = %self.id, lock, "room lock changed");
            inner.locked = lock;
            inner.broadcast_message(Message::LockRoom { lock });
        });
        respond(out, ServerCommand::LockRoom, reply);
    }

    pub(crate) fn set_cycle(&self, user: &User, cycle: bool, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.check_host(user.id).map(|()| {
            info!(room = %self.id, cycle, "room cycle changed");
            inner.cycle = cycle;
            inner.broadcast_message(Message::CycleRoom { cycle });
        });
        respond(out, ServerCommand::CycleRoom, reply);
    }

    pub(crate) fn can_select_chart(&self, user: i32) -> Reply {
        self.inner.lock().can_select_chart(user)
    }

    pub(crate) fn select_chart(&self, user: &User, chart: Arc<Chart>, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.select_chart(user, chart);
        if reply.is_ok() {
            debug!(room = %self.id, user = user.id, "chart selected");
        }
        respond(out, ServerCommand::SelectChart, reply);
    }

    pub(crate) fn request_start(&self, user: &User, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.request_start(&self.id, user);
        respond(out, ServerCommand::RequestStart, reply);
    }

    pub(crate) fn ready(&self, user: &User, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.ready(&self.id, user);
        respond(out, ServerCommand::Ready, reply);
    }

    pub(crate) fn cancel_ready(&self, user: &User, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.cancel_ready(user);
        respond(out, ServerCommand::CancelReady, reply);
    }

    pub(crate) fn played(&self, user: &User, record: &Record, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.played(&self.id, user, record);
        respond(out, ServerCommand::Played, reply);
    }

    pub(crate) fn awaits_result(&self, user: i32, round: u64) -> bool {
        self.inner.lock().awaits_result(user, round)
    }

    pub(crate) fn played_late(&self, user: &User, record: &Record, round: u64) -> bool {
        let mut inner = self.inner.lock();
        let awaited = inner.awaits_result(user.id, round);
        if awaited {
            let _ = inner.played(&self.id, user, record);
        }
        awaited
    }

    pub(crate) fn abort(&self, user: &User, out: &Outbox) {
        let mut inner = self.inner.lock();
        let reply = inner.abort(&self.id, user);
        respond(out, ServerCommand::Abort, reply);
    }

    pub(crate) fn relay(&self, cmd: &ServerCommand, size_hint: usize) -> bool {
        let inner = self.inner.lock();
        if !inner.live {
            return false;
        }
        if !inner.monitors.is_empty() {
            let frame = encode_frame_with_capacity(cmd, size_hint);
            for monitor in &inner.monitors {
                monitor.send_lossy(&frame);
            }
        }
        true
    }
}

enum Step {
    Start,
    End,
}

impl RoomInner {
    fn members(&self) -> impl Iterator<Item = &Arc<User>> {
        self.players.iter().chain(&self.monitors)
    }

    fn broadcast(&self, cmd: &ServerCommand) {
        let frame = encode_frame(cmd);
        for user in self.members() {
            user.send(&frame);
        }
    }

    fn broadcast_message(&self, msg: Message) {
        self.broadcast(&ServerCommand::Message(msg));
    }

    fn client_state(&self) -> RoomState {
        match self.phase {
            Phase::SelectChart => RoomState::SelectChart(self.chart.as_ref().map(|it| it.id)),
            Phase::WaitForReady { .. } => RoomState::WaitingForReady,
            Phase::Playing { .. } => RoomState::Playing,
        }
    }

    fn member_infos(&self) -> Vec<UserInfo> {
        let players = self.players.iter().map(|it| it.info(false));
        let monitors = self.monitors.iter().map(|it| it.info(true));
        players.chain(monitors).collect()
    }

    fn snapshot(&self, id: &RoomId, user: i32) -> ClientRoomState {
        ClientRoomState {
            id: id.clone(),
            state: self.client_state(),
            live: self.live,
            locked: self.locked,
            cycle: self.cycle,
            is_host: self.host == user,
            is_ready: matches!(&self.phase, Phase::WaitForReady { ready } if ready.contains(&user)),
            users: self.member_infos(),
        }
    }

    fn awaits_result(&self, user: i32, round: u64) -> bool {
        self.round == round
            && self.players.iter().any(|it| it.id == user)
            && matches!(&self.phase, Phase::Playing { finished, aborted }
                if !finished.contains(&user) && !aborted.contains(&user))
    }

    fn check_host(&self, user: i32) -> Reply {
        if self.host != user {
            return Err(NOT_HOST.to_owned());
        }
        Ok(())
    }

    fn can_select_chart(&self, user: i32) -> Reply {
        if !matches!(self.phase, Phase::SelectChart) {
            return Err(INVALID_STATE.to_owned());
        }
        self.check_host(user)
    }

    fn select_chart(&mut self, user: &User, chart: Arc<Chart>) -> Reply {
        // The room may have changed while the chart was fetched
        if self.disbanded || !self.players.iter().any(|it| it.id == user.id) {
            return Err(NO_ROOM.to_owned());
        }
        self.can_select_chart(user.id)?;
        self.broadcast_message(Message::SelectChart {
            user: user.id,
            name: chart.name.clone(),
            id: chart.id,
        });
        self.chart = Some(chart);
        self.broadcast(&ServerCommand::ChangeState(self.client_state()));
        Ok(())
    }

    fn request_start(&mut self, room: &RoomId, user: &User) -> Reply {
        self.can_select_chart(user.id)?;
        if self.chart.is_none() {
            return Err(user.lang.text(Text::NoChartSelected).to_owned());
        }
        debug!(room = %room, "waiting for players to get ready");
        self.broadcast_message(Message::GameStart { user: user.id });
        self.phase = Phase::WaitForReady {
            ready: vec![user.id],
        };
        self.broadcast(&ServerCommand::ChangeState(RoomState::WaitingForReady));
        self.advance(room);
        Ok(())
    }

    fn ready(&mut self, room: &RoomId, user: &User) -> Reply {
        if let Phase::WaitForReady { ready } = &mut self.phase {
            if ready.contains(&user.id) {
                return Err("already ready".to_owned());
            }
            ready.push(user.id);
            self.broadcast_message(Message::Ready { user: user.id });
            self.advance(room);
        }
        Ok(())
    }

    fn cancel_ready(&mut self, user: &User) -> Reply {
        if let Phase::WaitForReady { ready } = &mut self.phase {
            let Some(pos) = ready.iter().position(|&id| id == user.id) else {
                return Err("not ready".to_owned());
            };
            ready.swap_remove(pos);
            if self.host == user.id {
                self.broadcast_message(Message::CancelGame { user: user.id });
                self.phase = Phase::SelectChart;
                self.broadcast(&ServerCommand::ChangeState(self.client_state()));
            } else {
                self.broadcast_message(Message::CancelReady { user: user.id });
            }
        }
        Ok(())
    }

    fn played(&mut self, room: &RoomId, user: &User, record: &Record) -> Reply {
        let msg = Message::Played {
            user: user.id,
            score: record.score,
            accuracy: record.accuracy,
            full_combo: record.full_combo,
        };
        if let Phase::Playing { finished, aborted } = &mut self.phase {
            if aborted.contains(&user.id) {
                return Err("aborted".to_owned());
            }
            if finished.contains(&user.id) {
                return Err("already uploaded".to_owned());
            }
            finished.push(user.id);
            self.broadcast_message(msg);
            self.advance(room);
        } else {
            self.broadcast_message(msg);
        }
        Ok(())
    }

    fn abort(&mut self, room: &RoomId, user: &User) -> Reply {
        if let Phase::Playing { finished, aborted } = &mut self.phase {
            if finished.contains(&user.id) {
                return Err("already uploaded".to_owned());
            }
            if aborted.contains(&user.id) {
                return Err("aborted".to_owned());
            }
            aborted.push(user.id);
            self.broadcast_message(Message::Abort { user: user.id });
            self.advance(room);
        }
        Ok(())
    }

    fn advance(&mut self, room: &RoomId) {
        let step = match &self.phase {
            Phase::WaitForReady { ready } if self.members().all(|it| ready.contains(&it.id)) => {
                Step::Start
            }
            Phase::Playing { finished, aborted }
                if self
                    .players
                    .iter()
                    .all(|it| finished.contains(&it.id) || aborted.contains(&it.id)) =>
            {
                Step::End
            }
            _ => return,
        };
        match step {
            Step::Start => {
                info!(room = %room, "game started");
                self.round += 1;
                self.broadcast_message(Message::StartPlaying);
                self.phase = Phase::Playing {
                    finished: Vec::new(),
                    aborted: Vec::new(),
                };
                self.broadcast(&ServerCommand::ChangeState(RoomState::Playing));
            }
            Step::End => {
                info!(room = %room, "game ended");
                self.broadcast_message(Message::GameEnd);
                self.phase = Phase::SelectChart;
                if self.cycle && !self.players.is_empty() {
                    self.rotate_host();
                }
                self.broadcast(&ServerCommand::ChangeState(self.client_state()));
            }
        }
    }

    fn rotate_host(&mut self) {
        let old = self.host;
        let next = self
            .players
            .iter()
            .position(|it| it.id == old)
            .map_or(0, |pos| (pos + 1) % self.players.len());
        let new_host = Arc::clone(&self.players[next]);
        self.host = new_host.id;
        self.broadcast_message(Message::NewHost { user: new_host.id });
        if let Some(previous) = self.players.iter().find(|it| it.id == old) {
            previous.send(&encode_frame(&ServerCommand::ChangeHost(false)));
        }
        new_host.send(&encode_frame(&ServerCommand::ChangeHost(true)));
    }
}
