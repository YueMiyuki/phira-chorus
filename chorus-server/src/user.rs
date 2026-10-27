use crate::{
    l10n::{Lang, Text},
    outbox::Outbox,
    room::{ALREADY_IN_ROOM, NO_ROOM, Room},
};
use bytes::Bytes;
use chorus_proto::{Reply, UserInfo};
use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

pub struct User {
    pub id: i32,
    pub name: String,
    pub lang: Lang,
    pub can_monitor: bool,
    state: Mutex<UserState>,
}

#[derive(Default)]
pub(crate) struct UserState {
    pub session: Option<SessionLink>,
    pub room: Option<Arc<Room>>,
    pub monitor: bool,
    pub epoch: u64,
    pub retired: bool,
}

pub(crate) struct SessionLink {
    pub id: u64,
    pub outbox: Outbox,
}

impl UserState {
    pub fn is_current(&self, session: u64) -> bool {
        self.session.as_ref().is_some_and(|link| link.id == session)
    }
}

impl User {
    pub fn new(id: i32, name: String, lang: Lang, can_monitor: bool) -> Self {
        Self {
            id,
            name,
            lang,
            can_monitor,
            state: Mutex::default(),
        }
    }

    /// Never take a room lock while holding this
    pub(crate) fn state(&self) -> MutexGuard<'_, UserState> {
        self.state.lock()
    }

    pub fn info(&self, monitor: bool) -> UserInfo {
        UserInfo {
            id: self.id,
            name: self.name.clone(),
            monitor,
        }
    }

    pub fn send(&self, frame: &Bytes) {
        if let Some(link) = &self.state().session {
            link.outbox.send(frame.clone());
        }
    }

    pub fn send_lossy(&self, frame: &Bytes) {
        if let Some(link) = &self.state().session {
            link.outbox.send_lossy(frame.clone());
        }
    }

    pub(crate) fn replaced_error(&self) -> String {
        self.lang.text(Text::LoggedInElsewhere).to_owned()
    }

    pub(crate) fn room_for(&self, session: u64) -> Reply<Arc<Room>> {
        let state = self.state();
        if !state.is_current(session) {
            return Err(self.replaced_error());
        }
        state.room.clone().ok_or_else(|| NO_ROOM.to_owned())
    }

    pub(crate) fn check_can_enter(&self, session: u64) -> Reply {
        let state = self.state();
        if !state.is_current(session) {
            return Err(self.replaced_error());
        }
        if state.room.is_some() {
            return Err(ALREADY_IN_ROOM.to_owned());
        }
        Ok(())
    }
}
