use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default)]
pub struct Stats {
    pub connections: AtomicU64,
    pub accepted: AtomicU64,
    pub logins: AtomicU64,
    pub login_failures: AtomicU64,
    pub remembered_logins: AtomicU64,
    pub abandoned_logins: AtomicU64,
    pub revoked_logins: AtomicU64,
    pub frames_in: AtomicU64,
    pub bytes_in: AtomicU64,
    pub frames_out: AtomicU64,
    pub bytes_out: AtomicU64,
    pub dropped_frames: AtomicU64,
    pub kicked: AtomicU64,
    pub api_calls: AtomicU64,
    pub api_errors: AtomicU64,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub accepted: u64,
    pub logins: u64,
    pub login_failures: u64,
    pub remembered_logins: u64,
    pub abandoned_logins: u64,
    pub revoked_logins: u64,
    pub frames_in: u64,
    pub bytes_in: u64,
    pub frames_out: u64,
    pub bytes_out: u64,
    pub dropped_frames: u64,
    pub kicked: u64,
    pub api_calls: u64,
    pub api_errors: u64,
}

impl Stats {
    pub(crate) fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            accepted: self.accepted.load(Relaxed),
            logins: self.logins.load(Relaxed),
            login_failures: self.login_failures.load(Relaxed),
            remembered_logins: self.remembered_logins.load(Relaxed),
            abandoned_logins: self.abandoned_logins.load(Relaxed),
            revoked_logins: self.revoked_logins.load(Relaxed),
            frames_in: self.frames_in.load(Relaxed),
            bytes_in: self.bytes_in.load(Relaxed),
            frames_out: self.frames_out.load(Relaxed),
            bytes_out: self.bytes_out.load(Relaxed),
            dropped_frames: self.dropped_frames.load(Relaxed),
            kicked: self.kicked.load(Relaxed),
            api_calls: self.api_calls.load(Relaxed),
            api_errors: self.api_errors.load(Relaxed),
        }
    }
}

impl Snapshot {
    pub(crate) fn since(&self, earlier: &Self) -> Self {
        Self {
            accepted: self.accepted - earlier.accepted,
            logins: self.logins - earlier.logins,
            login_failures: self.login_failures - earlier.login_failures,
            remembered_logins: self.remembered_logins - earlier.remembered_logins,
            abandoned_logins: self.abandoned_logins - earlier.abandoned_logins,
            revoked_logins: self.revoked_logins - earlier.revoked_logins,
            frames_in: self.frames_in - earlier.frames_in,
            bytes_in: self.bytes_in - earlier.bytes_in,
            frames_out: self.frames_out - earlier.frames_out,
            bytes_out: self.bytes_out - earlier.bytes_out,
            dropped_frames: self.dropped_frames - earlier.dropped_frames,
            kicked: self.kicked - earlier.kicked,
            api_calls: self.api_calls - earlier.api_calls,
            api_errors: self.api_errors - earlier.api_errors,
        }
    }
}
