use crate::{
    api::{Api, ApiError, Lookup, Profile},
    config::Config,
    stats::Stats,
};
use lru::LruCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs, io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn};

const MAX_LOOKUPS: usize = 16_384;
const STORE_VERSION: u32 = 1;

type Key = [u8; 32];

#[derive(Clone)]
struct Known {
    profile: Arc<Profile>,
    verified: u64,
}

pub(crate) enum Admission {
    Known(Arc<Profile>),
    Recheck(Arc<Profile>, Lookup<Arc<Profile>>),
    Lookup(Lookup<Arc<Profile>>),
}

pub(crate) struct Logins {
    api: Arc<Api>,
    stats: Arc<Stats>,
    known: Option<Mutex<LruCache<Key, Known>>>,
    lookups: Mutex<HashMap<Key, Lookup<Arc<Profile>>>>,
    fresh: u64,
    keep: u64,
    path: Option<PathBuf>,
    dirty: AtomicBool,
    saving: Mutex<()>,
}

impl Logins {
    pub fn new(config: &Config, api: Arc<Api>, stats: Arc<Stats>) -> Self {
        let fresh = config.login_cache_secs;
        let keep = fresh.max(config.login_remember_days * 86_400);
        let known = NonZeroUsize::new(config.login_cache_size)
            .filter(|_| keep > 0)
            .map(LruCache::new);
        let path = (!config.login_store.is_empty() && known.is_some())
            .then(|| PathBuf::from(&config.login_store));
        let logins = Self {
            api,
            stats,
            known: known.map(Mutex::new),
            lookups: Mutex::default(),
            fresh,
            keep,
            path,
            dirty: AtomicBool::new(false),
            saving: Mutex::new(()),
        };
        logins.load();
        logins
    }

    pub fn persistent(&self) -> bool {
        self.path.is_some()
    }

    pub fn admit(self: &Arc<Self>, token: &str) -> Admission {
        let key: Key = Sha256::digest(token.as_bytes()).into();
        let now = unix_now();
        let known = self.known.as_ref().and_then(|known| {
            let mut known = known.lock();
            match known.get(&key) {
                Some(entry) if now.saturating_sub(entry.verified) < self.keep => {
                    Some(entry.clone())
                }
                Some(_) => {
                    known.pop(&key);
                    None
                }
                None => None,
            }
        });
        match known {
            Some(known) if now.saturating_sub(known.verified) < self.fresh => {
                Admission::Known(known.profile)
            }
            Some(known) => {
                Stats::add(&self.stats.remembered_logins, 1);
                Admission::Recheck(known.profile, self.lookup(key, token, false))
            }
            None => Admission::Lookup(self.lookup(key, token, true)),
        }
    }

    pub fn check(self: &Arc<Self>, token: &str, retry: bool) -> Lookup<Arc<Profile>> {
        self.lookup(Sha256::digest(token.as_bytes()).into(), token, retry)
    }

    fn lookup(self: &Arc<Self>, key: Key, token: &str, retry: bool) -> Lookup<Arc<Profile>> {
        let mut lookups = self.lookups.lock();
        if let Some(lookup) = lookups.get(&key) {
            return lookup.clone();
        }
        if lookups.len() >= MAX_LOOKUPS {
            return Lookup::ready(Err(ApiError::Busy));
        }
        let logins = Arc::clone(self);
        let token = token.to_owned();
        let lookup = Lookup::spawn(async move {
            let res = logins.api.profile(&token, retry).await.map(Arc::new);
            logins.settle(key, &res);
            logins.lookups.lock().remove(&key);
            res
        });
        lookups.insert(key, lookup.clone());
        lookup
    }

    fn settle(&self, key: Key, res: &Result<Arc<Profile>, ApiError>) {
        let Some(known) = &self.known else { return };
        match res {
            Ok(profile) => {
                let entry = Known {
                    profile: Arc::clone(profile),
                    verified: unix_now(),
                };
                known.lock().put(key, entry);
                self.dirty.store(true, Relaxed);
            }
            Err(ApiError::Unauthenticated) => {
                if known.lock().pop(&key).is_some() {
                    self.dirty.store(true, Relaxed);
                }
            }
            Err(_) => {}
        }
    }

    fn load(&self) {
        let (Some(path), Some(known)) = (&self.path, &self.known) else {
            return;
        };
        let text = match fs::read(path) {
            Ok(text) => text,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return,
            Err(err) => {
                warn!(path = %path.display(), %err, "could not read remembered logins");
                return;
            }
        };
        let file = match serde_json::from_slice::<StoreFile>(&text) {
            Ok(file) if file.version == STORE_VERSION => file,
            Ok(_) | Err(_) => {
                warn!(path = %path.display(), "ignoring unreadable remembered logins");
                return;
            }
        };
        let now = unix_now();
        let mut known = known.lock();
        for entry in file.logins {
            let Some(key) = decode_hex(&entry.token_sha256) else {
                continue;
            };
            if now.saturating_sub(entry.verified) >= self.keep {
                continue;
            }
            let profile = Profile {
                id: entry.id,
                name: entry.name,
                language: entry.language,
            };
            known.put(
                key,
                Known {
                    profile: Arc::new(profile),
                    verified: entry.verified,
                },
            );
        }
        info!(path = %path.display(), count = known.len(), "loaded remembered logins");
    }

    pub fn save(&self) -> io::Result<()> {
        let (Some(path), Some(known)) = (&self.path, &self.known) else {
            return Ok(());
        };
        let _saving = self.saving.lock();
        if !self.dirty.swap(false, Relaxed) {
            return Ok(());
        }
        let now = unix_now();
        // Oldest first, so loading restores the eviction order
        let entries: Vec<(Key, Known)> = known
            .lock()
            .iter()
            .rev()
            .filter(|(_, it)| now.saturating_sub(it.verified) < self.keep)
            .map(|(key, it)| (*key, it.clone()))
            .collect();
        let file = StoreFile {
            version: STORE_VERSION,
            logins: entries
                .into_iter()
                .map(|(key, it)| StoredLogin {
                    token_sha256: encode_hex(&key),
                    id: it.profile.id,
                    name: it.profile.name.clone(),
                    language: it.profile.language.clone(),
                    verified: it.verified,
                })
                .collect(),
        };
        let res = write_atomically(path, &serde_json::to_vec(&file)?);
        if res.is_err() {
            self.dirty.store(true, Relaxed);
        }
        res
    }
}

#[derive(Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    logins: Vec<StoredLogin>,
}

#[derive(Serialize, Deserialize)]
struct StoredLogin {
    token_sha256: String,
    id: i32,
    name: String,
    language: String,
    verified: u64,
}

fn write_atomically(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&tmp)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, path)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |it| it.as_secs())
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

fn decode_hex(text: &str) -> Option<Key> {
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    let nibble = |c: u8| (c as char).to_digit(16).map(|it| it as u8);
    let mut key = [0; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = (nibble(bytes[2 * i])? << 4) | nibble(bytes[2 * i + 1])?;
    }
    Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let key: Key = Sha256::digest(b"token").into();
        assert_eq!(decode_hex(&encode_hex(&key)), Some(key));
        assert_eq!(decode_hex("zz"), None);
        assert_eq!(decode_hex(&"g".repeat(64)), None);
    }
}
