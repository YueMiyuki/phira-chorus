use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashSet},
    net::{IpAddr, SocketAddr},
    path::Path,
    time::Duration,
};
use tracing::info;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub port: u16,
    pub monitors: Vec<i32>,
    pub api_url: String,
    pub room_max_users: usize,
    pub max_connections: usize,
    pub reconnect_grace_secs: u64,
    pub playing_grace_secs: u64,
    pub login_timeout_secs: u64,
    pub api_wait_secs: u64,
    pub api_retry_secs: u64,
    pub api_concurrency: usize,
    pub chart_cache_secs: u64,
    pub chart_cache_size: usize,
    pub login_cache_secs: u64,
    pub login_remember_days: u64,
    pub login_cache_size: usize,
    pub login_store: String,
    pub outbox_lossy_limit: usize,
    pub outbox_limit: usize,
    pub command_rate: u32,
    pub command_burst: u32,
    pub stats_interval_secs: u64,
    pub http_addr: String,
    /// Token name to token
    pub http_tokens: BTreeMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 12346,
            monitors: vec![2],
            api_url: "https://phira.5wyxi.com".to_owned(),
            room_max_users: 8,
            max_connections: 30_000,
            reconnect_grace_secs: 10,
            playing_grace_secs: 10,
            login_timeout_secs: 30,
            api_wait_secs: 6,
            api_retry_secs: 60,
            api_concurrency: 128,
            chart_cache_secs: 600,
            chart_cache_size: 4096,
            login_cache_secs: 60,
            login_remember_days: 30,
            login_cache_size: 100_000,
            login_store: "logins.json".to_owned(),
            outbox_lossy_limit: 256 * 1024,
            outbox_limit: 4 * 1024 * 1024,
            command_rate: 100,
            command_burst: 400,
            stats_interval_secs: 60,
            http_addr: String::new(),
            http_tokens: BTreeMap::new(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => serde_yaml::from_str(&text)
                .with_context(|| format!("invalid config file {}", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                info!("{} not found, using default settings", path.display());
                Ok(Self::default())
            }
            Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
        }
    }

    pub(crate) fn monitor_set(&self) -> HashSet<i32> {
        self.monitors.iter().copied().collect()
    }

    pub(crate) fn reconnect_grace(&self) -> Duration {
        Duration::from_secs(self.reconnect_grace_secs)
    }

    pub(crate) fn playing_grace(&self) -> Duration {
        Duration::from_secs(self.playing_grace_secs)
    }

    pub(crate) fn login_timeout(&self) -> Duration {
        Duration::from_secs(self.login_timeout_secs)
    }

    pub(crate) fn api_wait(&self) -> Duration {
        Duration::from_secs(self.api_wait_secs)
    }

    /// `None` when the HTTP API is off. `:<port>` is that port on `game`, the
    /// address the game server listens on
    pub(crate) fn http_addr(&self, game: IpAddr) -> Result<Option<SocketAddr>> {
        if self.http_addr.is_empty() {
            return Ok(None);
        }
        let addr = match self.http_addr.strip_prefix(':') {
            Some(port) => port.parse().ok().map(|port| SocketAddr::new(game, port)),
            None => self.http_addr.parse().ok(),
        };
        addr.map(Some)
            .with_context(|| format!("invalid http_addr {:?}", self.http_addr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_matches_defaults() {
        let example: Config =
            serde_yaml::from_str(include_str!("../../server_config.example.yml")).unwrap();
        assert_eq!(format!("{example:?}"), format!("{:?}", Config::default()));
    }

    #[test]
    fn http_addr_port_alone_uses_the_game_address() {
        let game: IpAddr = "::".parse().unwrap();
        let addr = |text: &str| {
            let config = Config {
                http_addr: text.to_owned(),
                ..Config::default()
            };
            config.http_addr(game)
        };
        assert_eq!(addr("").unwrap(), None);
        assert_eq!(addr(":12347").unwrap(), Some("[::]:12347".parse().unwrap()));
        assert_eq!(
            addr("127.0.0.1:8080").unwrap(),
            Some("127.0.0.1:8080".parse().unwrap())
        );
        for bad in [":", ":http", "12347", "localhost:12347"] {
            assert!(addr(bad).is_err(), "{bad}");
        }
    }
}
