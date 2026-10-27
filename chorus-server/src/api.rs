use crate::{config::Config, stats::Stats};
use anyhow::Result;
use lru::LruCache;
use parking_lot::Mutex;
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    hash::Hash,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, sync::watch, time};
use tracing::{debug, warn};

const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const RETRIES_PER_SEC: u32 = 50;
const RETRY_BURST: u32 = 100;

#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub language: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Chart {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Record {
    pub id: i32,
    pub player: i32,
    pub score: i32,
    pub accuracy: f32,
    pub full_combo: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    Unauthenticated,
    NotFound,
    Refused(u16),
    Status(u16),
    Timeout,
    Network,
    Decode,
    Busy,
}

impl ApiError {
    fn from_status(code: u16) -> Self {
        match code {
            404 => Self::NotFound,
            408 | 429 => Self::Status(code),
            400..=499 => Self::Refused(code),
            _ => Self::Status(code),
        }
    }

    fn from_reqwest(err: &reqwest::Error) -> Self {
        if err.is_timeout() {
            Self::Timeout
        } else if err.is_decode() {
            Self::Decode
        } else {
            Self::Network
        }
    }

    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Status(_) | Self::Timeout | Self::Network)
    }
}

#[derive(Deserialize)]
struct ErrorBody {
    code: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthenticated => f.write_str("invalid token"),
            Self::NotFound => f.write_str("not found"),
            Self::Refused(code) | Self::Status(code) => write!(f, "HTTP {code}"),
            Self::Timeout => f.write_str("request timed out"),
            Self::Network => f.write_str("network error"),
            Self::Decode => f.write_str("unexpected response"),
            Self::Busy => f.write_str("too many requests waiting"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Lookup<T>(watch::Receiver<Option<Result<T, ApiError>>>);

impl<T: Clone + Send + Sync + 'static> Lookup<T> {
    pub fn spawn(task: impl Future<Output = Result<T, ApiError>> + Send + 'static) -> Self {
        let (tx, rx) = watch::channel(None);
        tokio::spawn(async move {
            let _ = tx.send(Some(task.await));
        });
        Self(rx)
    }

    pub fn ready(result: Result<T, ApiError>) -> Self {
        Self(watch::channel(Some(result)).1)
    }

    pub async fn wait(mut self) -> Result<T, ApiError> {
        match self.0.wait_for(Option::is_some).await {
            Ok(result) => result.clone().expect("checked by wait_for"),
            Err(_) => Err(ApiError::Network),
        }
    }

    /// `None` if the lookup is still running after `limit`
    pub async fn wait_up_to(self, limit: Duration) -> Option<Result<T, ApiError>> {
        time::timeout(limit, self.wait()).await.ok()
    }
}

pub struct Api {
    http: reqwest::Client,
    base: String,
    login_slots: Semaphore,
    slots: Semaphore,
    retry_for: Duration,
    retries: Pacer,
    charts: Option<TtlCache<i32, Arc<Chart>>>,
    chart_lookups: Mutex<HashMap<i32, Lookup<Arc<Chart>>>>,
    stats: Arc<Stats>,
}

impl Api {
    pub fn new(config: &Config, stats: Arc<Stats>) -> Result<Self> {
        let concurrency = config.api_concurrency.max(1);
        let builder = || {
            reqwest::Client::builder()
                .user_agent(concat!("chorus-server/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(5))
                .pool_max_idle_per_host(concurrency * 2)
                .pool_idle_timeout(Duration::from_secs(300))
                .tcp_nodelay(true)
        };
        let http = match builder().build() {
            Ok(http) => http,
            Err(err) => {
                // Minimal systems may have no CA certificates
                let roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS
                    .iter()
                    .filter_map(|der| reqwest::Certificate::from_der(der).ok());
                let http = builder().tls_certs_only(roots).build().map_err(|_| err)?;
                warn!("no system CA certificates found, using bundled ones");
                http
            }
        };
        Ok(Self {
            http,
            base: config.api_url.trim_end_matches('/').to_owned(),
            login_slots: Semaphore::new(concurrency),
            slots: Semaphore::new(concurrency),
            retry_for: Duration::from_secs(config.api_retry_secs),
            retries: Pacer::new(RETRIES_PER_SEC, RETRY_BURST),
            charts: TtlCache::new(config.chart_cache_size, config.chart_cache_secs),
            chart_lookups: Mutex::default(),
            stats,
        })
    }

    pub(crate) async fn profile(&self, token: &str, retry: bool) -> Result<Profile, ApiError> {
        let retry = |err: &ApiError| retry && *err != ApiError::Unauthenticated;
        self.fetch(&self.login_slots, "/me", Some(token), retry)
            .await
    }

    pub(crate) fn chart(self: &Arc<Self>, id: i32) -> Lookup<Arc<Chart>> {
        if let Some(chart) = self.charts.as_ref().and_then(|it| it.get(&id)) {
            return Lookup::ready(Ok(chart));
        }
        let mut lookups = self.chart_lookups.lock();
        let lookup = lookups.entry(id).or_insert_with(|| {
            let api = Arc::clone(self);
            Lookup::spawn(async move {
                let path = format!("/chart/{id}");
                let res = api
                    .fetch(&api.slots, &path, None, ApiError::is_transient)
                    .await
                    .map(Arc::new);
                if let (Ok(chart), Some(cache)) = (&res, &api.charts) {
                    cache.put(id, Arc::clone(chart));
                }
                api.chart_lookups.lock().remove(&id);
                res
            })
        });
        lookup.clone()
    }

    pub(crate) fn record(self: &Arc<Self>, id: i32) -> Lookup<Record> {
        let api = Arc::clone(self);
        Lookup::spawn(async move {
            let path = format!("/record/{id}");
            api.fetch(&api.slots, &path, None, ApiError::is_transient)
                .await
        })
    }

    async fn fetch<T: DeserializeOwned>(
        &self,
        slots: &Semaphore,
        path: &str,
        token: Option<&str>,
        retry: impl Fn(&ApiError) -> bool,
    ) -> Result<T, ApiError> {
        let deadline = time::Instant::now() + self.retry_for;
        let mut backoff = FIRST_BACKOFF;
        loop {
            let res = time::timeout_at(deadline, self.attempt(slots, path, token))
                .await
                .unwrap_or(Err(ApiError::Timeout));
            match res {
                Err(err) if retry(&err) && time::Instant::now() + backoff < deadline => {
                    debug!(path, %err, "API request failed, retrying");
                    time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    if time::timeout_at(deadline, self.retries.wait())
                        .await
                        .is_err()
                    {
                        return Err(err);
                    }
                }
                res => return res,
            }
        }
    }

    async fn attempt<T: DeserializeOwned>(
        &self,
        slots: &Semaphore,
        path: &str,
        token: Option<&str>,
    ) -> Result<T, ApiError> {
        let _slot = slots.acquire().await.expect("semaphore is never closed");
        Stats::add(&self.stats.api_calls, 1);
        let mut req = self
            .http
            .get(format!("{}{path}", self.base))
            .timeout(ATTEMPT_TIMEOUT);
        if let Some(token) = token {
            req = req.bearer_auth(token);
        }
        let res = async {
            let resp = req.send().await.map_err(|it| ApiError::from_reqwest(&it))?;
            let status = resp.status();
            if status == reqwest::StatusCode::UNAUTHORIZED {
                // Only the API's own answer counts, not a proxy's
                let body = resp.json::<ErrorBody>().await;
                return Err(match body {
                    Ok(body) if body.code == "UNAUTHENTICATED" => ApiError::Unauthenticated,
                    _ => ApiError::Refused(401),
                });
            }
            if !status.is_success() {
                return Err(ApiError::from_status(status.as_u16()));
            }
            resp.json().await.map_err(|it| ApiError::from_reqwest(&it))
        }
        .await;
        if res.is_err() {
            Stats::add(&self.stats.api_errors, 1);
        }
        res
    }
}

struct Pacer {
    next: Mutex<time::Instant>,
    interval: Duration,
    burst: Duration,
}

impl Pacer {
    fn new(per_sec: u32, burst: u32) -> Self {
        let interval = Duration::from_secs(1) / per_sec.max(1);
        Self {
            next: Mutex::new(time::Instant::now()),
            interval,
            burst: interval * burst,
        }
    }

    async fn wait(&self) {
        let turn = {
            let mut next = self.next.lock();
            let now = time::Instant::now();
            let earliest = now.checked_sub(self.burst).unwrap_or(now);
            let turn = (*next).max(earliest);
            *next = turn + self.interval;
            turn
        };
        time::sleep_until(turn).await;
    }
}

struct TtlCache<K: Hash + Eq, V> {
    entries: Mutex<LruCache<K, (Instant, V)>>,
    ttl: Duration,
}

impl<K: Hash + Eq, V: Clone> TtlCache<K, V> {
    fn new(size: usize, ttl_secs: u64) -> Option<Self> {
        let size = NonZeroUsize::new(size)?;
        (ttl_secs > 0).then(|| Self {
            entries: Mutex::new(LruCache::new(size)),
            ttl: Duration::from_secs(ttl_secs),
        })
    }

    fn get(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.lock();
        match entries.get(key) {
            Some((at, value)) if at.elapsed() < self.ttl => Some(value.clone()),
            Some(_) => {
                entries.pop(key);
                None
            }
            None => None,
        }
    }

    fn put(&self, key: K, value: V) {
        self.entries.lock().put(key, (Instant::now(), value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pacer_spaces_turns_after_a_burst() {
        let pacer = Pacer::new(10, 5);
        time::advance(Duration::from_secs(10)).await;
        let start = time::Instant::now();
        for _ in 0..5 {
            pacer.wait().await;
        }
        assert_eq!(start.elapsed(), Duration::ZERO);
        for _ in 0..10 {
            pacer.wait().await;
        }
        assert_eq!(start.elapsed(), Duration::from_millis(900));
    }

    fn real_api() -> Arc<Api> {
        let config = Config::default();
        Arc::new(Api::new(&config, Arc::default()).unwrap())
    }

    /// `cargo test -p chorus-server -- --ignored`; set `PHIRA_TOKEN` to also check a login
    #[tokio::test]
    #[ignore = "needs network access to the Phira API"]
    async fn against_the_real_api() {
        let api = real_api();
        let chart = api.chart(1000).wait().await.unwrap();
        assert_eq!(chart.id, 1000);
        assert!(!chart.name.is_empty());
        assert_eq!(api.chart(-1).wait().await.unwrap_err(), ApiError::NotFound);

        let record = api.record(20_000_000).wait().await.unwrap();
        assert_eq!((record.id, record.player), (20_000_000, 446_855));

        let invalid = api.profile("0123456789abcdef0123456789abcdef", true).await;
        assert_eq!(invalid.unwrap_err(), ApiError::Unauthenticated);

        if let Ok(token) = std::env::var("PHIRA_TOKEN") {
            let me = api.profile(&token, true).await.unwrap();
            println!(
                "logged in as {} ({}), language {:?}",
                me.name, me.id, me.language
            );
        }
    }
}
