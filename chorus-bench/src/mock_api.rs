//! Stand-in for the Phira API: token `u<id>` or `u<id>-<language>` is user `<id>`

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinHandle, time};

#[derive(Default)]
pub struct Counters {
    pub me: AtomicU64,
    pub chart: AtomicU64,
    pub record: AtomicU64,
}

struct Ctx {
    latency_ms: AtomicU64,
    slots: Option<Semaphore>,
    revoked: Mutex<HashSet<i32>>,
    failures: Mutex<(u64, StatusCode)>,
    counters: Arc<Counters>,
}

pub struct MockApi {
    pub addr: SocketAddr,
    pub counters: Arc<Counters>,
    ctx: Arc<Ctx>,
    task: JoinHandle<()>,
}

impl MockApi {
    pub async fn spawn(addr: SocketAddr, latency: Duration) -> std::io::Result<Self> {
        Self::spawn_limited(addr, latency, 0).await
    }

    pub async fn spawn_limited(
        addr: SocketAddr,
        latency: Duration,
        concurrency: usize,
    ) -> std::io::Result<Self> {
        let socket = if addr.is_ipv4() {
            tokio::net::TcpSocket::new_v4()?
        } else {
            tokio::net::TcpSocket::new_v6()?
        };
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        let listener: TcpListener = socket.listen(4096)?;
        let addr = listener.local_addr()?;
        let counters = Arc::new(Counters::default());
        let ctx = Arc::new(Ctx {
            latency_ms: AtomicU64::new(latency.as_millis() as u64),
            slots: (concurrency > 0).then(|| Semaphore::new(concurrency)),
            revoked: Mutex::default(),
            failures: Mutex::new((0, StatusCode::SERVICE_UNAVAILABLE)),
            counters: Arc::clone(&counters),
        });
        let app = Router::new()
            .route("/me", get(me))
            .route("/chart/{id}", get(chart))
            .route("/record/{id}", get(record))
            .with_state(Arc::clone(&ctx));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            addr,
            counters,
            ctx,
            task,
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn set_latency(&self, latency: Duration) {
        self.ctx
            .latency_ms
            .store(latency.as_millis() as u64, Relaxed);
    }

    pub fn revoke(&self, id: i32) {
        self.ctx.revoked.lock().unwrap().insert(id);
    }

    pub fn fail_next(&self, n: u64, status: u16) {
        let status = StatusCode::from_u16(status).expect("valid status");
        *self.ctx.failures.lock().unwrap() = (n, status);
    }
}

impl Drop for MockApi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(ctx: &Ctx) -> Result<(), StatusCode> {
    let _slot = match &ctx.slots {
        Some(slots) => Some(slots.acquire().await.expect("never closed")),
        None => None,
    };
    let latency = ctx.latency_ms.load(Relaxed);
    if latency > 0 {
        time::sleep(Duration::from_millis(latency)).await;
    }
    let mut failures = ctx.failures.lock().unwrap();
    if failures.0 > 0 {
        failures.0 -= 1;
        return Err(failures.1);
    }
    Ok(())
}

async fn me(State(ctx): State<Arc<Ctx>>, headers: HeaderMap) -> Response {
    ctx.counters.me.fetch_add(1, Relaxed);
    if let Err(status) = serve(&ctx).await {
        return status.into_response();
    }
    let invalid = || {
        let body = json!({ "code": "UNAUTHENTICATED", "error": "invalid token" });
        (StatusCode::UNAUTHORIZED, Json(body)).into_response()
    };
    let Some(token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|it| it.to_str().ok())
        .and_then(|it| it.strip_prefix("Bearer "))
        .and_then(|it| it.strip_prefix('u'))
    else {
        return invalid();
    };
    let (id, language) = token.split_once('-').unwrap_or((token, "en-US"));
    let Ok(id) = id.parse::<i32>() else {
        return invalid();
    };
    if ctx.revoked.lock().unwrap().contains(&id) {
        return invalid();
    }
    Json(json!({
        "id": id,
        "name": format!("user{id}"),
        "language": language,
    }))
    .into_response()
}

async fn chart(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<i32>,
) -> Result<Json<Value>, StatusCode> {
    ctx.counters.chart.fetch_add(1, Relaxed);
    serve(&ctx).await?;
    if id < 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(json!({ "id": id, "name": format!("Chart {id}") })))
}

async fn record(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<i32>,
) -> Result<Json<Value>, StatusCode> {
    ctx.counters.record.fetch_add(1, Relaxed);
    serve(&ctx).await?;
    Ok(Json(json!({
        "id": id,
        "player": id,
        "score": 900_000 + id.rem_euclid(100_000),
        "perfect": 100,
        "good": 1,
        "bad": 0,
        "miss": 0,
        "max_combo": 101,
        "accuracy": 0.995,
        "full_combo": id % 2 == 0,
        "std": 12.5,
        "std_score": 0.9,
    })))
}
