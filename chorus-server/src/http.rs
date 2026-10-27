//! HTTP API. `/api` is a guide to it, open to anyone; routes under `/v1`
//! need a token from `http_tokens`

use crate::{
    server::Shared,
    usage::{Cpu, Ram, Sampler, Usage},
};
use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::{info, warn};

const GUIDE: &str = include_str!("http_guide.txt");

#[derive(Serialize)]
struct Rooms {
    rooms: Vec<RoomStatus>,
}

#[derive(Serialize)]
pub(crate) struct RoomStatus {
    pub roomid: String,
    pub cycle: bool,
    pub lock: bool,
    pub host: Named<String>,
    pub state: &'static str,
    pub chart: Option<Named<String>>,
    pub players: Vec<Named<i32>>,
}

#[derive(Serialize)]
pub(crate) struct Named<Id> {
    pub name: String,
    pub id: Id,
}

#[derive(Serialize)]
struct Status {
    status: &'static str,
    players: usize,
    cpu: Cpu,
    ram: Ram,
}

struct Ctx {
    shared: Arc<Shared>,
    /// Tokens are compared by SHA-256, so the time a mismatch takes says
    /// nothing about how much of a token was right
    tokens: Vec<([u8; 32], String)>,
    usage: Mutex<Usage>,
}

impl Ctx {
    /// The name of the token the request carries, if it is a known one
    fn authorize(&self, headers: &HeaderMap) -> Option<&str> {
        let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
        let (scheme, token) = value.split_once(' ')?;
        let token = token.trim();
        if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
            return None;
        }
        let digest = sha256(token);
        self.tokens
            .iter()
            .find(|(it, _)| *it == digest)
            .map(|(_, name)| name.as_str())
    }
}

pub async fn serve_http(listener: TcpListener, shared: Arc<Shared>) -> io::Result<()> {
    let mut tokens = Vec::new();
    for (name, token) in &shared.config.http_tokens {
        if token.is_empty() {
            warn!(token = %name, "empty HTTP token is ignored");
        } else {
            tokens.push((sha256(token), name.clone()));
        }
    }
    if tokens.is_empty() {
        warn!("no http_tokens set, every HTTP request will be refused");
    }
    let mut sampler = Sampler::new();
    let ctx = Arc::new(Ctx {
        shared,
        tokens,
        usage: Mutex::new(sampler.sample()),
    });
    let v1 = Router::new()
        .route("/rooms", get(rooms))
        .route("/status", get(status));
    let app = Router::new()
        .route("/api", get(guide))
        .route("/api/", get(guide))
        .nest("/v1", v1)
        .fallback(not_found)
        .with_state(Arc::clone(&ctx));
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    );
    tokio::select! {
        res = serve => res,
        never = sampler.run(&ctx.usage) => match never {},
    }
}

async fn guide() -> &'static str {
    GUIDE
}

async fn not_found() -> (StatusCode, &'static str) {
    (
        StatusCode::NOT_FOUND,
        "Not found. GET /api for a guide to the routes\n",
    )
}

async fn rooms(
    State(ctx): State<Arc<Ctx>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let Some(name) = ctx.authorize(&headers) else {
        return refuse(peer);
    };
    let rooms = ctx.shared.room_statuses();
    info!(token = %name, %peer, rooms = rooms.len(), "rooms requested");
    Json(Rooms { rooms }).into_response()
}

async fn status(
    State(ctx): State<Arc<Ctx>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let Some(name) = ctx.authorize(&headers) else {
        return refuse(peer);
    };
    info!(token = %name, %peer, "status requested");
    let usage = *ctx.usage.lock();
    Json(Status {
        status: "ok",
        players: ctx.shared.online_users(),
        cpu: usage.cpu,
        ram: usage.ram,
    })
    .into_response()
}

fn refuse(peer: SocketAddr) -> Response {
    warn!(%peer, "HTTP request refused, missing or unknown token");
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
    )
        .into_response()
}

fn sha256(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}
