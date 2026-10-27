use chorus_client::Connection;
use chorus_proto::{ClientCommand, ServerCommand};
use std::{fmt::Write, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::Mutex,
    time::{self, Instant},
};

#[derive(Clone, Debug)]
pub struct ProbeConfig {
    pub server: SocketAddr,
    pub user_base: i32,
    pub duration: Duration,
    pub ping_every: Duration,
    pub login_every: Duration,
}

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Samples {
    ping_ms: Vec<f64>,
    connect_ms: Vec<f64>,
    login_ms: Vec<f64>,
    failures: Vec<String>,
}

pub async fn run(config: ProbeConfig) -> String {
    let samples = Arc::new(Mutex::new(Samples::default()));
    let started = Instant::now();
    let deadline = started + config.duration;
    let pinger = tokio::spawn(ping_loop(config.clone(), Arc::clone(&samples), deadline));
    let mut k = 0;
    let mut ticker = time::interval(config.login_every);
    while Instant::now() < deadline {
        ticker.tick().await;
        k += 1;
        let user = config.user_base + k;
        let res = time::timeout(TIMEOUT * 2, fresh_login(config.server, user)).await;
        let mut s = samples.lock().await;
        match res {
            Ok(Ok((connect, login))) => {
                s.connect_ms.push(connect);
                s.login_ms.push(login);
            }
            Ok(Err(err)) => s.failures.push(format!("login: {err}")),
            Err(_) => s.failures.push("login: no answer in 20s".to_owned()),
        }
    }
    let _ = pinger.await;
    let mut guard = samples.lock().await;
    let s = &mut *guard;
    let mut out = String::new();
    let _ = writeln!(out, "probe ran {:.0}s", started.elapsed().as_secs_f64());
    for (name, v) in [
        ("ping rtt", &mut s.ping_ms),
        ("connect", &mut s.connect_ms),
        ("login", &mut s.login_ms),
    ] {
        v.sort_by(f64::total_cmp);
        if v.is_empty() {
            let _ = writeln!(out, "{name:>9}: no samples");
            continue;
        }
        let at = |p: f64| v[((v.len() - 1) as f64 * p).round() as usize];
        let _ = writeln!(
            out,
            "{name:>9} (n={}): p50 {:.2}ms  p99 {:.2}ms  max {:.2}ms",
            v.len(),
            at(0.5),
            at(0.99),
            at(1.)
        );
    }
    let _ = writeln!(out, "failures: {}", s.failures.len());
    for failure in s.failures.iter().take(10) {
        let _ = writeln!(out, "  {failure}");
    }
    out
}

async fn ping_loop(config: ProbeConfig, samples: Arc<Mutex<Samples>>, deadline: Instant) {
    let mut conn = match login(config.server, config.user_base).await {
        Ok(conn) => conn,
        Err(err) => {
            samples
                .lock()
                .await
                .failures
                .push(format!("probe login: {err}"));
            return;
        }
    };
    let mut ticker = time::interval(config.ping_every);
    let mut window_max = 0f64;
    let mut window_start = Instant::now();
    while Instant::now() < deadline {
        ticker.tick().await;
        let sent = Instant::now();
        if let Err(err) = conn.send(&ClientCommand::Ping).await {
            samples.lock().await.failures.push(format!("ping: {err}"));
            return;
        }
        let pong = time::timeout(TIMEOUT, async {
            loop {
                if conn.recv().await? == ServerCommand::Pong {
                    return std::io::Result::Ok(());
                }
            }
        })
        .await;
        match pong {
            Ok(Ok(())) => {
                let ms = sent.elapsed().as_secs_f64() * 1000.;
                window_max = window_max.max(ms);
                samples.lock().await.ping_ms.push(ms);
            }
            Ok(Err(err)) => {
                samples.lock().await.failures.push(format!("ping: {err}"));
                return;
            }
            Err(_) => samples
                .lock()
                .await
                .failures
                .push("ping: no pong in 10s".to_owned()),
        }
        if window_start.elapsed() >= Duration::from_secs(5) {
            eprintln!("[probe] worst ping in the last 5s: {window_max:.1}ms");
            window_max = 0.;
            window_start = Instant::now();
        }
    }
}

async fn login(server: SocketAddr, user: i32) -> anyhow::Result<Connection> {
    let mut conn = time::timeout(TIMEOUT, Connection::connect(server)).await??;
    conn.send(&ClientCommand::Authenticate {
        token: format!("u{user}"),
    })
    .await?;
    loop {
        match time::timeout(TIMEOUT, conn.recv()).await?? {
            ServerCommand::Authenticate(Ok(_)) => return Ok(conn),
            ServerCommand::Authenticate(Err(err)) => anyhow::bail!(err),
            _ => {}
        }
    }
}

async fn fresh_login(server: SocketAddr, user: i32) -> anyhow::Result<(f64, f64)> {
    let start = Instant::now();
    let mut conn = time::timeout(TIMEOUT, Connection::connect(server)).await??;
    let connected = start.elapsed().as_secs_f64() * 1000.;
    let sent = Instant::now();
    conn.send(&ClientCommand::Authenticate {
        token: format!("u{user}"),
    })
    .await?;
    loop {
        match time::timeout(TIMEOUT, conn.recv()).await?? {
            ServerCommand::Authenticate(Ok(_)) => break,
            ServerCommand::Authenticate(Err(err)) => anyhow::bail!(err),
            _ => {}
        }
    }
    Ok((connected, sent.elapsed().as_secs_f64() * 1000.))
}
