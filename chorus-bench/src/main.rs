use anyhow::{Context, Result};
use chorus_bench::{
    load::{self, LoadConfig},
    mock_api::MockApi,
    probe::{self, ProbeConfig},
};
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, time::Duration};
use tokio::net::lookup_host;

#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Serve a stand-in for the Phira API")]
    MockApi {
        #[arg(long, default_value = "127.0.0.1:18080")]
        listen: SocketAddr,
        #[arg(long, default_value_t = 20, help = "Delay added to every response")]
        latency_ms: u64,
        #[arg(
            long,
            default_value_t = 0,
            help = "Requests served at once, more queue up (0: no limit)"
        )]
        concurrency: usize,
    },
    #[command(about = "Print a server config matching a load run")]
    Config {
        #[command(flatten)]
        shape: Shape,
        #[arg(long, default_value = "http://127.0.0.1:18080")]
        api_url: String,
    },
    #[command(about = "Simulate players against a running server")]
    Load {
        #[arg(long, default_value = "127.0.0.1:12346")]
        server: String,
        #[command(flatten)]
        shape: Shape,
        #[arg(
            long,
            default_value_t = 20,
            help = "Seconds over which connections are spread"
        )]
        ramp: u64,
        #[arg(
            long,
            default_value_t = 60,
            help = "Seconds to keep playing after the ramp"
        )]
        duration: u64,
        #[arg(long, default_value_t = 20, help = "Seconds per song")]
        song: u64,
        #[arg(long, default_value_t = 2, help = "Seconds between rounds")]
        pause: u64,
        #[arg(
            long,
            default_value_t = 10.,
            help = "Touch batches per second while playing"
        )]
        touch_rate: f64,
        #[arg(
            long,
            default_value_t = 5.,
            help = "Judgement batches per second while playing"
        )]
        judge_rate: f64,
        #[arg(long, help = "Reconnect after a lost connection or failed login")]
        reconnect: bool,
        #[arg(
            long,
            default_value_t = 0,
            help = "Seconds between progress lines (0: off)"
        )]
        progress: u64,
    },
    #[command(about = "Measure ping and fresh login times with a single client")]
    Probe {
        #[arg(long, default_value = "127.0.0.1:12346")]
        server: String,
        #[arg(long, default_value_t = 990_000)]
        user_base: i32,
        #[arg(long, default_value_t = 60, help = "Seconds to run")]
        duration: u64,
        #[arg(long, default_value_t = 200)]
        ping_ms: u64,
        #[arg(long, default_value_t = 1000)]
        login_ms: u64,
    },
}

#[derive(clap::Args)]
struct Shape {
    #[arg(long, default_value_t = 5000)]
    players: usize,
    #[arg(long, default_value_t = 8)]
    room_size: usize,
    #[arg(long, help = "Add a monitor to every room, so live data is relayed")]
    monitors: bool,
    #[arg(long, default_value_t = 100_000)]
    id_base: i32,
    #[arg(long, default_value_t = 900_000)]
    monitor_id_base: i32,
}

impl Shape {
    fn config(&self, server: SocketAddr) -> LoadConfig {
        LoadConfig {
            server,
            players: self.players,
            room_size: self.room_size.max(1),
            monitors: self.monitors,
            id_base: self.id_base,
            monitor_id_base: self.monitor_id_base,
            ramp: Duration::ZERO,
            duration: Duration::ZERO,
            song: Duration::ZERO,
            pause: Duration::ZERO,
            touch_rate: 0.,
            judge_rate: 0.,
            reconnect: false,
            progress: Duration::ZERO,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    raise_fd_limit();
    match Args::parse().command {
        Command::MockApi {
            listen,
            latency_ms,
            concurrency,
        } => {
            let latency = Duration::from_millis(latency_ms);
            let api = MockApi::spawn_limited(listen, latency, concurrency).await?;
            println!("mock API listening on {}", api.url());
            tokio::signal::ctrl_c().await?;
        }
        Command::Config { shape, api_url } => {
            let config = shape.config(SocketAddr::from(([127, 0, 0, 1], 0)));
            println!("api_url: {api_url}");
            println!("max_connections: {}", (config.players + config.rooms()) * 2);
            println!("stats_interval_secs: 10");
            println!("monitors: {:?}", config.monitor_ids());
        }
        Command::Load {
            server,
            shape,
            ramp,
            duration,
            song,
            pause,
            touch_rate,
            judge_rate,
            reconnect,
            progress,
        } => {
            let addr = lookup_host(&server)
                .await?
                .next()
                .context("server address did not resolve")?;
            let config = LoadConfig {
                ramp: Duration::from_secs(ramp),
                duration: Duration::from_secs(duration),
                song: Duration::from_secs(song),
                pause: Duration::from_secs(pause),
                touch_rate,
                judge_rate,
                reconnect,
                progress: Duration::from_secs(progress),
                ..shape.config(addr)
            };
            println!(
                "starting {} players in {} rooms against {addr}",
                config.players,
                config.rooms()
            );
            print!("{}", load::run(config).await);
        }
        Command::Probe {
            server,
            user_base,
            duration,
            ping_ms,
            login_ms,
        } => {
            let server = lookup_host(&server)
                .await?
                .next()
                .context("server address did not resolve")?;
            let config = ProbeConfig {
                server,
                user_base,
                duration: Duration::from_secs(duration),
                ping_every: Duration::from_millis(ping_ms),
                login_every: Duration::from_millis(login_ms),
            };
            print!("{}", probe::run(config).await);
        }
    }
    Ok(())
}

fn raise_fd_limit() {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid rlimit for both calls
        unsafe {
            if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
                limit.rlim_cur = limit.rlim_max.min(1 << 20);
                libc::setrlimit(libc::RLIMIT_NOFILE, &limit);
            }
        }
    }
}
