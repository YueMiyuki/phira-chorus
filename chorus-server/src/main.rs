use anyhow::Result;
use chorus_server::Config;
use clap::Parser;
use std::{
    io::IsTerminal,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
};
use tracing::{info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(
        short,
        long,
        help = "Port to listen on [default: `port` from the config, 12346]"
    )]
    port: Option<u16>,
    #[arg(
        long,
        default_value_t = IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        help = "Address to bind; the default accepts IPv6 and IPv4 clients"
    )]
    bind: IpAddr,
    #[arg(
        short,
        long,
        default_value = "server_config.yml",
        help = "YAML config file; a missing file means default settings"
    )]
    config: PathBuf,
    #[arg(long, help = "Runtime worker threads [default: CPU cores]")]
    workers: Option<usize>,
    #[arg(long, help = "Also write hourly-rotated log files to this directory")]
    log_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let _log_guard = init_logging(args.log_dir.as_deref())?;
    match raise_fd_limit() {
        Ok(limit) => info!(limit, "open file limit"),
        Err(err) => warn!(%err, "could not raise the open file limit"),
    }
    let config = Config::load(&args.config)?;

    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();
    if let Some(workers) = args.workers {
        runtime.worker_threads(workers);
    }
    runtime.build()?.block_on(async move {
        let addr = SocketAddr::new(args.bind, args.port.unwrap_or(config.port));
        let listener = chorus_server::bind(addr)?;
        info!(%addr, api = %config.api_url, "listening");
        chorus_server::serve(listener, config, shutdown_signal()).await
    })
}

fn init_logging(dir: Option<&Path>) -> Result<Option<WorkerGuard>> {
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout = fmt::layer()
        .with_ansi(std::io::stdout().is_terminal())
        .with_filter(filter());
    let (file, guard) = match dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            let appender = tracing_appender::rolling::hourly(dir, "chorus-server.log");
            let (writer, guard) = tracing_appender::non_blocking(appender);
            let layer = fmt::layer()
                .with_ansi(false)
                .with_writer(writer)
                .with_filter(filter());
            (Some(layer), Some(guard))
        }
        None => (None, None),
    };
    tracing_subscriber::registry()
        .with(stdout)
        .with(file)
        .init();
    Ok(guard)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down");
}

#[cfg(unix)]
fn raise_fd_limit() -> std::io::Result<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid, writable rlimit
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let target = max_soft_limit(limit.rlim_max);
    if limit.rlim_cur < target {
        limit.rlim_cur = target;
        // SAFETY: `limit` is a valid rlimit
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    #[allow(clippy::unnecessary_cast)] // `rlim_t` is narrower on some targets
    Ok(limit.rlim_cur as u64)
}

#[cfg(target_os = "macos")]
fn max_soft_limit(hard: libc::rlim_t) -> libc::rlim_t {
    let mut per_proc: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated and the output buffer matches `size`
    let ok = unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            (&mut per_proc as *mut libc::c_int).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } == 0;
    if ok && per_proc > 0 {
        hard.min(per_proc as libc::rlim_t)
    } else {
        hard
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn max_soft_limit(hard: libc::rlim_t) -> libc::rlim_t {
    hard
}

#[cfg(not(unix))]
fn raise_fd_limit() -> std::io::Result<u64> {
    Ok(0)
}
