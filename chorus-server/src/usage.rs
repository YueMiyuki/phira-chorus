//! CPU and memory use of the server and of the machine it runs on

use parking_lot::Mutex;
use serde::Serialize;
use std::{convert::Infallible, time::Duration};
use sysinfo::{
    MINIMUM_CPU_UPDATE_INTERVAL, Pid, ProcessRefreshKind, ProcessesToUpdate, System,
    get_current_pid,
};
use tokio::time;
const SAMPLE_EVERY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Default)]
pub(crate) struct Usage {
    pub cpu: Cpu,
    pub ram: Ram,
}

/// Percent of the whole machine, all cores together
#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct Cpu {
    pub process: f32,
    pub system: f32,
}

/// Bytes
#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct Ram {
    pub process: u64,
    pub used: u64,
    pub total: u64,
}

pub(crate) struct Sampler {
    system: System,
    pid: Option<Pid>,
}

impl Sampler {
    pub fn new() -> Self {
        Self {
            system: System::new(),
            pid: get_current_pid().ok(),
        }
    }

    /// CPU use since the previous sample; the first one reports none
    pub fn sample(&mut self) -> Usage {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        if let Some(pid) = self.pid {
            self.system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                false,
                ProcessRefreshKind::nothing().with_cpu().with_memory(),
            );
        }
        let process = self.pid.and_then(|pid| self.system.process(pid));
        // A process can use up to 100% of each core
        let cores = self.system.cpus().len().max(1) as f32;
        Usage {
            cpu: Cpu {
                process: round(process.map_or(0., |it| it.cpu_usage()) / cores),
                system: round(self.system.global_cpu_usage()),
            },
            ram: Ram {
                process: process.map_or(0, |it| it.memory()),
                used: self.system.used_memory(),
                total: self.system.total_memory(),
            },
        }
    }

    /// Keeps `latest` up to date; never returns
    pub async fn run(mut self, latest: &Mutex<Usage>) -> Infallible {
        time::sleep(MINIMUM_CPU_UPDATE_INTERVAL).await;
        let mut ticker = time::interval(SAMPLE_EVERY);
        loop {
            ticker.tick().await;
            *latest.lock() = self.sample();
        }
    }
}

fn round(percent: f32) -> f32 {
    (percent * 10.).round() / 10.
}
