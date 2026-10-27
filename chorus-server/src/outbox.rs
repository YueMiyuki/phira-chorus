use crate::stats::Stats;
use bytes::Bytes;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::mpsc, time};
use tokio_util::sync::CancellationToken;

const BATCH_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct Outbox {
    tx: mpsc::UnboundedSender<Bytes>,
    shared: Arc<Shared>,
}

/// Holds no sender, so the writer sees the channel close once every `Outbox` is gone
struct Shared {
    queued: AtomicUsize,
    lossy_limit: usize,
    limit: usize,
    overflowed: AtomicBool,
    close: CancellationToken,
    stats: Arc<Stats>,
}

pub struct OutboxWriter {
    rx: mpsc::UnboundedReceiver<Bytes>,
    shared: Arc<Shared>,
}

impl Outbox {
    pub fn new(
        lossy_limit: usize,
        limit: usize,
        close: CancellationToken,
        stats: Arc<Stats>,
    ) -> (Self, OutboxWriter) {
        let (tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            queued: AtomicUsize::new(0),
            lossy_limit,
            limit,
            overflowed: AtomicBool::new(false),
            close,
            stats,
        });
        let outbox = Self {
            tx,
            shared: Arc::clone(&shared),
        };
        (outbox, OutboxWriter { rx, shared })
    }

    pub fn send(&self, frame: Bytes) {
        let len = frame.len();
        let shared = &self.shared;
        let queued = shared.queued.fetch_add(len, Relaxed) + len;
        if queued > shared.limit {
            shared.queued.fetch_sub(len, Relaxed);
            if !shared.overflowed.swap(true, Relaxed) {
                Stats::add(&shared.stats.kicked, 1);
            }
            shared.close.cancel();
            return;
        }
        if self.tx.send(frame).is_err() {
            shared.queued.fetch_sub(len, Relaxed);
        }
    }

    pub fn send_lossy(&self, frame: Bytes) {
        let shared = &self.shared;
        if shared.queued.load(Relaxed) + frame.len() > shared.lossy_limit {
            Stats::add(&shared.stats.dropped_frames, 1);
            return;
        }
        self.send(frame);
    }

    pub fn overflowed(&self) -> bool {
        self.shared.overflowed.load(Relaxed)
    }
}

impl OutboxWriter {
    pub async fn run(mut self, mut wr: OwnedWriteHalf) {
        let close = self.shared.close.clone();
        let mut batch = Vec::new();
        loop {
            let first = tokio::select! {
                biased;
                _ = close.cancelled() => break,
                frame = self.rx.recv() => match frame {
                    Some(frame) => frame,
                    None => break,
                },
            };
            let mut frames = 1;
            let mut size = first.len();
            let ok = match self.rx.try_recv() {
                Err(_) => write(&mut wr, &first, &close).await,
                Ok(second) => {
                    batch.clear();
                    batch.extend_from_slice(&first);
                    batch.extend_from_slice(&second);
                    frames += 1;
                    size += second.len();
                    while batch.len() < BATCH_BYTES {
                        let Ok(frame) = self.rx.try_recv() else { break };
                        batch.extend_from_slice(&frame);
                        frames += 1;
                        size += frame.len();
                    }
                    let ok = write(&mut wr, &batch, &close).await;
                    if batch.capacity() > BATCH_BYTES {
                        batch = Vec::new();
                    }
                    ok
                }
            };
            self.shared.queued.fetch_sub(size, Relaxed);
            Stats::add(&self.shared.stats.frames_out, frames);
            Stats::add(&self.shared.stats.bytes_out, size as u64);
            if !ok {
                break;
            }
        }
        close.cancel();
        let _ = time::timeout(Duration::from_secs(1), wr.shutdown()).await;
    }
}

async fn write(wr: &mut OwnedWriteHalf, buf: &[u8], close: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = close.cancelled() => false,
        res = wr.write_all(buf) => res.is_ok(),
    }
}
