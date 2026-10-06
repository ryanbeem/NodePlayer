//! Shared network clock.
//!
//! Each node measures time on its own monotonic clock. Followers estimate the
//! offset between their clock and the leader's with an NTP-style exchange over
//! UDP, and the leader's clock is the "shared" clock every timeline refers to.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::watch;

/// How many recent samples to keep when picking the best offset.
const WINDOW: usize = 16;
/// How often a follower pings the leader.
pub const PING_INTERVAL: Duration = Duration::from_millis(500);

const MAGIC: u32 = 0x4e50_434b; // "NPCK"
const REQUEST_LEN: usize = 16;
const REPLY_LEN: usize = 32;

/// Microseconds on this process's monotonic clock.
#[derive(Clone, Debug)]
pub struct LocalClock {
    base: Instant,
}

impl LocalClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
        }
    }

    pub fn now_us(&self) -> i64 {
        self.base.elapsed().as_micros() as i64
    }
}

impl Default for LocalClock {
    fn default() -> Self {
        Self::new()
    }
}

/// One request/response exchange, in the usual NTP notation:
/// t0 = client send, t1 = server receive, t2 = server send, t3 = client receive.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    /// Server clock minus client clock.
    pub offset_us: i64,
    /// Round trip time, excluding time spent inside the server.
    pub rtt_us: i64,
}

impl Sample {
    pub fn from_timestamps(t0: i64, t1: i64, t2: i64, t3: i64) -> Self {
        Self {
            offset_us: ((t1 - t0) + (t2 - t3)) / 2,
            rtt_us: (t3 - t0) - (t2 - t1),
        }
    }
}

/// Keeps a window of samples and trusts the one with the lowest round trip,
/// since queueing delay on Wi-Fi only ever adds time and skews the offset.
#[derive(Debug, Default)]
pub struct OffsetEstimator {
    samples: VecDeque<Sample>,
}

impl OffsetEstimator {
    pub fn add(&mut self, sample: Sample) {
        if sample.rtt_us < 0 {
            return;
        }
        if self.samples.len() == WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    pub fn best(&self) -> Option<Sample> {
        self.samples.iter().min_by_key(|s| s.rtt_us).copied()
    }

    pub fn offset_us(&self) -> Option<i64> {
        self.best().map(|s| s.offset_us)
    }

    pub fn reset(&mut self) {
        self.samples.clear();
    }
}

#[derive(Debug)]
struct ClockState {
    is_leader: bool,
    /// Bumped whenever the leader changes, since times from different
    /// leaders' clocks cannot be compared.
    epoch: u64,
    estimator: OffsetEstimator,
}

/// The clock every timeline is expressed in: the leader's local clock.
#[derive(Debug)]
pub struct SharedClock {
    local: LocalClock,
    state: Mutex<ClockState>,
}

impl SharedClock {
    pub fn new(local: LocalClock) -> Arc<Self> {
        Arc::new(Self {
            local,
            state: Mutex::new(ClockState {
                is_leader: true,
                epoch: 0,
                estimator: OffsetEstimator::default(),
            }),
        })
    }

    pub fn local(&self) -> &LocalClock {
        &self.local
    }

    /// Offset from local to shared time, if known. Zero when this node leads.
    pub fn offset_us(&self) -> Option<i64> {
        let state = self.state.lock().unwrap();
        if state.is_leader {
            Some(0)
        } else {
            state.estimator.offset_us()
        }
    }

    /// Current shared time, or `None` until a follower has synced.
    pub fn now_us(&self) -> Option<i64> {
        self.offset_us().map(|o| self.local.now_us() + o)
    }

    /// Like `now_us`, but `None` unless the clock still follows the leader
    /// of `epoch`.
    pub fn now_in_epoch(&self, epoch: u64) -> Option<i64> {
        let state = self.state.lock().unwrap();
        if state.epoch != epoch {
            return None;
        }
        let offset = if state.is_leader {
            Some(0)
        } else {
            state.estimator.offset_us()
        };
        offset.map(|o| self.local.now_us() + o)
    }

    pub fn epoch(&self) -> u64 {
        self.state.lock().unwrap().epoch
    }

    pub fn is_synced(&self) -> bool {
        self.offset_us().is_some()
    }

    pub fn set_leader(&self) {
        let mut state = self.state.lock().unwrap();
        state.is_leader = true;
        state.epoch += 1;
        state.estimator.reset();
    }

    pub fn set_follower(&self) {
        let mut state = self.state.lock().unwrap();
        state.is_leader = false;
        state.epoch += 1;
        state.estimator.reset();
    }

    pub fn add_sample(&self, sample: Sample) {
        self.state.lock().unwrap().estimator.add(sample);
    }

    pub fn best_sample(&self) -> Option<Sample> {
        self.state.lock().unwrap().estimator.best()
    }
}

/// Answers clock pings from other nodes using this node's local clock.
pub async fn serve(socket: Arc<UdpSocket>, local: LocalClock) {
    let mut buf = [0u8; 64];
    loop {
        let Ok((len, from)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let t1 = local.now_us();
        if len != REQUEST_LEN || u32::from_be_bytes(buf[0..4].try_into().unwrap()) != MAGIC {
            continue;
        }
        let mut reply = [0u8; REPLY_LEN];
        reply[0..16].copy_from_slice(&buf[0..16]);
        reply[16..24].copy_from_slice(&t1.to_be_bytes());
        let t2 = local.now_us();
        reply[24..32].copy_from_slice(&t2.to_be_bytes());
        let _ = socket.send_to(&reply, from).await;
    }
}

/// Pings whichever clock server `target` names and feeds the samples into
/// `clock`. Uses the same socket as `serve` would for replies, so it owns
/// its own socket.
pub async fn follow(clock: Arc<SharedClock>, mut target: watch::Receiver<Option<SocketAddr>>) {
    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("clock client could not bind: {e}");
            return;
        }
    };
    let mut seq: u32 = 0;
    let mut buf = [0u8; 64];
    let mut ticker = tokio::time::interval(PING_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let Some(addr) = *target.borrow() else { continue };
                seq = seq.wrapping_add(1);
                let mut req = [0u8; REQUEST_LEN];
                req[0..4].copy_from_slice(&MAGIC.to_be_bytes());
                req[4..8].copy_from_slice(&seq.to_be_bytes());
                req[8..16].copy_from_slice(&clock.local().now_us().to_be_bytes());
                let _ = socket.send_to(&req, addr).await;
            }
            changed = target.changed() => {
                if changed.is_err() {
                    return;
                }
                // Ping the new leader straight away.
                ticker.reset_immediately();
            }
            recv = socket.recv_from(&mut buf) => {
                let t3 = clock.local().now_us();
                let Ok((len, from)) = recv else { continue };
                if len != REPLY_LEN || Some(from) != *target.borrow() {
                    continue;
                }
                if u32::from_be_bytes(buf[0..4].try_into().unwrap()) != MAGIC
                    || u32::from_be_bytes(buf[4..8].try_into().unwrap()) != seq
                {
                    continue;
                }
                let t0 = i64::from_be_bytes(buf[8..16].try_into().unwrap());
                let t1 = i64::from_be_bytes(buf[16..24].try_into().unwrap());
                let t2 = i64::from_be_bytes(buf[24..32].try_into().unwrap());
                clock.add_sample(Sample::from_timestamps(t0, t1, t2, t3));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symmetric_delay_gives_exact_offset() {
        // Server is 1000us ahead, 200us each way, 50us inside the server.
        let s = Sample::from_timestamps(0, 1200, 1250, 450);
        assert_eq!(s.offset_us, 1000);
        assert_eq!(s.rtt_us, 400);
    }

    #[test]
    fn estimator_prefers_lowest_round_trip() {
        let mut e = OffsetEstimator::default();
        e.add(Sample {
            offset_us: 900,
            rtt_us: 5000,
        });
        e.add(Sample {
            offset_us: 1000,
            rtt_us: 300,
        });
        e.add(Sample {
            offset_us: 1400,
            rtt_us: 9000,
        });
        assert_eq!(e.offset_us(), Some(1000));
    }

    #[test]
    fn estimator_forgets_old_samples() {
        let mut e = OffsetEstimator::default();
        e.add(Sample {
            offset_us: 1,
            rtt_us: 1,
        });
        for _ in 0..WINDOW {
            e.add(Sample {
                offset_us: 7,
                rtt_us: 100,
            });
        }
        assert_eq!(e.offset_us(), Some(7));
    }

    #[test]
    fn follower_is_unsynced_until_first_sample() {
        let c = SharedClock::new(LocalClock::new());
        assert!(c.is_synced());
        c.set_follower();
        assert!(!c.is_synced());
        c.add_sample(Sample {
            offset_us: 5,
            rtt_us: 10,
        });
        assert_eq!(c.offset_us(), Some(5));
    }

    #[test]
    fn readings_from_an_old_epoch_are_refused() {
        let c = SharedClock::new(LocalClock::new());
        let epoch = c.epoch();
        assert!(c.now_in_epoch(epoch).is_some());
        c.set_follower();
        c.add_sample(Sample {
            offset_us: 5,
            rtt_us: 10,
        });
        assert!(c.now_in_epoch(epoch).is_none());
        assert!(c.now_in_epoch(c.epoch()).is_some());
    }

    #[tokio::test]
    async fn client_and_server_agree_over_loopback() {
        let server_clock = LocalClock::new();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let client = SharedClock::new(LocalClock::new());
        client.set_follower();

        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = sock.local_addr().unwrap();
        tokio::spawn(serve(sock, server_clock.clone()));
        let (_tx, rx) = watch::channel(Some(addr));
        tokio::spawn(follow(client.clone(), rx));

        for _ in 0..50 {
            if client.is_synced() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let shared = client.now_us().expect("synced");
        let truth = server_clock.now_us();
        assert!(
            (shared - truth).abs() < 2_000,
            "off by {}us",
            shared - truth
        );
    }
}
