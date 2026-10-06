//! Plays the shared timeline through mpv and keeps it in sync.
//!
//! mpv runs as a separate process controlled over its JSON IPC socket. Every
//! tick the player compares mpv's position with where the timeline says it
//! should be, nudges playback speed for small errors, and re-cues for big
//! ones.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot, watch};

use crate::clock::SharedClock;
use crate::media::SharedFiles;
use crate::node::View;
use crate::protocol::Command as NodeCommand;
use crate::timeline::Timeline;

const TICK: Duration = Duration::from_millis(100);
/// Errors above this are fixed by re-cueing rather than by changing speed.
const RECUE_US: i64 = 300_000;
/// Speed correction starts above this error...
const CORRECT_START_US: i64 = 20_000;
/// ...and stops once the error is back under this.
const CORRECT_STOP_US: i64 = 5_000;
/// Speed change per second of error (40 ms ahead plays 4% slow).
const GAIN: f64 = 1.0;
/// Never change speed by more than this fraction (pitch is corrected by mpv).
const MAX_ADJUST: f64 = 0.05;
/// When re-cueing, how far ahead of the timeline to seek and wait.
const CUE_LEAD_US: i64 = 300_000;
/// Within this of the cue point, sleep precisely instead of waiting a tick.
const CUE_WINDOW_US: i64 = 120_000;
/// Ignore position readings for this long after starting playback.
const SETTLE_US: i64 = 400_000;
/// After the user acts in the mpv window, leave mpv alone this long so the
/// session can catch up instead of the player undoing it.
const USER_HOLD: Duration = Duration::from_millis(1500);
/// A paused player further than this from the timeline seeks.
const PAUSED_TOLERANCE_US: i64 = 40_000;

#[derive(Clone, Debug, PartialEq)]
pub enum Correction {
    Recue,
    Speed(f64),
}

/// Turns a measured error into a correction, with hysteresis so it does not
/// keep fiddling with speed once in sync.
#[derive(Debug, Default)]
pub struct SyncController {
    correcting: bool,
}

impl SyncController {
    /// `error_us` is actual minus expected position: positive means ahead.
    pub fn update(&mut self, error_us: i64, rate: f64) -> Correction {
        let abs = error_us.abs();
        if abs > RECUE_US {
            self.correcting = false;
            return Correction::Recue;
        }
        if abs > CORRECT_START_US {
            self.correcting = true;
        } else if abs < CORRECT_STOP_US {
            self.correcting = false;
        }
        if !self.correcting {
            return Correction::Speed(rate);
        }
        let adjust = (error_us as f64 / 1e6 * GAIN).clamp(-MAX_ADJUST, MAX_ADJUST);
        Correction::Speed(rate * (1.0 - adjust))
    }
}

pub struct PlayerConfig {
    pub mpv_path: String,
    /// Plays this much ahead (positive) or behind, to make up for output
    /// latency mpv cannot see, such as Bluetooth speakers.
    pub offset_ms: i64,
    pub extra_args: Vec<String>,
}

impl Default for PlayerConfig {
    fn default() -> Self {
        Self {
            mpv_path: "mpv".into(),
            offset_ms: 0,
            extra_args: Vec::new(),
        }
    }
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>;

/// A running mpv process.
pub struct Mpv {
    _child: Child,
    tx: mpsc::UnboundedSender<String>,
    pending: Pending,
    next_id: AtomicU64,
    events: mpsc::UnboundedReceiver<Value>,
}

impl Mpv {
    pub async fn spawn(cfg: &PlayerConfig) -> anyhow::Result<Mpv> {
        let ipc = ipc_path();
        let child = Command::new(&cfg.mpv_path)
            .args([
                "--idle=yes",
                "--force-window=yes",
                "--keep-open=no",
                "--pause",
                "--hr-seek=yes",
                "--no-terminal",
                "--title=NodePlayer",
            ])
            .arg(format!("--input-ipc-server={ipc}"))
            .args(&cfg.extra_args)
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting {} (is mpv installed?)", cfg.mpv_path))?;

        let (reader, writer) = connect_ipc(&ipc).await?;
        let (tx, mut out) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            let mut writer = writer;
            while let Some(line) = out.recv().await {
                if writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
        });

        let pending: Pending = Arc::default();
        let (event_tx, events) = mpsc::unbounded_channel();
        let pending_r = pending.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(id) = v.get("request_id").and_then(Value::as_u64) {
                    if let Some(reply) = pending_r.lock().unwrap().remove(&id) {
                        let _ = reply.send(v);
                    }
                } else if v.get("event").is_some() && event_tx.send(v).is_err() {
                    break;
                }
            }
        });

        Ok(Mpv {
            _child: child,
            tx,
            pending,
            next_id: AtomicU64::new(1),
            events,
        })
    }

    pub async fn command(&self, args: Value) -> anyhow::Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, reply_tx);
        let mut line = json!({ "command": args, "request_id": id }).to_string();
        line.push('\n');
        self.tx.send(line).map_err(|_| anyhow!("mpv has exited"))?;
        let reply = tokio::time::timeout(Duration::from_secs(2), reply_rx)
            .await
            .map_err(|_| anyhow!("mpv did not answer {args}"))??;
        match reply.get("error").and_then(Value::as_str) {
            Some("success") => Ok(reply.get("data").cloned().unwrap_or(Value::Null)),
            other => Err(anyhow!("mpv {args}: {}", other.unwrap_or("no status"))),
        }
    }

    async fn set(&self, prop: &str, value: Value) -> anyhow::Result<()> {
        self.command(json!(["set_property", prop, value]))
            .await
            .map(|_| ())
    }

    async fn get_f64(&self, prop: &str) -> Option<f64> {
        self.command(json!(["get_property", prop]))
            .await
            .ok()?
            .as_f64()
    }

    /// Playback position in microseconds. Prefers the audio clock, which mpv
    /// reports with output latency taken into account and finer than frames.
    async fn position_us(&self) -> Option<i64> {
        let secs = match self.get_f64("audio-pts").await {
            Some(s) => s,
            None => self.get_f64("time-pos").await?,
        };
        Some((secs * 1e6) as i64)
    }

    async fn seek_us(&self, pos_us: i64) -> anyhow::Result<()> {
        let secs = pos_us.max(0) as f64 / 1e6;
        self.command(json!(["seek", secs, "absolute+exact"]))
            .await
            .map(|_| ())
    }
}

fn ipc_path() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    if cfg!(windows) {
        format!(r"\\.\pipe\nodeplayer-{id}")
    } else {
        std::env::temp_dir()
            .join(format!("nodeplayer-{id}.sock"))
            .to_string_lossy()
            .into_owned()
    }
}

type IpcHalves = (
    Box<dyn AsyncRead + Send + Unpin>,
    Box<dyn AsyncWrite + Send + Unpin>,
);

async fn connect_ipc(path: &str) -> anyhow::Result<IpcHalves> {
    let mut last_err = None;
    for _ in 0..50 {
        match open_ipc(path).await {
            Ok(halves) => return Ok(halves),
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!(
        "could not connect to mpv at {path}: {:?}",
        last_err
    ))
}

#[cfg(unix)]
async fn open_ipc(path: &str) -> std::io::Result<IpcHalves> {
    let stream = tokio::net::UnixStream::connect(path).await?;
    let (r, w) = stream.into_split();
    Ok((Box::new(r), Box::new(w)))
}

#[cfg(windows)]
async fn open_ipc(path: &str) -> std::io::Result<IpcHalves> {
    let pipe = tokio::net::windows::named_pipe::ClientOptions::new().open(path)?;
    let (r, w) = tokio::io::split(pipe);
    Ok((Box::new(r), Box::new(w)))
}

/// What the player tells the rest of the app.
#[derive(Clone, Debug, PartialEq)]
pub enum PlayerEvent {
    /// An item played to its end.
    Ended(String),
    /// The user paused, resumed or seeked in the mpv window; apply it to
    /// the whole session.
    Command(NodeCommand),
    /// The user dropped a file or URL onto the mpv window.
    Dropped(String),
}

/// Drives one mpv instance from the node's view until mpv exits.
pub struct Player {
    mpv: Mpv,
    clock: Arc<SharedClock>,
    files: SharedFiles,
    view: watch::Receiver<View>,
    offset_us: i64,
    controller: SyncController,
    /// Item currently loaded in mpv.
    loaded: Option<String>,
    /// mpv has finished loading `loaded`.
    ready: bool,
    /// Item that played to its end, and the timeline it ended under.
    finished: Option<(String, Timeline)>,
    paused: bool,
    speed: f64,
    /// Paused at this media position, waiting for the timeline to reach it.
    cue: Option<i64>,
    /// Ignore position readings until this shared time.
    settle_until: i64,
    last_timeline: Option<Timeline>,
    warned_unresolved: Option<String>,
    /// Seeks and loads we asked for that mpv has not reported yet, so the
    /// ones it reports beyond these came from the user.
    our_seeks: u32,
    our_loads: u32,
    user_seeking: bool,
    user_loading: bool,
    hold_until: Option<std::time::Instant>,
}

impl Player {
    pub async fn start(
        cfg: &PlayerConfig,
        clock: Arc<SharedClock>,
        files: SharedFiles,
        view: watch::Receiver<View>,
    ) -> anyhow::Result<Player> {
        let mpv = Mpv::spawn(cfg).await?;
        mpv.command(json!(["observe_property", 1, "pause"]))
            .await
            .ok();
        Ok(Player {
            mpv,
            clock,
            files,
            view,
            offset_us: cfg.offset_ms * 1000,
            controller: SyncController::default(),
            loaded: None,
            ready: false,
            finished: None,
            paused: true,
            speed: 1.0,
            cue: None,
            settle_until: 0,
            last_timeline: None,
            warned_unresolved: None,
            our_seeks: 0,
            our_loads: 0,
            user_seeking: false,
            user_loading: false,
            hold_until: None,
        })
    }

    /// Runs until mpv exits, reporting what happens through `on_event`.
    pub async fn run(mut self, on_event: impl Fn(PlayerEvent)) -> anyhow::Result<()> {
        let mut tick = tokio::time::interval(TICK);
        loop {
            tokio::select! {
                event = self.mpv.events.recv() => {
                    let Some(event) = event else { return Ok(()) };
                    if !self.on_mpv_event(&event, &on_event).await {
                        return Ok(());
                    }
                }
                _ = tick.tick() => {
                    if let Err(e) = self.step().await {
                        tracing::warn!("player: {e}");
                    }
                }
            }
        }
    }

    /// Handles one mpv event. Returns false once mpv is shutting down.
    async fn on_mpv_event(&mut self, event: &Value, on_event: &impl Fn(PlayerEvent)) -> bool {
        match event.get("event").and_then(Value::as_str) {
            Some("start-file") => {
                if self.our_loads > 0 {
                    self.our_loads -= 1;
                } else {
                    self.user_loading = true;
                    self.hold();
                }
            }
            Some("file-loaded") => {
                if self.user_loading {
                    self.user_loading = false;
                    self.loaded = None;
                    self.ready = false;
                    if let Ok(Value::String(path)) =
                        self.mpv.command(json!(["get_property", "path"])).await
                    {
                        on_event(PlayerEvent::Dropped(path));
                    }
                    self.hold();
                } else {
                    self.ready = true;
                }
            }
            Some("end-file") => {
                let eof = event.get("reason").and_then(Value::as_str) == Some("eof");
                if eof && let Some(id) = self.loaded.take() {
                    let tl = self.view.borrow().state.timeline.clone();
                    self.finished = Some((id.clone(), tl));
                    self.ready = false;
                    on_event(PlayerEvent::Ended(id));
                }
            }
            Some("property-change")
                if event.get("name").and_then(Value::as_str) == Some("pause") =>
            {
                let Some(paused) = event.get("data").and_then(Value::as_bool) else {
                    return true;
                };
                // Our own changes already updated `self.paused`.
                if paused != self.paused && self.loaded.is_some() {
                    self.paused = paused;
                    self.cue = None;
                    self.hold();
                    let command = if paused {
                        NodeCommand::Pause
                    } else {
                        NodeCommand::Resume
                    };
                    on_event(PlayerEvent::Command(command));
                }
            }
            Some("seek") => {
                if self.our_seeks > 0 {
                    self.our_seeks -= 1;
                } else if self.loaded.is_some() {
                    self.user_seeking = true;
                    self.hold();
                }
            }
            Some("playback-restart") if self.user_seeking => {
                self.user_seeking = false;
                if let Some(pos) = self.mpv.get_f64("time-pos").await {
                    let pos_ms = ((pos * 1e6) as i64 - self.offset_us) / 1000;
                    on_event(PlayerEvent::Command(NodeCommand::Seek { pos_ms }));
                }
                self.hold();
            }
            Some("shutdown") => return false,
            _ => {}
        }
        true
    }

    fn hold(&mut self) {
        self.hold_until = Some(std::time::Instant::now() + USER_HOLD);
    }

    async fn seek(&mut self, pos_us: i64) -> anyhow::Result<()> {
        self.our_seeks += 1;
        let result = self.mpv.seek_us(pos_us).await;
        if result.is_err() {
            self.our_seeks -= 1;
        }
        result
    }

    async fn step(&mut self) -> anyhow::Result<()> {
        if self
            .hold_until
            .is_some_and(|t| std::time::Instant::now() < t)
        {
            return Ok(());
        }
        let view = self.view.borrow().clone();
        let tl = view.state.timeline.clone();

        let Some(item_id) = tl.item_id.clone() else {
            if self.loaded.take().is_some() {
                self.mpv.command(json!(["stop"])).await?;
                self.ready = false;
            }
            self.finished = None;
            self.last_timeline = Some(tl);
            return Ok(());
        };

        if self.loaded.as_deref() != Some(&item_id) {
            if let Some((done, done_tl)) = &self.finished
                && *done == item_id
                && *done_tl == tl
            {
                return Ok(()); // Played to the end; wait for the leader.
            }
            let Some(item) = view.state.playlist.get(&item_id) else {
                return Ok(());
            };
            let Some(url) = view.resolve(item, &self.files) else {
                if self.warned_unresolved.as_deref() != Some(&item_id) {
                    tracing::warn!("cannot reach the node sharing {}", item.title);
                    self.warned_unresolved = Some(item_id);
                }
                return Ok(());
            };
            tracing::info!("loading {}", item.title);
            self.our_loads += 1;
            self.mpv
                .command(json!(["loadfile", url, "replace"]))
                .await?;
            self.mpv.set("pause", json!(true)).await?;
            self.paused = true;
            self.loaded = Some(item_id);
            self.ready = false;
            self.finished = None;
            self.cue = None;
            self.last_timeline = None;
            return Ok(());
        }
        if !self.ready {
            return Ok(());
        }
        let epoch = view.clock_epoch;
        let Some(now) = self.clock.now_in_epoch(epoch) else {
            return Ok(()); // Not synced yet, or the leader just changed.
        };

        let changed = self.last_timeline.as_ref() != Some(&tl);
        self.last_timeline = Some(tl.clone());

        if !tl.playing {
            self.cue = None;
            self.set_paused(true).await?;
            self.set_speed(tl.rate).await?;
            let target = tl.position_at(now) + self.offset_us;
            if let Some(pos) = self.mpv.position_us().await
                && (pos - target).abs() > PAUSED_TOLERANCE_US
            {
                self.seek(target).await?;
            }
            return Ok(());
        }

        if changed {
            let target = tl.position_at(now) + self.offset_us;
            return self.recue(target).await;
        }

        if let Some(cue) = self.cue {
            let until_cue =
                ((cue - (tl.position_at(now) + self.offset_us)) as f64 / tl.rate) as i64;
            if until_cue > CUE_WINDOW_US {
                return Ok(());
            }
            if until_cue > 0 {
                tokio::time::sleep(Duration::from_micros(until_cue as u64)).await;
            }
            self.set_paused(false).await?;
            self.cue = None;
            self.settle_until = now + until_cue.max(0) + SETTLE_US;
            return Ok(());
        }

        if self.paused {
            let target = tl.position_at(now) + self.offset_us;
            return self.recue(target).await;
        }
        if now < self.settle_until {
            return Ok(());
        }

        let Some(before) = self.clock.now_in_epoch(epoch) else {
            return Ok(());
        };
        let Some(pos) = self.mpv.position_us().await else {
            return Ok(());
        };
        let Some(after) = self.clock.now_in_epoch(epoch) else {
            return Ok(());
        };
        let target = tl.position_at((before + after) / 2) + self.offset_us;
        tracing::debug!(
            error_ms = (pos - target) as f64 / 1000.0,
            speed = self.speed,
            "sync"
        );
        match self.controller.update(pos - target, tl.rate) {
            Correction::Recue => {
                tracing::info!("off by {} ms; re-cueing", (pos - target) / 1000);
                self.recue(target).await?;
            }
            Correction::Speed(speed) => self.set_speed(speed).await?,
        }
        Ok(())
    }

    /// Pause, seek a little ahead of the timeline, and wait for it to arrive.
    async fn recue(&mut self, target: i64) -> anyhow::Result<()> {
        let cue = (target + CUE_LEAD_US).max(0);
        self.set_paused(true).await?;
        self.set_speed(self.last_timeline.as_ref().map_or(1.0, |t| t.rate))
            .await?;
        self.seek(cue).await?;
        self.cue = Some(cue);
        Ok(())
    }

    async fn set_paused(&mut self, paused: bool) -> anyhow::Result<()> {
        if self.paused != paused {
            self.mpv.set("pause", json!(paused)).await?;
            self.paused = paused;
        }
        Ok(())
    }

    async fn set_speed(&mut self, speed: f64) -> anyhow::Result<()> {
        if (self.speed - speed).abs() > 0.0005 {
            self.mpv.set("speed", json!(speed)).await?;
            self.speed = speed;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_errors_are_left_alone() {
        let mut c = SyncController::default();
        assert_eq!(c.update(10_000, 1.0), Correction::Speed(1.0));
        assert_eq!(c.update(-15_000, 1.0), Correction::Speed(1.0));
    }

    #[test]
    fn ahead_slows_down_until_back_in_sync() {
        let mut c = SyncController::default();
        let Correction::Speed(s) = c.update(40_000, 1.0) else {
            panic!()
        };
        assert!((s - 0.96).abs() < 1e-9);
        // Still correcting inside the hysteresis band.
        let Correction::Speed(s) = c.update(10_000, 1.0) else {
            panic!()
        };
        assert!(s < 1.0);
        assert_eq!(c.update(2_000, 1.0), Correction::Speed(1.0));
    }

    #[test]
    fn behind_speeds_up_within_limit() {
        let mut c = SyncController::default();
        let Correction::Speed(s) = c.update(-200_000, 1.0) else {
            panic!()
        };
        assert!((s - 1.05).abs() < 1e-9);
    }

    #[test]
    fn large_errors_recue() {
        let mut c = SyncController::default();
        assert_eq!(c.update(500_000, 1.0), Correction::Recue);
        assert_eq!(c.update(-500_000, 1.0), Correction::Recue);
    }
}
