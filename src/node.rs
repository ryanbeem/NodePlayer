//! A running NodePlayer node: control links to other nodes, playlist
//! sessions, host hand-off, and the shared playlist and timeline.
//!
//! A node starts out idle. It can create a named playlist session, which it
//! then hosts, or join one that another node advertises. The host holds the
//! playlist, the timeline and the clock every member follows. If the host
//! leaves, the longest-running remaining member takes over.
//!
//! All state lives in one actor task that handles events one at a time, so
//! there is no locking around the playlist or timeline.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::AbortHandle;

use crate::clock::{self, LocalClock, SharedClock};
use crate::discovery::{self, Discovery};
use crate::media::{self, SharedFiles};
use crate::playlist::{Item, Source};
use crate::protocol::{
    Command, EditPolicy, Message, MessageReader, NodeInfo, SessionInfo, SharedState, write_message,
};

/// The host re-sends its state this often, which doubles as a heartbeat.
/// Members also use this tick to retry reaching the host.
const HEARTBEAT: Duration = Duration::from_secs(2);
/// A member that hears nothing from its host for this long drops it.
const LEADER_TIMEOUT: Duration = Duration::from_secs(7);

pub struct Config {
    pub name: String,
    pub bind_ip: IpAddr,
    /// TCP port for the control link; 0 picks a free one.
    pub control_port: u16,
    /// Nodes to connect to directly, for networks where mDNS is blocked.
    pub peers: Vec<SocketAddr>,
    pub mdns: bool,
    /// How far ahead play, resume and seek are scheduled so every node can
    /// get ready in time.
    pub start_delay: Duration,
    /// Overrides the start time used to pick a new host (tests).
    pub started_ms: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            name: "nodeplayer".into(),
            bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            control_port: 0,
            peers: Vec::new(),
            mdns: true,
            start_delay: Duration::from_millis(500),
            started_ms: None,
        }
    }
}

/// A playlist session some node on the network advertises.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: String,
    pub name: String,
    pub host_name: String,
    pub members: usize,
}

/// A snapshot of what this node knows, for the UI and the player.
#[derive(Clone, Debug)]
pub struct View {
    pub me: NodeInfo,
    /// Host of the session this node is in (this node itself when idle).
    pub leader: String,
    pub peers: Vec<NodeInfo>,
    pub state: SharedState,
    /// True when this node hosts, or is a member with a live link to the host.
    pub connected: bool,
    /// Clock epoch the timeline is expressed in (see `SharedClock::now_in_epoch`).
    pub clock_epoch: u64,
}

impl View {
    pub fn session(&self) -> Option<&SessionInfo> {
        self.me.session.as_ref()
    }

    pub fn is_leader(&self) -> bool {
        self.leader == self.me.id
    }

    pub fn is_host(&self) -> bool {
        self.session().is_some() && self.is_leader()
    }

    pub fn name_of(&self, id: &str) -> String {
        if id == self.me.id {
            return self.me.name.clone();
        }
        self.peers
            .iter()
            .find(|p| p.id == id)
            .map(|p| p.name.clone())
            .unwrap_or_else(|| id.chars().take(8).collect())
    }

    pub fn leader_name(&self) -> String {
        self.name_of(&self.leader)
    }

    /// Whether this node may change the playlist.
    pub fn can_edit(&self) -> bool {
        self.state.edit_policy == EditPolicy::Anyone || self.is_leader()
    }

    /// Sessions advertised on the network, including this node's own.
    pub fn sessions(&self) -> Vec<SessionSummary> {
        let mut out: Vec<SessionSummary> = Vec::new();
        for node in self.peers.iter().chain(std::iter::once(&self.me)) {
            let Some(s) = &node.session else { continue };
            match out.iter_mut().find(|o| o.id == s.id) {
                Some(o) => o.members += 1,
                None => out.push(SessionSummary {
                    id: s.id.clone(),
                    name: s.name.clone(),
                    host_name: self.name_of(&s.host),
                    members: 1,
                }),
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        out
    }

    /// Where this node's player should open `item` from.
    pub fn resolve(&self, item: &Item, files: &SharedFiles) -> Option<String> {
        match &item.source {
            Source::Url { url } => Some(url.clone()),
            Source::Shared { owner, file_id } if *owner == self.me.id => {
                files.get(file_id).map(|p| p.to_string_lossy().into_owned())
            }
            Source::Shared { owner, file_id } => self
                .peers
                .iter()
                .find(|p| p.id == *owner)?
                .media_url(file_id),
        }
    }
}

enum Event {
    Command(Command),
    Create { name: String, policy: EditPolicy },
    Join { session_id: String },
    Leave,
    Ended(String),
    Discovery(Discovery),
    Connected(TcpStream, SocketAddr),
    DialFailed(SocketAddr),
    Inbound { conn: u64, msg: Message },
    Closed { conn: u64 },
    Tick,
}

pub struct Node {
    pub info: NodeInfo,
    pub clock: Arc<SharedClock>,
    pub files: SharedFiles,
    events: mpsc::UnboundedSender<Event>,
    view: watch::Receiver<View>,
    notices: broadcast::Sender<String>,
    tasks: Vec<AbortHandle>,
    _mdns: Option<mdns_sd::ServiceDaemon>,
}

impl Node {
    pub async fn start(cfg: Config) -> anyhow::Result<Node> {
        let local = LocalClock::new();
        let clock = SharedClock::new(local.clone());
        let files = SharedFiles::default();

        let control = TcpListener::bind((cfg.bind_ip, cfg.control_port))
            .await
            .context("binding control port")?;
        let udp = Arc::new(
            UdpSocket::bind((cfg.bind_ip, 0))
                .await
                .context("binding clock port")?,
        );
        let media_listener = TcpListener::bind((cfg.bind_ip, 0))
            .await
            .context("binding media port")?;

        let started_ms = cfg.started_ms.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        });
        let me = NodeInfo {
            id: uuid::Uuid::new_v4().simple().to_string(),
            name: cfg.name.clone(),
            started_ms,
            host: String::new(),
            control_port: control.local_addr()?.port(),
            clock_port: udp.local_addr()?.port(),
            media_port: media_listener.local_addr()?.port(),
            session: None,
        };

        let (events, rx) = mpsc::unbounded_channel();
        let (clock_target, clock_target_rx) = watch::channel(None);
        let (advert, advert_rx) = watch::channel(me.clone());
        let (notices, _) = broadcast::channel(32);
        let state = SharedState {
            leader: me.id.clone(),
            ..Default::default()
        };
        let (view_tx, view) = watch::channel(View {
            me: me.clone(),
            leader: me.id.clone(),
            peers: Vec::new(),
            state: state.clone(),
            connected: true,
            clock_epoch: clock.epoch(),
        });

        let mut tasks = vec![
            tokio::spawn(clock::serve(udp, local)).abort_handle(),
            tokio::spawn(clock::follow(clock.clone(), clock_target_rx)).abort_handle(),
            tokio::spawn(media::serve(media_listener, files.clone())).abort_handle(),
        ];

        let accept_events = events.clone();
        tasks.push(
            tokio::spawn(async move {
                loop {
                    if let Ok((stream, addr)) = control.accept().await {
                        let _ = stream.set_nodelay(true);
                        if accept_events.send(Event::Connected(stream, addr)).is_err() {
                            return;
                        }
                    }
                }
            })
            .abort_handle(),
        );

        let tick_events = events.clone();
        tasks.push(
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(HEARTBEAT);
                loop {
                    tick.tick().await;
                    if tick_events.send(Event::Tick).is_err() {
                        return;
                    }
                }
            })
            .abort_handle(),
        );

        let mdns = if cfg.mdns {
            let (dtx, mut drx) = mpsc::unbounded_channel();
            let daemon = discovery::start(advert_rx, dtx).context("starting mDNS")?;
            let fwd = events.clone();
            tasks.push(
                tokio::spawn(async move {
                    while let Some(d) = drx.recv().await {
                        if fwd.send(Event::Discovery(d)).is_err() {
                            return;
                        }
                    }
                })
                .abort_handle(),
            );
            Some(daemon)
        } else {
            None
        };

        let mut actor = Actor {
            me: me.clone(),
            start_delay_us: cfg.start_delay.as_micros() as i64,
            clock: clock.clone(),
            peers: HashMap::new(),
            leader: me.id.clone(),
            state,
            conns: HashMap::new(),
            next_conn: 0,
            upstream: None,
            last_from_leader: Instant::now(),
            dialing: HashSet::new(),
            static_peers: cfg.peers.iter().copied().collect(),
            events: events.clone(),
            clock_target,
            advert,
            view: view_tx,
            notices: notices.clone(),
        };
        for addr in &cfg.peers {
            actor.dial(*addr);
        }
        tasks.push(tokio::spawn(actor.run(rx)).abort_handle());

        Ok(Node {
            info: me,
            clock,
            files,
            events,
            view,
            notices,
            tasks,
            _mdns: mdns,
        })
    }

    pub fn view(&self) -> watch::Receiver<View> {
        self.view.clone()
    }

    /// Messages for the user, such as a command the host refused.
    pub fn notices(&self) -> broadcast::Receiver<String> {
        self.notices.subscribe()
    }

    pub fn command(&self, command: Command) {
        let _ = self.events.send(Event::Command(command));
    }

    /// Start a new playlist session hosted by this node, leaving any other.
    pub fn create(&self, name: &str, policy: EditPolicy) {
        let _ = self.events.send(Event::Create {
            name: name.to_string(),
            policy,
        });
    }

    /// Join a session another node advertises (see `View::sessions`).
    pub fn join(&self, session_id: &str) {
        let _ = self.events.send(Event::Join {
            session_id: session_id.to_string(),
        });
    }

    pub fn leave(&self) {
        let _ = self.events.send(Event::Leave);
    }

    /// Adds every media file in `dir` and its subfolders, in name order.
    /// Returns the items added, or an error if none were found.
    pub fn add_folder(&self, dir: &str) -> anyhow::Result<Vec<Item>> {
        let mut files = Vec::new();
        collect_media(Path::new(dir), &mut files)?;
        files.sort();
        anyhow::ensure!(!files.is_empty(), "no audio or video files found in {dir}");
        files
            .iter()
            .map(|f| self.add(&f.to_string_lossy()))
            .collect()
    }

    /// Adds a local file (shared from this node) or a URL to the playlist.
    pub fn add(&self, input: &str) -> anyhow::Result<Item> {
        let path = Path::new(input);
        let (title, source) = if path.is_file() {
            let path = path.canonicalize()?;
            let file_id = uuid::Uuid::new_v4().simple().to_string();
            let title = path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| input.to_string());
            self.files.insert(file_id.clone(), path);
            (
                title,
                Source::Shared {
                    owner: self.info.id.clone(),
                    file_id,
                },
            )
        } else if input.contains("://") {
            let title = input
                .rsplit('/')
                .find(|s| !s.is_empty())
                .unwrap_or(input)
                .to_string();
            (
                title,
                Source::Url {
                    url: input.to_string(),
                },
            )
        } else {
            bail!("{input} is not a file or a URL");
        };
        let item = Item {
            id: uuid::Uuid::new_v4().simple().to_string(),
            title,
            source,
            added_by: self.info.name.clone(),
        };
        self.command(Command::Add { item: item.clone() });
        Ok(item)
    }

    /// A callback for the player to report an item that played to its end.
    pub fn ended_reporter(&self) -> impl Fn(String) + Send + Sync + 'static {
        let events = self.events.clone();
        move |item_id| {
            let _ = events.send(Event::Ended(item_id));
        }
    }
}

/// File extensions `add_folder` picks up.
const MEDIA_EXTENSIONS: &[&str] = &[
    "mp4", "m4v", "mkv", "webm", "mov", "avi", "wmv", "flv", "mpg", "mpeg", "ts", "m2ts", "mp3",
    "flac", "wav", "ogg", "oga", "opus", "m4a", "aac", "wma", "aiff",
];

fn collect_media(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        let hidden = path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with('.'));
        if hidden {
            continue;
        }
        if path.is_dir() {
            collect_media(&path, out)?;
        } else if path.extension().is_some_and(|e| {
            MEDIA_EXTENSIONS.contains(&e.to_string_lossy().to_lowercase().as_str())
        }) {
            out.push(path);
        }
    }
    Ok(())
}

impl Drop for Node {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
        if let Some(d) = &self._mdns {
            let _ = d.shutdown();
        }
    }
}

struct Conn {
    peer: Option<String>,
    addr: SocketAddr,
    tx: mpsc::UnboundedSender<Message>,
    tasks: [AbortHandle; 2],
}

impl Drop for Conn {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

struct Actor {
    /// This node's details, including the session it is in.
    me: NodeInfo,
    start_delay_us: i64,
    clock: Arc<SharedClock>,
    peers: HashMap<String, NodeInfo>,
    /// Host of our session, or our own id when idle.
    leader: String,
    state: SharedState,
    conns: HashMap<u64, Conn>,
    next_conn: u64,
    /// Connection to the host, when we are a member.
    upstream: Option<u64>,
    last_from_leader: Instant,
    dialing: HashSet<SocketAddr>,
    static_peers: HashSet<SocketAddr>,
    events: mpsc::UnboundedSender<Event>,
    clock_target: watch::Sender<Option<SocketAddr>>,
    advert: watch::Sender<NodeInfo>,
    view: watch::Sender<View>,
    notices: broadcast::Sender<String>,
}

impl Actor {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Event>) {
        while let Some(event) = rx.recv().await {
            self.handle(event);
        }
    }

    fn is_leader(&self) -> bool {
        self.leader == self.me.id
    }

    fn session_id(&self) -> Option<&str> {
        self.me.session.as_ref().map(|s| s.id.as_str())
    }

    fn notice(&self, text: impl Into<String>) {
        let _ = self.notices.send(text.into());
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Command(command) => {
                if self.me.session.is_none() {
                    self.notice("Join or create a playlist first (type sessions, join or create).");
                } else if self.is_leader() {
                    let me = self.me.id.clone();
                    if let Err(e) = self.apply(command, &me) {
                        self.notice(e);
                    }
                } else if command.edits_playlist() && self.state.edit_policy == EditPolicy::HostOnly
                {
                    self.notice("Only the host can change this playlist.");
                } else if !self.send_upstream(Message::Request { command }) {
                    self.notice("Not connected to the host yet; try again in a moment.");
                }
            }
            Event::Create { name, policy } => self.create(name, policy),
            Event::Join { session_id } => self.join(&session_id),
            Event::Leave => {
                if self.me.session.is_some() {
                    self.leave();
                }
            }
            Event::Ended(item_id) => {
                if self.is_leader() {
                    self.on_ended(&item_id);
                } else {
                    self.send_upstream(Message::Ended { item_id });
                }
            }
            Event::Discovery(Discovery::Found(info)) => {
                if self.peers.get(&info.id) != Some(&info) {
                    if !self.peers.contains_key(&info.id) {
                        tracing::info!("found {} at {}", info.name, info.host);
                    }
                    self.peers.insert(info.id.clone(), info);
                    self.elect();
                }
            }
            Event::Discovery(Discovery::Lost(id)) => {
                if !self.conns.values().any(|c| c.peer.as_deref() == Some(&id))
                    && self.peers.remove(&id).is_some()
                {
                    self.elect();
                }
            }
            Event::Connected(stream, addr) => {
                self.dialing.remove(&addr);
                self.add_conn(stream, addr);
            }
            Event::DialFailed(addr) => {
                self.dialing.remove(&addr);
                let dead: Vec<String> = self
                    .peers
                    .values()
                    .filter(|p| p.control_addr() == Some(addr))
                    .map(|p| p.id.clone())
                    .collect();
                for id in dead {
                    tracing::info!("cannot reach {id}; forgetting it");
                    self.peers.remove(&id);
                }
                self.elect();
            }
            Event::Inbound { conn, msg } => self.on_message(conn, msg),
            Event::Closed { conn } => self.drop_conn(conn),
            Event::Tick => self.on_tick(),
        }
    }

    /// Leave any session and become an idle node.
    fn leave(&mut self) {
        self.me.session = None;
        self.become_idle();
        self.announce();
        self.publish();
    }

    fn become_idle(&mut self) {
        self.leader = self.me.id.clone();
        self.upstream = None;
        self.clock.set_leader();
        self.clock_target.send_replace(None);
        self.state = SharedState {
            leader: self.me.id.clone(),
            ..Default::default()
        };
    }

    fn create(&mut self, name: String, policy: EditPolicy) {
        self.become_idle();
        let session = SessionInfo {
            id: uuid::Uuid::new_v4().simple().to_string(),
            name,
            host: self.me.id.clone(),
        };
        self.me.session = Some(session.clone());
        self.state.session = Some(session);
        self.state.edit_policy = policy;
        self.state.version = 1;
        self.announce();
        self.publish();
    }

    fn join(&mut self, session_id: &str) {
        if self.session_id() == Some(session_id) {
            return;
        }
        let Some(session) = self
            .peers
            .values()
            .filter_map(|p| p.session.clone())
            .find(|s| s.id == session_id)
        else {
            self.notice("No playlist with that name is on the network right now.");
            return;
        };
        self.become_idle();
        self.me.session = Some(session);
        // Not yet following anyone: the election below picks the host.
        self.leader = String::new();
        self.state = SharedState::default();
        self.announce();
        self.elect();
    }

    /// Tell other nodes our details changed, over mDNS and open links.
    fn announce(&mut self) {
        self.advert.send_replace(self.me.clone());
        let msg = Message::Announce {
            info: self.me.clone(),
        };
        for c in self.conns.values().filter(|c| c.peer.is_some()) {
            let _ = c.tx.send(msg.clone());
        }
    }

    fn on_tick(&mut self) {
        if self.me.session.is_none() {
            return;
        }
        if self.is_leader() {
            self.broadcast_state();
        } else if let Some(up) = self.upstream {
            if self.last_from_leader.elapsed() > LEADER_TIMEOUT {
                tracing::warn!("host went quiet; dropping it");
                self.drop_conn(up);
            }
        } else {
            self.ensure_upstream();
        }
    }

    fn on_message(&mut self, conn: u64, msg: Message) {
        let Some(c) = self.conns.get(&conn) else {
            return;
        };
        let addr = c.addr;
        let from = c.peer.clone();
        if Some(conn) == self.upstream {
            self.last_from_leader = Instant::now();
        }
        match msg {
            Message::Hello { info, peers } => {
                if info.id == self.me.id {
                    // Dialled ourselves through a static peer address.
                    self.conns.remove(&conn);
                    return;
                }
                if let Some(c) = self.conns.get_mut(&conn) {
                    c.peer = Some(info.id.clone());
                }
                self.update_peer(info.clone(), addr);
                for p in peers {
                    self.learn_peer(p);
                }
                self.elect();
                if self.is_leader() && self.session_id().is_some_and(|s| info.in_session(s)) {
                    let msg = Message::State {
                        state: self.state.clone(),
                    };
                    self.send(conn, msg);
                }
            }
            Message::Announce { info } => {
                if info.id != self.me.id {
                    self.update_peer(info, addr);
                    self.elect();
                }
            }
            Message::Request { command } => {
                let Some(from) = from else { return };
                if self.is_leader() && self.me.session.is_some() {
                    if let Err(e) = self.apply(command, &from) {
                        self.send(conn, Message::Notice { text: e });
                    }
                } else {
                    self.send_upstream(Message::Request { command });
                }
            }
            Message::State { state } => {
                let ours = state.session.as_ref().map(|s| s.id.as_str()) == self.session_id();
                if self.is_leader() || state.leader != self.leader || !ours {
                    return;
                }
                if state.version > self.state.version || self.state.leader != state.leader {
                    for p in &state.members {
                        self.learn_peer(p.clone());
                    }
                    self.state = state;
                    // Keep our advertised host current so joiners find it.
                    if let (Some(mine), Some(theirs)) = (&self.me.session, &self.state.session)
                        && mine.host != theirs.host
                    {
                        self.me.session = Some(theirs.clone());
                        self.announce();
                    }
                    self.publish();
                }
            }
            Message::Ended { item_id } => {
                if self.is_leader() {
                    self.on_ended(&item_id);
                }
            }
            Message::Notice { text } => self.notice(text),
        }
    }

    /// Record what a node told us about itself, with the address we see it at.
    fn update_peer(&mut self, mut info: NodeInfo, addr: SocketAddr) {
        info.host = addr.ip().to_string();
        self.peers.insert(info.id.clone(), info);
    }

    /// Adds or refreshes a node someone else told us about.
    fn learn_peer(&mut self, p: NodeInfo) {
        if p.id == self.me.id || p.host.is_empty() {
            return;
        }
        // A direct link tells us more than hearsay, so don't overwrite it.
        let linked = self
            .conns
            .values()
            .any(|c| c.peer.as_deref() == Some(&p.id));
        if !linked || !self.peers.contains_key(&p.id) {
            self.peers.insert(p.id.clone(), p);
        }
    }

    /// Who should host our session: the current host while it is still a
    /// member, else the advertised host, else the longest-running member.
    fn pick_host(&self) -> String {
        let Some(session) = &self.me.session else {
            return self.me.id.clone();
        };
        let members: Vec<&NodeInfo> = self
            .peers
            .values()
            .filter(|p| p.in_session(&session.id))
            .chain(std::iter::once(&self.me))
            .collect();
        for preferred in [&self.leader, &session.host] {
            if members.iter().any(|m| m.id == *preferred) {
                return preferred.clone();
            }
        }
        members
            .into_iter()
            .min_by(|a, b| a.seniority().cmp(&b.seniority()))
            .map(|n| n.id.clone())
            .unwrap()
    }

    /// Re-checks who hosts our session and reconnects if that changed.
    fn elect(&mut self) {
        let new = self.pick_host();
        if new != self.leader {
            let old = std::mem::replace(&mut self.leader, new.clone());
            if new == self.me.id {
                tracing::info!("this node now hosts the playlist");
                self.take_over(&old);
            } else {
                tracing::info!("following host {new}");
                self.clock.set_follower();
                self.upstream = None;
                let target = self.peers.get(&new).and_then(|p| p.clock_addr());
                self.clock_target.send_replace(target);
            }
        }
        self.ensure_upstream();
        if self.is_leader()
            && let Some(sid) = self.session_id()
        {
            let sid = sid.to_string();
            self.state.members = self
                .peers
                .values()
                .filter(|p| p.in_session(&sid))
                .cloned()
                .collect();
            self.broadcast_state();
        }
        self.publish();
    }

    /// Become host, carrying on from the last state the old host sent.
    fn take_over(&mut self, old: &str) {
        if !old.is_empty() && self.state.leader != self.me.id {
            match self.clock.offset_us() {
                // Shared time was local + offset; ours is now just local.
                Some(offset) => self.state.timeline.rebase(-offset),
                None => self.state.timeline.stop(),
            }
        }
        self.clock.set_leader();
        self.clock_target.send_replace(None);
        self.upstream = None;
        if let Some(session) = &mut self.me.session {
            session.host = self.me.id.clone();
            self.state.session = Some(session.clone());
        }
        self.state.leader = self.me.id.clone();
        self.state.version += 1;
        self.announce();
    }

    fn ensure_upstream(&mut self) {
        if self.is_leader() || self.upstream.is_some() {
            return;
        }
        if let Some((&id, _)) = self
            .conns
            .iter()
            .find(|(_, c)| c.peer.as_deref() == Some(&self.leader))
        {
            self.upstream = Some(id);
            self.last_from_leader = Instant::now();
            // The host only sends its state to members; tell it we joined.
            let msg = Message::Announce {
                info: self.me.clone(),
            };
            self.send(id, msg);
            return;
        }
        if let Some(addr) = self.peers.get(&self.leader).and_then(|p| p.control_addr()) {
            self.dial(addr);
        }
    }

    fn dial(&mut self, addr: SocketAddr) {
        if !self.dialing.insert(addr) {
            return;
        }
        let persistent = self.static_peers.contains(&addr);
        let events = self.events.clone();
        tokio::spawn(async move {
            let mut tries = 0;
            loop {
                if let Ok(Ok(stream)) =
                    tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await
                {
                    let _ = stream.set_nodelay(true);
                    let _ = events.send(Event::Connected(stream, addr));
                    return;
                }
                tries += 1;
                if !persistent && tries >= 3 {
                    let _ = events.send(Event::DialFailed(addr));
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    fn add_conn(&mut self, stream: TcpStream, addr: SocketAddr) {
        let id = self.next_conn;
        self.next_conn += 1;
        let (r, mut w) = stream.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        let writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if write_message(&mut w, &msg).await.is_err() {
                    break;
                }
            }
        });
        let events = self.events.clone();
        let reader = tokio::spawn(async move {
            let mut reader = MessageReader::new(r);
            while let Some(msg) = reader.next().await {
                if events.send(Event::Inbound { conn: id, msg }).is_err() {
                    return;
                }
            }
            let _ = events.send(Event::Closed { conn: id });
        });
        self.conns.insert(
            id,
            Conn {
                peer: None,
                addr,
                tx,
                tasks: [reader.abort_handle(), writer.abort_handle()],
            },
        );
        let hello = Message::Hello {
            info: self.me.clone(),
            peers: self.peers.values().cloned().collect(),
        };
        self.send(id, hello);
    }

    fn drop_conn(&mut self, conn: u64) {
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        if self.upstream == Some(conn) {
            self.upstream = None;
        }
        if let Some(peer) = c.peer.clone() {
            let still_linked = self
                .conns
                .values()
                .any(|o| o.peer.as_deref() == Some(&peer));
            if !still_linked {
                tracing::info!("lost link to {peer}");
                self.peers.remove(&peer);
            }
        }
        if self.static_peers.contains(&c.addr) {
            self.dial(c.addr);
        }
        self.elect();
    }

    fn send(&self, conn: u64, msg: Message) {
        if let Some(c) = self.conns.get(&conn) {
            let _ = c.tx.send(msg);
        }
    }

    fn send_upstream(&self, msg: Message) -> bool {
        match self.upstream {
            Some(up) if self.conns.contains_key(&up) => {
                self.send(up, msg);
                true
            }
            _ => false,
        }
    }

    fn broadcast_state(&self) {
        let Some(sid) = self.session_id() else { return };
        let msg = Message::State {
            state: self.state.clone(),
        };
        for c in self.conns.values() {
            let member = c
                .peer
                .as_ref()
                .and_then(|p| self.peers.get(p))
                .is_some_and(|p| p.in_session(sid));
            if member {
                let _ = c.tx.send(msg.clone());
            }
        }
    }

    fn publish(&self) {
        let mut peers: Vec<NodeInfo> = self.peers.values().cloned().collect();
        peers.sort_by(|a, b| a.seniority().cmp(&b.seniority()));
        self.view.send_replace(View {
            me: self.me.clone(),
            leader: self.leader.clone(),
            peers,
            state: self.state.clone(),
            connected: self.is_leader() || self.upstream.is_some(),
            clock_epoch: self.clock.epoch(),
        });
    }

    fn on_ended(&mut self, item_id: &str) {
        let tl = &self.state.timeline;
        if tl.playing && tl.item_id.as_deref() == Some(item_id) {
            let me = self.me.id.clone();
            let _ = self.apply(Command::Next, &me);
        }
    }

    /// Host only: change the shared state for a command sent by node
    /// `origin`, and tell every member. Returns why a command was refused.
    fn apply(&mut self, command: Command, origin: &str) -> Result<(), String> {
        let is_host = origin == self.me.id;
        if command.edits_playlist() && self.state.edit_policy == EditPolicy::HostOnly && !is_host {
            return Err("Only the host can change this playlist.".into());
        }
        let now = self.clock.local().now_us();
        let start = now + self.start_delay_us;
        let current = self.state.timeline.item_id.clone();
        let playlist = &mut self.state.playlist;
        let timeline = &mut self.state.timeline;
        match command {
            Command::Add { item } => playlist.add(item),
            Command::Remove { item_id } => {
                if current.as_deref() == Some(&item_id) {
                    match playlist.after(&item_id) {
                        Some(next) => timeline.start(next.id.clone(), 0, start),
                        None => timeline.stop(),
                    }
                }
                playlist.remove(&item_id);
            }
            Command::Move { item_id, to } => {
                playlist.move_to(&item_id, to);
            }
            Command::Play { item_id: Some(id) } => {
                if playlist.get(&id).is_some() {
                    timeline.start(id, 0, start);
                }
            }
            Command::Play { item_id: None } => match current {
                Some(_) if !timeline.playing => timeline.resume(start),
                Some(_) => {}
                None => {
                    if let Some(first) = playlist.first() {
                        timeline.start(first.id.clone(), 0, start);
                    }
                }
            },
            Command::Pause => timeline.pause(now),
            Command::Resume => timeline.resume(start),
            Command::Seek { pos_ms } => timeline.seek(pos_ms * 1000, now, start),
            Command::Next => {
                if let Some(cur) = current {
                    match playlist.after(&cur) {
                        Some(next) => timeline.start(next.id.clone(), 0, start),
                        None => timeline.stop(),
                    }
                }
            }
            Command::Prev => {
                if let Some(cur) = current {
                    let id = playlist.before(&cur).map(|i| i.id.clone()).unwrap_or(cur);
                    timeline.start(id, 0, start);
                }
            }
            Command::Stop => timeline.stop(),
            Command::SetEditPolicy { policy } => {
                if !is_host {
                    return Err("Only the host can change who may edit.".into());
                }
                self.state.edit_policy = policy;
            }
        }
        self.state.version += 1;
        self.broadcast_state();
        self.publish();
        Ok(())
    }
}
