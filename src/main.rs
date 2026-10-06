use std::io::Write;
use std::net::SocketAddr;

use clap::Parser;
use nodeplayer::node::{Config, Node, View};
use nodeplayer::player::{Player, PlayerConfig, PlayerEvent};
use nodeplayer::protocol::{Command, EditPolicy};
use tokio::io::{AsyncBufReadExt, BufReader};

/// Plays media in sync across PCs on the same network.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Name shown to other nodes (defaults to this computer's name).
    #[arg(long)]
    name: Option<String>,
    /// Connect directly to another node's control address (host:port), for
    /// networks where automatic discovery is blocked. Repeatable.
    #[arg(long = "peer")]
    peers: Vec<SocketAddr>,
    /// Control port to listen on (default: any free port).
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Do not use mDNS discovery.
    #[arg(long)]
    no_mdns: bool,
    /// Do not open a player window (control only).
    #[arg(long)]
    no_player: bool,
    /// Play this many milliseconds ahead (or behind, if negative) to make up
    /// for speaker latency.
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    offset_ms: i64,
    /// Path to the mpv executable.
    #[arg(long, default_value = "mpv")]
    mpv: String,
    /// Extra option to pass to mpv, e.g. --mpv-arg=--fullscreen. Repeatable.
    #[arg(long = "mpv-arg", allow_hyphen_values = true)]
    mpv_args: Vec<String>,
}

const HELP: &str = "\
playlists:
  sessions            show playlists on the network
  create <name> [host-only]
                      start a playlist that others can join; with host-only,
                      only this PC can change the playlist
  join <n or name>    join a playlist from the sessions list
  leave               leave the playlist
  edits all|host      host only: who may change the playlist
playback (in a playlist):
  add <file, folder or URL>
                      add to the shared playlist (a folder adds all its
                      audio and video files, subfolders included)
  list                show the playlist
  play [n]            play item n (1-based), or resume / start
  pause | resume      pause or resume everywhere
  seek <seconds>      jump to a position
  next | prev         skip
  remove <n>          remove item n
  move <n> <m>        move item n to position m
  stop                stop playback
  peers               show PCs on the network
  status              show the playlist and what is playing
  quit";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nodeplayer=info".into()),
        )
        .with_target(false)
        .init();
    let args = Args::parse();
    let name = args.name.clone().unwrap_or_else(default_name);

    let node = Node::start(Config {
        name,
        control_port: args.port,
        peers: args.peers.clone(),
        mdns: !args.no_mdns,
        ..Default::default()
    })
    .await?;
    println!(
        "NodePlayer \"{}\" is on the network (control port {}).",
        node.info.name, node.info.control_port
    );
    println!(
        "Type sessions to see playlists, join <n> to join one, create <name> to start one, or help."
    );

    let mut notices = node.notices();
    tokio::spawn(async move {
        while let Ok(text) = notices.recv().await {
            println!("{text}");
        }
    });

    let (player_tx, mut player_rx) = tokio::sync::mpsc::unbounded_channel();
    if !args.no_player {
        let cfg = PlayerConfig {
            mpv_path: find_mpv(&args.mpv),
            offset_ms: args.offset_ms,
            extra_args: args.mpv_args.clone(),
        };
        let player =
            Player::start(&cfg, node.clock.clone(), node.files.clone(), node.view()).await?;
        tokio::spawn(async move {
            let result = player.run(move |event| {
                let _ = player_tx.send(event);
            });
            if let Err(e) = result.await {
                eprintln!("player stopped: {e}");
            }
        });
    }
    let report_ended = node.ended_reporter();

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    prompt();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { break };
                let view = node.view().borrow().clone();
                match run_command(&node, &view, line.trim()) {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(e) => println!("{e}"),
                }
                prompt();
            }
            Some(event) = player_rx.recv() => match event {
                PlayerEvent::Ended(id) => report_ended(id),
                PlayerEvent::Command(command) => node.command(command),
                PlayerEvent::Dropped(path) => {
                    let view = node.view().borrow().clone();
                    if view.session().is_none() {
                        println!("join or create a playlist first; the dropped file plays only here");
                    } else if !view.can_edit() {
                        println!("only the host can change this playlist");
                    } else {
                        match node.add(&path) {
                            Ok(item) => {
                                println!("added {}", item.title);
                                node.command(Command::Play { item_id: Some(item.id) });
                            }
                            Err(e) => println!("{e}"),
                        }
                    }
                }
            },
        }
    }
    Ok(())
}

/// Prefers an mpv placed next to this program (handy on Windows, where mpv
/// is usually unzipped rather than installed), else uses `requested`.
fn find_mpv(requested: &str) -> String {
    if requested == "mpv" {
        let name = if cfg!(windows) { "mpv.exe" } else { "mpv" };
        let beside = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.join(name)))
            .filter(|p| p.is_file());
        if let Some(path) = beside {
            return path.to_string_lossy().into_owned();
        }
    }
    requested.to_string()
}

fn prompt() {
    print!("> ");
    let _ = std::io::stdout().flush();
}

fn default_name() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "nodeplayer".into())
}

/// Returns true when the user asked to quit.
fn run_command(node: &Node, view: &View, line: &str) -> anyhow::Result<bool> {
    let (cmd, rest) = line
        .split_once(' ')
        .map_or((line, ""), |(c, r)| (c, r.trim()));
    let item_at = |n: &str| -> anyhow::Result<String> {
        let n: usize = n.parse()?;
        view.state
            .playlist
            .items
            .get(n.wrapping_sub(1))
            .map(|i| i.id.clone())
            .ok_or_else(|| anyhow::anyhow!("no item {n}"))
    };
    match cmd {
        "" => {}
        "help" | "?" => println!("{HELP}"),
        "quit" | "exit" => return Ok(true),
        "sessions" => {
            let sessions = view.sessions();
            if sessions.is_empty() {
                println!("no playlists on the network yet; start one with create <name>");
            }
            let current = view.session().map(|s| s.id.as_str());
            for (i, s) in sessions.iter().enumerate() {
                let mark = if Some(s.id.as_str()) == current {
                    ">"
                } else {
                    " "
                };
                let pcs = if s.members == 1 { "PC" } else { "PCs" };
                println!(
                    "{mark} {:>2}. {}  (host {}, {} {pcs})",
                    i + 1,
                    s.name,
                    s.host_name,
                    s.members
                );
            }
        }
        "create" | "new" => {
            let (name, policy) = match rest.strip_suffix("host-only") {
                Some(name) => (name.trim(), EditPolicy::HostOnly),
                None => (rest, EditPolicy::Anyone),
            };
            anyhow::ensure!(!name.is_empty(), "usage: create <name> [host-only]");
            node.create(name, policy);
            let who = match policy {
                EditPolicy::Anyone => "any PC that joins can change it",
                EditPolicy::HostOnly => "only this PC can change it",
            };
            println!("created playlist {name}; {who}");
        }
        "join" => {
            let sessions = view.sessions();
            let found = match rest.parse::<usize>() {
                Ok(n) => sessions.get(n.wrapping_sub(1)),
                Err(_) => sessions.iter().find(|s| s.name.eq_ignore_ascii_case(rest)),
            };
            let s = found
                .ok_or_else(|| anyhow::anyhow!("no playlist {rest}; type sessions to list them"))?;
            node.join(&s.id);
            println!("joining {} (host {})", s.name, s.host_name);
        }
        "leave" => node.leave(),
        "edits" => {
            let policy = match rest {
                "all" | "anyone" => EditPolicy::Anyone,
                "host" | "host-only" => EditPolicy::HostOnly,
                _ => anyhow::bail!("usage: edits all|host"),
            };
            anyhow::ensure!(view.is_host(), "only the host can change who may edit");
            node.command(Command::SetEditPolicy { policy });
        }
        "add" | "remove" | "rm" | "move" | "mv" if view.session().is_some() && !view.can_edit() => {
            anyhow::bail!("only the host can change this playlist")
        }
        "add" => {
            anyhow::ensure!(view.session().is_some(), "join or create a playlist first");
            // Paths dragged into a terminal often arrive quoted.
            let path = rest.trim_matches(|c| c == '\'' || c == '"');
            if std::path::Path::new(path).is_dir() {
                let items = node.add_folder(path)?;
                println!("added {} files from {path}", items.len());
            } else {
                let item = node.add(path)?;
                println!("added {}", item.title);
            }
        }
        "list" | "ls" => {
            if view.state.playlist.items.is_empty() {
                println!("playlist is empty");
            }
            for (i, item) in view.state.playlist.items.iter().enumerate() {
                let mark = if view.state.timeline.item_id.as_deref() == Some(&item.id) {
                    ">"
                } else {
                    " "
                };
                println!(
                    "{mark} {:>2}. {}  (added by {})",
                    i + 1,
                    item.title,
                    item.added_by
                );
            }
        }
        "play" => {
            let item_id = if rest.is_empty() {
                None
            } else {
                Some(item_at(rest)?)
            };
            node.command(Command::Play { item_id });
        }
        "pause" => node.command(Command::Pause),
        "resume" => node.command(Command::Resume),
        "seek" => {
            let secs: f64 = rest.parse()?;
            node.command(Command::Seek {
                pos_ms: (secs * 1000.0) as i64,
            });
        }
        "next" => node.command(Command::Next),
        "prev" => node.command(Command::Prev),
        "stop" => node.command(Command::Stop),
        "remove" | "rm" => node.command(Command::Remove {
            item_id: item_at(rest)?,
        }),
        "move" | "mv" => {
            let (a, b) = rest
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("usage: move <n> <m>"))?;
            let to: usize = b.trim().parse()?;
            node.command(Command::Move {
                item_id: item_at(a)?,
                to: to.saturating_sub(1),
            });
        }
        "peers" => {
            let describe = |p: &nodeplayer::protocol::NodeInfo| match &p.session {
                Some(s) if s.host == p.id => format!("hosting {}", s.name),
                Some(s) => format!("in {}", s.name),
                None => "not in a playlist".to_string(),
            };
            println!("this PC: {} ({})", view.me.name, describe(&view.me));
            for p in &view.peers {
                println!("  {} at {} ({})", p.name, p.host, describe(p));
            }
        }
        "status" => {
            let Some(session) = view.session() else {
                println!("not in a playlist; type sessions, join <n> or create <name>");
                return Ok(false);
            };
            let editors = match view.state.edit_policy {
                EditPolicy::Anyone => "anyone can edit",
                EditPolicy::HostOnly => "only the host can edit",
            };
            println!(
                "playlist {} (host {}, {editors})",
                session.name,
                view.leader_name()
            );
            let tl = &view.state.timeline;
            match tl
                .item_id
                .as_deref()
                .and_then(|id| view.state.playlist.get(id))
            {
                None => println!("nothing playing"),
                Some(item) => {
                    let state = if tl.playing { "playing" } else { "paused" };
                    match node.clock.now_in_epoch(view.clock_epoch) {
                        Some(now) => {
                            let p = tl.position_at(now).max(0) / 1_000_000;
                            println!("{state} {} at {}:{:02}", item.title, p / 60, p % 60);
                        }
                        None => println!("{state} {}", item.title),
                    }
                }
            }
            if !view.is_leader() {
                match node.clock.best_sample() {
                    Some(s) => println!(
                        "clock synced to {} (round trip {:.1} ms)",
                        view.leader_name(),
                        s.rtt_us as f64 / 1000.0
                    ),
                    None => println!("waiting to sync with {}", view.leader_name()),
                }
            }
        }
        other => println!("unknown command {other}; type help"),
    }
    Ok(false)
}
