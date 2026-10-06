use std::io::Write;
use std::net::SocketAddr;

use clap::Parser;
use nodeplayer::node::{Config, Node, View};
use nodeplayer::player::{Player, PlayerConfig};
use nodeplayer::protocol::Command;
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
commands:
  add <file or URL>   add to the shared playlist
  list                show the playlist
  play [n]            play item n (1-based), or resume / start
  pause | resume      pause or resume everywhere
  seek <seconds>      jump to a position
  next | prev         skip
  remove <n>          remove item n
  move <n> <m>        move item n to position m
  stop                stop playback
  peers               show nodes on the network
  status              show what is playing
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
        "NodePlayer \"{}\" is on the network (control port {}). Type help for commands.",
        node.info.name, node.info.control_port
    );

    if !args.no_player {
        let cfg = PlayerConfig {
            mpv_path: args.mpv.clone(),
            offset_ms: args.offset_ms,
            extra_args: args.mpv_args.clone(),
        };
        let player =
            Player::start(&cfg, node.clock.clone(), node.files.clone(), node.view()).await?;
        let report_ended = node.ended_reporter();
        tokio::spawn(async move {
            if let Err(e) = player.run(report_ended).await {
                eprintln!("player stopped: {e}");
            }
        });
    }

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    prompt();
    while let Some(line) = lines.next_line().await? {
        let view = node.view().borrow().clone();
        match run_command(&node, &view, line.trim()) {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => println!("{e}"),
        }
        prompt();
    }
    Ok(())
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
        "add" => {
            let item = node.add(rest)?;
            println!("added {}", item.title);
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
            let role = |id: &str| if id == view.leader { " (leader)" } else { "" };
            println!("this node: {}{}", view.me.name, role(&view.me.id));
            for p in &view.peers {
                println!("  {} at {}{}", p.name, p.host, role(&p.id));
            }
        }
        "status" => {
            let tl = &view.state.timeline;
            let title = tl
                .item_id
                .as_deref()
                .and_then(|id| view.state.playlist.get(id))
                .map_or("nothing", |i| i.title.as_str());
            let pos = node
                .clock
                .now_us()
                .map(|now| tl.position_at(now).max(0) / 1_000_000);
            let state = if tl.playing { "playing" } else { "paused" };
            match (tl.item_id.is_some(), pos) {
                (true, Some(p)) => println!("{state} {title} at {}:{:02}", p / 60, p % 60),
                _ => println!("playing {title}"),
            }
            let sync = match node.clock.best_sample() {
                _ if view.is_leader() => "this node is the leader".to_string(),
                Some(s) => format!(
                    "clock synced to {} (round trip {:.1} ms)",
                    view.leader_name(),
                    s.rtt_us as f64 / 1000.0
                ),
                None => format!("waiting to sync with {}", view.leader_name()),
            };
            println!("{sync}");
        }
        other => println!("unknown command {other}; type help"),
    }
    Ok(false)
}
