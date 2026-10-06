//! Pieces shared by the desktop app and the terminal version.

use std::net::SocketAddr;

use clap::Args;

use crate::node::Config;
use crate::player::PlayerConfig;

#[derive(Args, Clone, Debug)]
pub struct CommonArgs {
    /// Name shown to other PCs (defaults to this computer's name).
    #[arg(long)]
    pub name: Option<String>,
    /// Connect directly to another node's control address (host:port), for
    /// networks where automatic discovery is blocked. Repeatable.
    #[arg(long = "peer")]
    pub peers: Vec<SocketAddr>,
    /// Control port to listen on (default: any free port).
    #[arg(long, default_value_t = 0)]
    pub port: u16,
    /// Do not use mDNS discovery.
    #[arg(long)]
    pub no_mdns: bool,
    /// Do not open a player window (control only).
    #[arg(long)]
    pub no_player: bool,
    /// Play this many milliseconds ahead (or behind, if negative) to make up
    /// for speaker latency.
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    pub offset_ms: i64,
    /// Path to the mpv executable.
    #[arg(long, default_value = "mpv")]
    pub mpv: String,
    /// Extra option to pass to mpv, e.g. --mpv-arg=--fullscreen. Repeatable.
    #[arg(long = "mpv-arg", allow_hyphen_values = true)]
    pub mpv_args: Vec<String>,
}

impl CommonArgs {
    pub fn node_config(&self) -> Config {
        Config {
            name: self.name.clone().unwrap_or_else(default_name),
            control_port: self.port,
            peers: self.peers.clone(),
            mdns: !self.no_mdns,
            ..Default::default()
        }
    }

    pub fn player_config(&self) -> PlayerConfig {
        PlayerConfig {
            mpv_path: find_mpv(&self.mpv),
            offset_ms: self.offset_ms,
            extra_args: self.mpv_args.clone(),
        }
    }
}

/// Prefers an mpv placed next to this program (handy on Windows, where mpv
/// is usually unzipped rather than installed), else uses `requested`.
pub fn find_mpv(requested: &str) -> String {
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

pub fn default_name() -> String {
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
