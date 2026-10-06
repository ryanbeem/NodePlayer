//! Messages between nodes on the TCP control link, one JSON object per line.

use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::playlist::{Item, Playlist};
use crate::timeline::Timeline;

/// mDNS service type every node advertises.
pub const SERVICE_TYPE: &str = "_nodeplayer._tcp.local.";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub name: String,
    /// Unix time in ms when the node started. The oldest node leads.
    pub started_ms: u64,
    /// Address other nodes reach this node on. Filled in by whoever observed
    /// it (mDNS or the TCP peer address); empty for a node's view of itself.
    pub host: String,
    pub control_port: u16,
    pub clock_port: u16,
    pub media_port: u16,
}

impl NodeInfo {
    fn ip(&self) -> Option<IpAddr> {
        self.host.parse().ok()
    }

    pub fn control_addr(&self) -> Option<SocketAddr> {
        self.ip().map(|ip| SocketAddr::new(ip, self.control_port))
    }

    pub fn clock_addr(&self) -> Option<SocketAddr> {
        self.ip().map(|ip| SocketAddr::new(ip, self.clock_port))
    }

    pub fn media_url(&self, file_id: &str) -> Option<String> {
        self.ip().map(|ip| {
            format!(
                "http://{}/media/{file_id}",
                SocketAddr::new(ip, self.media_port)
            )
        })
    }

    /// Sort key for leader election: earliest start wins, id breaks ties.
    pub fn seniority(&self) -> (u64, &str) {
        (self.started_ms, &self.id)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    Add {
        item: Item,
    },
    Remove {
        item_id: String,
    },
    Move {
        item_id: String,
        to: usize,
    },
    /// Play a given item from the start, or with no id resume / start the first.
    Play {
        item_id: Option<String>,
    },
    Pause,
    Resume,
    Seek {
        pos_ms: i64,
    },
    Next,
    Prev,
    Stop,
}

/// Everything followers mirror from the leader.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SharedState {
    pub leader: String,
    pub version: u64,
    pub playlist: Playlist,
    pub timeline: Timeline,
    /// Nodes the leader is linked to, so followers can reach each other's
    /// shared files even without mDNS.
    pub members: Vec<NodeInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// First message on every connection, both directions.
    Hello {
        info: NodeInfo,
        peers: Vec<NodeInfo>,
    },
    /// Follower asks the leader to apply a command.
    Request { command: Command },
    /// Leader publishes its state.
    State { state: SharedState },
    /// A node's player reached the end of an item.
    Ended { item_id: String },
}

pub async fn write_message<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    msg: &Message,
) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(msg)?;
    line.push(b'\n');
    w.write_all(&line).await
}

/// Reads messages until the stream ends, skipping lines that don't parse.
pub struct MessageReader<R> {
    lines: tokio::io::Lines<BufReader<R>>,
}

impl<R: tokio::io::AsyncRead + Unpin> MessageReader<R> {
    pub fn new(r: R) -> Self {
        Self {
            lines: BufReader::new(r).lines(),
        }
    }

    pub async fn next(&mut self) -> Option<Message> {
        loop {
            let line = self.lines.next_line().await.ok()??;
            match serde_json::from_str(&line) {
                Ok(msg) => return Some(msg),
                Err(e) => tracing::warn!("ignoring bad message: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn messages_round_trip_over_a_stream() {
        let (a, b) = tokio::io::duplex(4096);
        let (_, mut w) = tokio::io::split(a);
        let (r, _) = tokio::io::split(b);
        let sent = Message::Request {
            command: Command::Seek { pos_ms: 1234 },
        };
        write_message(&mut w, &sent).await.unwrap();
        let mut reader = MessageReader::new(r);
        assert_eq!(reader.next().await, Some(sent));
    }

    #[test]
    fn older_node_is_senior() {
        let mk = |id: &str, started_ms| NodeInfo {
            id: id.into(),
            name: id.into(),
            started_ms,
            host: String::new(),
            control_port: 0,
            clock_port: 0,
            media_port: 0,
        };
        assert!(mk("z", 1).seniority() < mk("a", 2).seniority());
        assert!(mk("a", 1).seniority() < mk("b", 1).seniority());
    }
}
