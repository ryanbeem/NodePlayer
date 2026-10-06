//! Finds other nodes on the LAN with mDNS / DNS-SD.

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::sync::mpsc;

use crate::protocol::{NodeInfo, SERVICE_TYPE};

pub enum Discovery {
    Found(NodeInfo),
    Lost(String),
}

/// Advertises `me` and reports other nodes until the receiver is dropped.
/// Returns the daemon, which keeps the advertisement alive while it exists.
pub fn start(
    me: &NodeInfo,
    events: mpsc::UnboundedSender<Discovery>,
) -> anyhow::Result<ServiceDaemon> {
    let daemon = ServiceDaemon::new()?;
    let host = format!("nodeplayer-{}.local.", &me.id[..8.min(me.id.len())]);
    let props = [
        ("id", me.id.clone()),
        ("name", me.name.clone()),
        ("started", me.started_ms.to_string()),
        ("clock", me.clock_port.to_string()),
        ("media", me.media_port.to_string()),
    ];
    let service = ServiceInfo::new(SERVICE_TYPE, &me.id, &host, "", me.control_port, &props[..])?
        .enable_addr_auto();
    daemon.register(service)?;

    let browse = daemon.browse(SERVICE_TYPE)?;
    let my_id = me.id.clone();
    tokio::spawn(async move {
        while let Ok(event) = browse.recv_async().await {
            let msg = match event {
                ServiceEvent::ServiceResolved(info) => {
                    let get =
                        |k: &str| info.get_property_val_str(k).unwrap_or_default().to_string();
                    let Some(ip) = info.get_addresses_v4().into_iter().next() else {
                        continue;
                    };
                    let node = NodeInfo {
                        id: get("id"),
                        name: get("name"),
                        started_ms: get("started").parse().unwrap_or(u64::MAX),
                        host: ip.to_string(),
                        control_port: info.get_port(),
                        clock_port: get("clock").parse().unwrap_or(0),
                        media_port: get("media").parse().unwrap_or(0),
                    };
                    if node.id.is_empty() || node.id == my_id {
                        continue;
                    }
                    Discovery::Found(node)
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    let id = fullname
                        .trim_end_matches(SERVICE_TYPE)
                        .trim_end_matches('.')
                        .to_string();
                    if id == my_id {
                        continue;
                    }
                    Discovery::Lost(id)
                }
                _ => continue,
            };
            if events.send(msg).is_err() {
                break;
            }
        }
    });
    Ok(daemon)
}
