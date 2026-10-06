//! Runs several nodes in one process over loopback and checks they agree.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use nodeplayer::node::{Config, Node, View};
use nodeplayer::protocol::{Command, EditPolicy};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

async fn start(name: &str, started_ms: u64, peers: Vec<SocketAddr>) -> Node {
    Node::start(Config {
        name: name.into(),
        bind_ip: LOCALHOST,
        peers,
        mdns: false,
        started_ms: Some(started_ms),
        ..Default::default()
    })
    .await
    .unwrap()
}

fn control_addr(node: &Node) -> SocketAddr {
    SocketAddr::new(LOCALHOST, node.info.control_port)
}

async fn wait_for(node: &Node, what: &str, pred: impl Fn(&View) -> bool) -> View {
    let mut rx = node.view();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        {
            let v = rx.borrow_and_update();
            if pred(&v) {
                return v.clone();
            }
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() || tokio::time::timeout(left, rx.changed()).await.is_err() {
            panic!(
                "{} timed out waiting for {what}: {:#?}",
                node.info.name,
                *rx.borrow()
            );
        }
    }
}

/// `host` creates a playlist and every node in `members` joins it.
async fn session(host: &Node, policy: EditPolicy, members: &[&Node]) {
    host.create("movie night", policy);
    let v = wait_for(host, "session", |v| v.session().is_some()).await;
    let id = v.session().unwrap().id.clone();
    for m in members {
        wait_for(m, "the session", |v| {
            v.sessions().iter().any(|s| s.id == id)
        })
        .await;
        m.join(&id);
        wait_for(m, "host link", |v| v.leader == host.info.id && v.connected).await;
        wait_for(host, "member", |v| {
            v.peers
                .iter()
                .any(|p| p.id == m.info.id && p.in_session(&id))
        })
        .await;
    }
}

async fn wait_synced(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.clock.is_synced() {
        assert!(
            Instant::now() < deadline,
            "{} never synced its clock",
            node.info.name
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn nodes_share_playlist_clock_and_timeline() {
    let a = start("a", 1000, vec![]).await;
    let b = start("b", 2000, vec![control_addr(&a)]).await;

    session(&a, EditPolicy::Anyone, &[&b]).await;
    wait_synced(&b).await;

    // Clocks agree to within a couple of milliseconds over loopback.
    let shared = b.clock.now_us().unwrap();
    let truth = a.clock.local().now_us();
    assert!(
        (shared - truth).abs() < 3_000,
        "clocks differ by {}us",
        shared - truth
    );

    // Either node can add to the playlist.
    a.add("http://example.com/one.mp4").unwrap();
    b.add("http://example.com/two.mp4").unwrap();
    let va = wait_for(&a, "two items", |v| v.state.playlist.items.len() == 2).await;
    let vb = wait_for(&b, "a's playlist", |v| v.state.version == va.state.version).await;
    assert_eq!(va.state.playlist, vb.state.playlist);

    // A follower's play command reaches everyone, scheduled in the future.
    b.command(Command::Play { item_id: None });
    let va = wait_for(&a, "playing", |v| v.state.timeline.playing).await;
    let vb = wait_for(&b, "playing", |v| v.state.timeline.playing).await;
    assert_eq!(va.state.timeline, vb.state.timeline);
    assert_eq!(
        va.state.timeline.item_id.as_deref(),
        Some(va.state.playlist.items[0].id.as_str())
    );
    assert!(va.state.timeline.anchor_clock_us > a.clock.local().now_us() - 500_000);

    // Pause and seek from the follower too.
    b.command(Command::Pause);
    wait_for(&a, "paused", |v| !v.state.timeline.playing).await;
    b.command(Command::Seek { pos_ms: 90_000 });
    let vb = wait_for(&b, "seek", |v| v.state.timeline.anchor_pos_us == 90_000_000).await;
    assert!(!vb.state.timeline.playing);

    b.command(Command::Next);
    let va = wait_for(&a, "second item", |v| {
        v.state.timeline.item_id.as_deref() == Some(v.state.playlist.items[1].id.as_str())
    })
    .await;
    assert!(va.state.timeline.playing);
}

#[tokio::test]
async fn shared_files_stream_with_range_requests() {
    let a = start("a", 1000, vec![]).await;
    let b = start("b", 2000, vec![control_addr(&a)]).await;
    session(&a, EditPolicy::Anyone, &[&b]).await;

    let dir = std::env::temp_dir().join(format!("nodeplayer-test-{}", b.info.id));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("clip.bin");
    std::fs::write(&path, b"0123456789").unwrap();
    let item = b.add(path.to_str().unwrap()).unwrap();

    let va = wait_for(&a, "b's file", |v| v.state.playlist.get(&item.id).is_some()).await;
    let url = va.resolve(&item, &a.files).expect("a can reach b's file");
    let rest = url.strip_prefix("http://").unwrap();
    let (host, path_part) = rest.split_once('/').unwrap();

    let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
    let req = format!(
        "GET /{path_part} HTTP/1.1\r\nHost: {host}\r\nRange: bytes=2-5\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut resp = String::new();
    s.read_to_string(&mut resp).await.unwrap();
    assert!(resp.starts_with("HTTP/1.1 206"), "{resp}");
    assert!(resp.ends_with("2345"), "{resp}");
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn next_oldest_node_takes_over_when_leader_leaves() {
    let a = start("a", 1000, vec![]).await;
    let b = start("b", 2000, vec![control_addr(&a)]).await;
    // c only knows a; it learns about b from a.
    let c = start("c", 3000, vec![control_addr(&a)]).await;
    session(&a, EditPolicy::Anyone, &[&b, &c]).await;
    wait_synced(&b).await;

    a.add("http://example.com/movie.mp4").unwrap();
    a.command(Command::Play { item_id: None });
    let vb = wait_for(&b, "playing", |v| v.state.timeline.playing).await;
    wait_for(&c, "b as member", |v| {
        v.peers.iter().any(|p| p.id == b.info.id)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Where b thinks playback is, in b's own clock terms.
    let offset = b.clock.offset_us().unwrap();
    let local_before = b.clock.local().now_us();
    let pos_before = vb.state.timeline.position_at(local_before + offset);

    drop(a);

    let vb = wait_for(&b, "b to lead", |v| v.is_leader()).await;
    wait_for(&c, "b as leader", |v| v.leader == b.info.id && v.connected).await;
    wait_synced(&c).await;

    // The playlist survived and playback carried on from the same place.
    assert_eq!(vb.state.playlist.items.len(), 1);
    assert!(vb.state.timeline.playing);
    let local_now = b.clock.local().now_us();
    let expected = pos_before + (local_now - local_before);
    let actual = vb.state.timeline.position_at(local_now);
    assert!(
        (actual - expected).abs() < 5_000,
        "position jumped by {}us",
        actual - expected
    );
    let vc = wait_for(&c, "b's state", |v| v.state.version >= vb.state.version).await;
    assert_eq!(vc.state.playlist, vb.state.playlist);
}

#[tokio::test]
async fn idle_nodes_ignore_a_session_until_they_join() {
    let a = start("a", 1000, vec![]).await;
    let b = start("b", 2000, vec![control_addr(&a)]).await;
    a.create("party", EditPolicy::Anyone);
    let vb = wait_for(&b, "the session", |v| v.sessions().len() == 1).await;
    assert_eq!(vb.sessions()[0].name, "party");
    assert_eq!(vb.sessions()[0].host_name, "a");

    a.add("http://example.com/song.mp3").unwrap();
    a.command(Command::Play { item_id: None });
    wait_for(&a, "playing", |v| v.state.timeline.playing).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let vb = b.view().borrow().clone();
    assert!(vb.session().is_none());
    assert!(vb.state.playlist.items.is_empty());
    assert!(!vb.state.timeline.playing);

    // Joining picks up the playlist already playing; leaving drops it.
    b.join(&vb.sessions()[0].id);
    wait_for(&b, "playing", |v| {
        v.state.timeline.playing && v.state.playlist.items.len() == 1
    })
    .await;
    b.leave();
    let vb = wait_for(&b, "idle", |v| v.session().is_none()).await;
    assert!(!vb.state.timeline.playing);
    wait_for(&a, "b gone from the session", |v| {
        v.sessions()[0].members == 1
    })
    .await;
}

#[tokio::test]
async fn host_only_playlists_refuse_edits_from_members() {
    let a = start("a", 1000, vec![]).await;
    let b = start("b", 2000, vec![control_addr(&a)]).await;
    session(&a, EditPolicy::HostOnly, &[&b]).await;
    let mut notices = b.notices();

    // b's own check refuses straight away.
    wait_for(&b, "the policy", |v| {
        v.state.edit_policy == EditPolicy::HostOnly
    })
    .await;
    b.add("http://example.com/b.mp4").unwrap();
    let text = tokio::time::timeout(Duration::from_secs(5), notices.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(text.contains("Only the host"), "{text}");

    // The host can edit, and members can still control playback.
    a.add("http://example.com/a.mp4").unwrap();
    wait_for(&b, "a's item", |v| v.state.playlist.items.len() == 1).await;
    b.command(Command::Play { item_id: None });
    wait_for(&a, "playing", |v| v.state.timeline.playing).await;
    assert_eq!(a.view().borrow().state.playlist.items.len(), 1);

    // Opening the playlist up lets b add.
    a.command(Command::SetEditPolicy {
        policy: EditPolicy::Anyone,
    });
    wait_for(&b, "open policy", |v| {
        v.state.edit_policy == EditPolicy::Anyone
    })
    .await;
    b.add("http://example.com/b.mp4").unwrap();
    wait_for(&a, "b's item", |v| v.state.playlist.items.len() == 2).await;
}
