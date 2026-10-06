# NodePlayer

Play video and music in sync across devices on your home network.

This is the first prototype: a PC version for Windows, macOS and Linux.
Install it on two or more PCs on the same network and they find each other,
share one playlist that any PC can add to, and keep play, pause, seek and the
video itself in sync. The design for the full project (phones, tablets, smart
speakers) is in the
[NodePlayer Design doc](https://claude.ai/code/artifact/815b1155-49b3-4d21-a60d-daaddec4bff6).

## Requirements

- [Rust](https://rustup.rs) 1.88 or newer
- [mpv](https://mpv.io/installation/) on your `PATH` (or pass `--mpv <path>`)

## Run it

```sh
cargo build --release
./target/release/nodeplayer        # nodeplayer.exe on Windows
```

This opens the NodePlayer window. Do the same on another PC on the same
network. Each PC starts out idle.

1. On one PC, type a name under **Start a playlist** and click **Create**.
   Tick **Only this PC can change it** if only this PC should be able to edit
   the playlist. Members can still play, pause, seek and skip either way, and
   the host can change this setting later.
2. On the other PC, the playlist appears under **Playlists on the network**.
   Click **Join**.
3. On any PC in the playlist (or only the host, for host-only), click
   **Add files…** or **Add folder…**, paste a URL, or drag files onto the
   window. Then click **Play**, or the ▶ next to an item.

Every PC in the playlist opens an mpv window and plays the movie together.
The bottom bar has play/pause, previous, next, stop and a seek bar, and
pausing, seeking or dropping a file in any mpv window does the same on every
PC in the playlist.
A file added on one PC is streamed from that PC to the others, so it does
not need to be copied first. URLs (`http://…`) and network share paths work
too. Several playlists can run on the same network at once.

### Terminal version

`./target/release/nodeplayer-cli` does the same from a terminal. Commands:
`sessions`, `create <name> [host-only]`, `join <n>`, `leave`,
`edits all|host`, `add <file|folder|URL>`, `list`, `play [n]`, `pause`,
`resume`, `seek <seconds>`, `next`, `prev`, `remove <n>`, `move <n> <m>`,
`stop`, `peers`, `status`, `quit`. Type `help` for details. Both versions
take the options below and can be in the same playlist.

Useful options:

| Option | What it does |
| --- | --- |
| `--name <name>` | Name other PCs see (defaults to the computer name) |
| `--peer <host:port>` | Connect to a node directly when mDNS is blocked; the port is printed at startup |
| `--port <port>` | Fix the control port, handy with `--peer` |
| `--offset-ms <ms>` | Play ahead (or behind, if negative) to make up for speaker latency such as Bluetooth |
| `--mpv-arg <arg>` | Pass an option to mpv, for example `--mpv-arg=--fullscreen` |
| `--no-player` | Control only, no mpv window |

Set `RUST_LOG=nodeplayer=debug` to see each node's measured sync error.

## How it works

- **Discovery.** Each node advertises `_nodeplayer._tcp` over mDNS,
  including the playlist it is in, so `sessions` can list every playlist.
- **Host.** The PC that creates a playlist hosts it. It holds the playlist
  and the playback timeline; members send it requests over a TCP link (one
  JSON message per line) and mirror the state it broadcasts. The host
  enforces the host-only setting. If the host quits or leaves, the
  longest-running remaining member takes over from its copy of the state,
  and for a host-only playlist it becomes the one PC allowed to edit.
- **Clock.** Followers measure the offset to the leader's clock with an
  NTP-style UDP exchange twice a second, trusting the sample with the lowest
  round trip.
- **Timeline.** Playback is an anchor: media position P plays at shared time
  T. Play, pause and seek publish a new anchor half a second in the future so
  every node can get ready.
- **Player.** Each node drives its own mpv over JSON IPC. Ten times a second it
  compares mpv's position with the timeline: small errors are corrected by
  changing playback speed by up to 5% (pitch is preserved), large ones by
  seeking slightly ahead and waiting for the timeline to arrive.

Code map: `src/clock.rs`, `src/timeline.rs`, `src/playlist.rs`,
`src/protocol.rs`, `src/node.rs` (networking and leader election),
`src/discovery.rs`, `src/media.rs` (file sharing over HTTP),
`src/player.rs` (mpv), `src/main.rs` (desktop window, egui),
`src/bin/nodeplayer-cli.rs` (terminal version).

## Tests

```sh
cargo test
```

The integration tests in `tests/sync.rs` run several nodes in one process
and check that they share the playlist, agree on the clock, stream shared
files with range requests, and hand over leadership when the leader leaves.

## Known limits

- No pairing or encryption yet: any NodePlayer on the network can join.
- Sync depends on mpv reporting output latency correctly. Bluetooth speakers
  usually need `--offset-ms`.
