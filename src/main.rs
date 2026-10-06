//! NodePlayer desktop app: playlists and playback controls in a window,
//! video in mpv.

// No console window behind the app on Windows.
#![cfg_attr(windows, windows_subsystem = "windows")]

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use clap::Parser;
use eframe::egui;
use nodeplayer::node::{Node, View};
use nodeplayer::player::{Player, PlayerEvent};
use nodeplayer::protocol::{Command, EditPolicy};
use nodeplayer::util::CommonArgs;
use tokio::sync::{broadcast, watch};

/// Plays media in sync across PCs on the same network.
#[derive(Parser)]
#[command(version)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}

/// How long a message stays in the status line.
const MESSAGE_TTL: Duration = Duration::from_secs(6);

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nodeplayer=info".into()),
        )
        .with_target(false)
        .init();
    let args = Args::parse().common;

    let runtime = tokio::runtime::Runtime::new()?;
    let node = runtime.block_on(Node::start(args.node_config()))?;

    let (player_tx, player_rx) = mpsc::channel();
    let mut duration = None;
    let mut messages = VecDeque::new();
    if !args.no_player {
        match runtime.block_on(Player::start(
            &args.player_config(),
            node.clock.clone(),
            node.files.clone(),
            node.view(),
        )) {
            Ok(player) => {
                duration = Some(player.duration());
                runtime.spawn(async move {
                    let result = player.run(move |event| {
                        let _ = player_tx.send(event);
                    });
                    if let Err(e) = result.await {
                        tracing::error!("player stopped: {e}");
                    }
                });
            }
            Err(e) => {
                messages.push_back((Instant::now() + Duration::from_secs(3600), format!("{e:#}")))
            }
        }
    }

    let app = App {
        view: node.view(),
        notices: node.notices(),
        ended: Box::new(node.ended_reporter()),
        node,
        _runtime: runtime,
        player_events: player_rx,
        duration,
        messages,
        picks: mpsc::channel(),
        new_name: String::new(),
        host_only: false,
        url: String::new(),
        seek_to: None,
    };
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("NodePlayer")
            .with_inner_size([960.0, 600.0])
            .with_min_inner_size([640.0, 400.0]),
        ..Default::default()
    };
    eframe::run_native("NodePlayer", options, Box::new(|_cc| Ok(Box::new(app))))
        .map_err(|e| anyhow::anyhow!("could not open the window: {e}"))
}

struct App {
    node: Node,
    /// Keeps networking and the player running while the window is open.
    _runtime: tokio::runtime::Runtime,
    view: watch::Receiver<View>,
    notices: broadcast::Receiver<String>,
    ended: Box<dyn Fn(String)>,
    player_events: mpsc::Receiver<PlayerEvent>,
    duration: Option<watch::Receiver<Option<f64>>>,
    /// Status-line messages and when they expire.
    messages: VecDeque<(Instant, String)>,
    /// Files and folders picked in a file dialog (which runs on its own thread).
    picks: (mpsc::Sender<Vec<PathBuf>>, mpsc::Receiver<Vec<PathBuf>>),
    new_name: String,
    host_only: bool,
    url: String,
    /// Slider position while the user drags it, in seconds.
    seek_to: Option<f64>,
}

impl App {
    fn say(&mut self, text: impl Into<String>) {
        self.messages
            .push_back((Instant::now() + MESSAGE_TTL, text.into()));
        while self.messages.len() > 3 {
            self.messages.pop_front();
        }
    }

    /// Adds files, folders or URLs to the playlist.
    fn add_paths(&mut self, paths: Vec<PathBuf>) {
        for path in paths {
            let text = path.to_string_lossy().into_owned();
            let result = if path.is_dir() {
                self.node.add_folder(&text).map(|items| items.len())
            } else {
                self.node.add(&text).map(|_| 1)
            };
            match result {
                Ok(n) if n > 1 => self.say(format!("Added {n} files from {text}")),
                Ok(_) => {}
                Err(e) => self.say(format!("{e:#}")),
            }
        }
    }

    fn pick(&self, folder: bool) {
        let tx = self.picks.0.clone();
        std::thread::spawn(move || {
            let dialog = rfd::FileDialog::new();
            let picked = if folder {
                dialog.pick_folder().map(|f| vec![f])
            } else {
                dialog.pick_files()
            };
            if let Some(paths) = picked {
                let _ = tx.send(paths);
            }
        });
    }

    /// Handles everything that arrived since the last frame.
    fn drain(&mut self, ctx: &egui::Context) {
        while let Ok(text) = self.notices.try_recv() {
            self.say(text);
        }
        while let Ok(event) = self.player_events.try_recv() {
            match event {
                PlayerEvent::Ended(id) => (self.ended)(id),
                PlayerEvent::Command(command) => self.node.command(command),
                PlayerEvent::Dropped(path) => self.add_dropped(vec![PathBuf::from(path)], true),
            }
        }
        while let Ok(paths) = self.picks.1.try_recv() {
            self.add_paths(paths);
        }
        let dropped: Vec<PathBuf> = ctx.input(|i| {
            i.raw
                .dropped_files
                .iter()
                .filter_map(|f| f.path.clone())
                .collect()
        });
        if !dropped.is_empty() {
            self.add_dropped(dropped, false);
        }
        let now = Instant::now();
        self.messages.retain(|(until, _)| *until > now);
    }

    /// Files dropped on this window or on mpv's: add them, and for mpv's,
    /// play straight away the way mpv would have on its own.
    fn add_dropped(&mut self, paths: Vec<PathBuf>, play: bool) {
        let view = self.view.borrow().clone();
        if view.session().is_none() {
            self.say("Join or start a playlist first.");
        } else if !view.can_edit() {
            self.say("Only the host can change this playlist.");
        } else if play && paths.len() == 1 {
            match self.node.add(&paths[0].to_string_lossy()) {
                Ok(item) => self.node.command(Command::Play {
                    item_id: Some(item.id),
                }),
                Err(e) => self.say(format!("{e:#}")),
            }
        } else {
            self.add_paths(paths);
        }
    }

    fn sessions_panel(&mut self, ui: &mut egui::Ui, view: &View) {
        ui.heading("Playlists");
        ui.add_space(4.0);
        let sessions = view.sessions();
        if sessions.is_empty() {
            ui.weak("None on the network yet.");
        }
        let current = view.session().map(|s| s.id.clone());
        for s in &sessions {
            ui.horizontal(|ui| {
                ui.vertical(|ui| {
                    let mine = current.as_deref() == Some(s.id.as_str());
                    let name = egui::RichText::new(&s.name).strong();
                    ui.label(if mine {
                        name.color(ui.visuals().hyperlink_color)
                    } else {
                        name
                    });
                    let pcs = if s.members == 1 { "PC" } else { "PCs" };
                    ui.weak(format!("host {} · {} {pcs}", s.host_name, s.members));
                });
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if current.as_deref() == Some(s.id.as_str()) {
                        if ui.button("Leave").clicked() {
                            self.node.leave();
                        }
                    } else if ui.button("Join").clicked() {
                        self.node.join(&s.id);
                    }
                });
            });
            ui.add_space(4.0);
        }

        ui.separator();
        ui.label(egui::RichText::new("Start a playlist").strong());
        let field = ui.add(
            egui::TextEdit::singleline(&mut self.new_name).hint_text("Name, e.g. Movie night"),
        );
        ui.checkbox(&mut self.host_only, "Only this PC can change it");
        let name = self.new_name.trim().to_string();
        let enter = field.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
        if (ui
            .add_enabled(!name.is_empty(), egui::Button::new("Create"))
            .clicked()
            || enter)
            && !name.is_empty()
        {
            let policy = if self.host_only {
                EditPolicy::HostOnly
            } else {
                EditPolicy::Anyone
            };
            self.node.create(&name, policy);
            self.new_name.clear();
        }

        ui.separator();
        ui.label(egui::RichText::new("PCs on the network").strong());
        ui.label(format!("{} (this PC)", view.me.name));
        if view.peers.is_empty() {
            ui.weak("No other PCs found yet.");
        }
        for p in &view.peers {
            ui.weak(format!("{} · {}", p.name, p.host));
        }
    }

    fn playlist_panel(&mut self, ui: &mut egui::Ui, view: &View) {
        let Some(session) = view.session() else {
            ui.add_space(40.0);
            ui.vertical_centered(|ui| {
                ui.heading("Not in a playlist");
                ui.label("Join one from the list on the left, or start your own.");
            });
            return;
        };
        ui.heading(&session.name);
        ui.horizontal(|ui| {
            let host = if view.is_leader() {
                "you".to_string()
            } else {
                view.leader_name()
            };
            ui.weak(format!("Hosted by {host}"));
            if view.is_leader() {
                let mut host_only = view.state.edit_policy == EditPolicy::HostOnly;
                if ui
                    .checkbox(&mut host_only, "Only I can change the playlist")
                    .changed()
                {
                    let policy = if host_only {
                        EditPolicy::HostOnly
                    } else {
                        EditPolicy::Anyone
                    };
                    self.node.command(Command::SetEditPolicy { policy });
                }
            } else if view.state.edit_policy == EditPolicy::HostOnly {
                ui.weak("· only the host can change the playlist");
            }
            if !view.connected {
                ui.colored_label(ui.visuals().warn_fg_color, "· connecting to the host…");
            }
        });
        ui.add_space(6.0);

        let can_edit = view.can_edit();
        if can_edit {
            ui.horizontal(|ui| {
                if ui.button("Add files…").clicked() {
                    self.pick(false);
                }
                if ui.button("Add folder…").clicked() {
                    self.pick(true);
                }
                let field = ui.add(
                    egui::TextEdit::singleline(&mut self.url)
                        .hint_text("or paste a URL")
                        .desired_width(240.0),
                );
                let enter = field.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                let url = self.url.trim().to_string();
                if (ui
                    .add_enabled(!url.is_empty(), egui::Button::new("Add"))
                    .clicked()
                    || enter)
                    && !url.is_empty()
                {
                    match self.node.add(&url) {
                        Ok(_) => self.url.clear(),
                        Err(e) => self.say(format!("{e:#}")),
                    }
                }
            });
            ui.weak("You can also drop files or folders onto this window.");
        }
        ui.separator();

        let items = &view.state.playlist.items;
        if items.is_empty() {
            ui.weak("The playlist is empty.");
            return;
        }
        let current = view.state.timeline.item_id.as_deref();
        egui::ScrollArea::vertical()
            .auto_shrink(false)
            .show(ui, |ui| {
                for (i, item) in items.iter().enumerate() {
                    ui.horizontal(|ui| {
                        let playing = current == Some(item.id.as_str());
                        if playing {
                            ui.label(egui::RichText::new("▶").color(ui.visuals().hyperlink_color));
                        } else if ui.small_button("▶").on_hover_text("Play this").clicked() {
                            self.node.command(Command::Play {
                                item_id: Some(item.id.clone()),
                            });
                        }
                        let title = egui::RichText::new(format!("{}. {}", i + 1, item.title));
                        ui.label(if playing { title.strong() } else { title });
                        ui.weak(format!("added by {}", item.added_by));
                        if can_edit {
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui.small_button("Remove").clicked() {
                                        self.node.command(Command::Remove {
                                            item_id: item.id.clone(),
                                        });
                                    }
                                    if i + 1 < items.len()
                                        && ui.small_button("⬇").on_hover_text("Move down").clicked()
                                    {
                                        self.node.command(Command::Move {
                                            item_id: item.id.clone(),
                                            to: i + 1,
                                        });
                                    }
                                    if i > 0
                                        && ui.small_button("⬆").on_hover_text("Move up").clicked()
                                    {
                                        self.node.command(Command::Move {
                                            item_id: item.id.clone(),
                                            to: i - 1,
                                        });
                                    }
                                },
                            );
                        }
                    });
                }
            });
    }

    fn transport(&mut self, ui: &mut egui::Ui, view: &View) {
        ui.add_space(6.0);
        if view.session().is_some() {
            let tl = &view.state.timeline;
            let title = tl
                .item_id
                .as_deref()
                .and_then(|id| view.state.playlist.get(id))
                .map(|i| i.title.clone());
            let position = self
                .node
                .clock
                .now_in_epoch(view.clock_epoch)
                .map_or(0.0, |now| tl.position_at(now).max(0) as f64 / 1e6);
            let duration = self
                .duration
                .as_ref()
                .and_then(|d| *d.borrow())
                .filter(|_| title.is_some());

            ui.horizontal(|ui| {
                let has_item = title.is_some();
                if ui
                    .add_enabled(has_item, egui::Button::new("⏮"))
                    .on_hover_text("Previous")
                    .clicked()
                {
                    self.node.command(Command::Prev);
                }
                let (label, command) = if tl.playing {
                    ("⏸ Pause", Command::Pause)
                } else {
                    ("▶ Play", Command::Play { item_id: None })
                };
                let can_play = has_item || !view.state.playlist.items.is_empty();
                if ui
                    .add_enabled(
                        can_play,
                        egui::Button::new(label).min_size(egui::vec2(80.0, 0.0)),
                    )
                    .clicked()
                {
                    self.node.command(command);
                }
                if ui
                    .add_enabled(has_item, egui::Button::new("⏭"))
                    .on_hover_text("Next")
                    .clicked()
                {
                    self.node.command(Command::Next);
                }
                if ui
                    .add_enabled(has_item, egui::Button::new("⏹"))
                    .on_hover_text("Stop")
                    .clicked()
                {
                    self.node.command(Command::Stop);
                }
                ui.label(clock_text(self.seek_to.unwrap_or(position)));
                match duration {
                    Some(d) if d > 0.0 => {
                        let mut value = self.seek_to.unwrap_or(position).min(d);
                        let width = (ui.available_width() - 70.0).max(80.0);
                        ui.spacing_mut().slider_width = width;
                        let slider =
                            ui.add(egui::Slider::new(&mut value, 0.0..=d).show_value(false));
                        if slider.dragged() {
                            self.seek_to = Some(value);
                        }
                        if slider.drag_stopped() || (slider.changed() && !slider.dragged()) {
                            self.seek_to = None;
                            self.node.command(Command::Seek {
                                pos_ms: (value * 1000.0) as i64,
                            });
                        }
                        ui.label(clock_text(d));
                    }
                    _ => {
                        ui.weak(title.as_deref().unwrap_or("Nothing playing"));
                    }
                }
            });
            if let Some(title) = &title {
                ui.weak(title);
            }
        }
        let status = match self.messages.back() {
            Some((_, text)) => text.clone(),
            None if view.session().is_none() => "Not in a playlist.".to_string(),
            None if view.is_leader() => "This PC is the host.".to_string(),
            None => match self.node.clock.best_sample() {
                Some(s) => format!(
                    "In sync with {} (network round trip {:.1} ms).",
                    view.leader_name(),
                    s.rtt_us as f64 / 1000.0
                ),
                None => format!("Syncing with {}…", view.leader_name()),
            },
        };
        ui.label(status);
        ui.add_space(4.0);
    }
}

fn clock_text(secs: f64) -> String {
    let s = secs.max(0.0) as u64;
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60)
    } else {
        format!("{}:{:02}", s / 60, s % 60)
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain(ctx);
        let view = self.view.borrow().clone();

        egui::TopBottomPanel::bottom("transport").show(ctx, |ui| self.transport(ui, &view));
        egui::SidePanel::left("sessions")
            .resizable(true)
            .default_width(260.0)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| self.sessions_panel(ui, &view));
            });
        egui::CentralPanel::default().show(ctx, |ui| self.playlist_panel(ui, &view));

        // Keep the position and peer list fresh.
        ctx.request_repaint_after(Duration::from_millis(250));
    }
}
