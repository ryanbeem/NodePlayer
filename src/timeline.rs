//! The playback timeline every node follows.
//!
//! A timeline is an anchor: media position `anchor_pos_us` plays at shared
//! clock time `anchor_clock_us`, moving at `rate`. Pause, resume and seek all
//! just publish a new anchor.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Timeline {
    /// Playlist item being played, if any.
    pub item_id: Option<String>,
    pub playing: bool,
    pub anchor_pos_us: i64,
    pub anchor_clock_us: i64,
    pub rate: f64,
}

impl Default for Timeline {
    fn default() -> Self {
        Self::stopped()
    }
}

impl Timeline {
    pub fn stopped() -> Self {
        Self {
            item_id: None,
            playing: false,
            anchor_pos_us: 0,
            anchor_clock_us: 0,
            rate: 1.0,
        }
    }

    /// Media position at shared time `clock_us`. While playing this is
    /// negative before the scheduled start, which means "not yet".
    pub fn position_at(&self, clock_us: i64) -> i64 {
        if !self.playing {
            return self.anchor_pos_us;
        }
        self.anchor_pos_us + ((clock_us - self.anchor_clock_us) as f64 * self.rate) as i64
    }

    /// Play `item_id` from `pos_us`, starting at shared time `start_us`.
    pub fn start(&mut self, item_id: String, pos_us: i64, start_us: i64) {
        self.item_id = Some(item_id);
        self.playing = true;
        self.anchor_pos_us = pos_us;
        self.anchor_clock_us = start_us;
    }

    pub fn stop(&mut self) {
        *self = Self {
            rate: self.rate,
            ..Self::stopped()
        };
    }

    pub fn pause(&mut self, now_us: i64) {
        if !self.playing {
            return;
        }
        self.anchor_pos_us = self.position_at(now_us).max(0);
        self.anchor_clock_us = now_us;
        self.playing = false;
    }

    /// Resume at shared time `start_us` (a little in the future, so every
    /// node can get ready).
    pub fn resume(&mut self, start_us: i64) {
        if self.playing || self.item_id.is_none() {
            return;
        }
        self.anchor_clock_us = start_us;
        self.playing = true;
    }

    pub fn seek(&mut self, pos_us: i64, now_us: i64, start_us: i64) {
        self.anchor_pos_us = pos_us.max(0);
        self.anchor_clock_us = if self.playing { start_us } else { now_us };
    }

    /// Move the anchor into another clock domain, given `delta_us` =
    /// new clock minus old clock. Used when leadership changes hands.
    pub fn rebase(&mut self, delta_us: i64) {
        self.anchor_clock_us += delta_us;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_runs_from_scheduled_start() {
        let mut t = Timeline::stopped();
        t.start("a".into(), 0, 1_000_000);
        assert_eq!(t.position_at(500_000), -500_000);
        assert_eq!(t.position_at(1_000_000), 0);
        assert_eq!(t.position_at(3_500_000), 2_500_000);
    }

    #[test]
    fn pause_freezes_and_resume_continues() {
        let mut t = Timeline::stopped();
        t.start("a".into(), 0, 0);
        t.pause(4_000_000);
        assert_eq!(t.position_at(9_000_000), 4_000_000);
        t.resume(10_000_000);
        assert_eq!(t.position_at(11_000_000), 5_000_000);
    }

    #[test]
    fn seek_while_playing_schedules_new_start() {
        let mut t = Timeline::stopped();
        t.start("a".into(), 0, 0);
        t.seek(60_000_000, 5_000_000, 5_500_000);
        assert_eq!(t.position_at(5_500_000), 60_000_000);
        assert_eq!(t.position_at(6_500_000), 61_000_000);
    }

    #[test]
    fn seek_while_paused_moves_frozen_position() {
        let mut t = Timeline::stopped();
        t.start("a".into(), 0, 0);
        t.pause(1_000_000);
        t.seek(30_000_000, 2_000_000, 2_500_000);
        assert!(!t.playing);
        assert_eq!(t.position_at(99_000_000), 30_000_000);
    }

    #[test]
    fn rebase_keeps_position_in_new_clock() {
        let mut t = Timeline::stopped();
        t.start("a".into(), 0, 1_000_000);
        let before = t.position_at(5_000_000);
        // The new clock reads 2s more than the old one at the same instant.
        t.rebase(2_000_000);
        assert_eq!(t.position_at(7_000_000), before);
    }

    #[test]
    fn resume_without_item_does_nothing() {
        let mut t = Timeline::stopped();
        t.resume(10);
        assert!(!t.playing);
    }
}
