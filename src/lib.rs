//! NodePlayer: keeps media playback in sync across PCs on a local network.
//!
//! Every node runs the same program. Nodes find each other with mDNS, agree
//! on one leader (the node that has been running longest), and follow the
//! leader's clock, playlist and playback timeline.

pub mod clock;
pub mod discovery;
pub mod media;
pub mod node;
pub mod player;
pub mod playlist;
pub mod protocol;
pub mod timeline;
