//! The shared playlist. The leader holds the authoritative copy; any node can
//! ask it to change.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
    /// Anything every node can open directly: http(s) URL, network share path.
    Url { url: String },
    /// A local file on the node `owner`, which serves it to the others.
    Shared { owner: String, file_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub title: String,
    pub source: Source,
    /// Name of the node that added the item.
    pub added_by: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Playlist {
    pub items: Vec<Item>,
}

impl Playlist {
    pub fn add(&mut self, item: Item) {
        if self.index_of(&item.id).is_none() {
            self.items.push(item);
        }
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.items.len();
        self.items.retain(|i| i.id != id);
        self.items.len() != before
    }

    /// Move an item to position `to`, clamped to the end of the list.
    pub fn move_to(&mut self, id: &str, to: usize) -> bool {
        let Some(from) = self.index_of(id) else {
            return false;
        };
        let item = self.items.remove(from);
        let to = to.min(self.items.len());
        self.items.insert(to, item);
        true
    }

    pub fn index_of(&self, id: &str) -> Option<usize> {
        self.items.iter().position(|i| i.id == id)
    }

    pub fn get(&self, id: &str) -> Option<&Item> {
        self.items.iter().find(|i| i.id == id)
    }

    pub fn first(&self) -> Option<&Item> {
        self.items.first()
    }

    pub fn after(&self, id: &str) -> Option<&Item> {
        self.index_of(id).and_then(|i| self.items.get(i + 1))
    }

    pub fn before(&self, id: &str) -> Option<&Item> {
        self.index_of(id)
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| self.items.get(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str) -> Item {
        Item {
            id: id.into(),
            title: id.into(),
            source: Source::Url {
                url: format!("http://x/{id}"),
            },
            added_by: "test".into(),
        }
    }

    #[test]
    fn add_ignores_duplicates() {
        let mut p = Playlist::default();
        p.add(item("a"));
        p.add(item("a"));
        assert_eq!(p.items.len(), 1);
    }

    #[test]
    fn move_and_neighbours() {
        let mut p = Playlist::default();
        for id in ["a", "b", "c"] {
            p.add(item(id));
        }
        assert!(p.move_to("c", 0));
        let ids: Vec<_> = p.items.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["c", "a", "b"]);
        assert_eq!(p.after("a").unwrap().id, "b");
        assert_eq!(p.before("a").unwrap().id, "c");
        assert!(p.before("c").is_none());
        assert!(p.after("b").is_none());
    }

    #[test]
    fn remove_reports_whether_found() {
        let mut p = Playlist::default();
        p.add(item("a"));
        assert!(p.remove("a"));
        assert!(!p.remove("a"));
    }
}
