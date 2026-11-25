//! The manifest, the version object and store references
//! (docs/format.md sections 2, 3 and 5).
use crate::search::SearchRef;
pub use crate::ObjRef;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 3.1: the version's `audio_base` is `files_base` (3.0 versions still read).
/// 3.0: objects are named by the sha256 of their decoded bytes and stored
/// with an encoding byte (`strata_object`); the tree moved to `v3/`.
pub const SPEC_VERSION: &str = "3.1.0";
/// `Manifest::format`.
pub const FORMAT: &str = "v3";
/// The published tree's prefix under the public base: `{TREE}/objects/{sha}`,
/// `{TREE}/{index}/manifest.json`, and the index's side files.
pub const TREE: &str = "v3";

/// Whether a reader of this software reads a manifest of this format.
pub fn readable(m: &Manifest) -> bool {
    m.format == FORMAT && m.spec_version.split('.').next() == SPEC_VERSION.split('.').next()
}

/// `v3/{index}/manifest.json`: the only mutable file; written last.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub format: String,
    pub spec_version: String,
    pub index: String,
    pub seq: u64,
    pub built_at: String,
    pub version: ObjRef,
}

/// One complete version, stored as an object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Version {
    pub index: String,
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<ObjRef>,
    pub profile: String,
    /// Base URL the rows' files are relative to (3.0 called it `audio_base`).
    #[serde(alias = "audio_base")]
    pub files_base: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attribution: Vec<String>,
    pub search: SearchRef,
    pub store: StoreRef,
    pub records: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MapRef {
    pub root: String,
    pub depth: u8,
    pub count: u64,
    /// Nodes per level, leaves first (for in-place edits; `prolly::Root::levels`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<u64>,
    /// Key columns in order ("col" or "col desc").
    pub key: Vec<String>,
    /// For a table map: its value columns, in order. Empty for an index map.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// For an index map: the table it indexes (its keys end with that
    /// table's primary key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub of: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreRef {
    pub node_target: usize,
    pub maps: BTreeMap<String, MapRef>,
}

#[cfg(test)]
mod files_base_tests {
    use super::*;

    #[test]
    fn a_3_0_version_with_audio_base_still_reads() {
        let mut v = serde_json::json!({
            "index": "x", "seq": 1, "profile": "p",
            "files_base": "https://f/",
            "search": {"engine": "e", "fields": [], "chunk_size": 1, "files": {}, "segments": []},
            "store": {"node_target": 16384, "maps": {}},
            "records": 0
        });
        let new: Version = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(new.files_base, "https://f/");
        let o = v.as_object_mut().unwrap();
        let b = o.remove("files_base").unwrap();
        o.insert("audio_base".into(), b);
        let old: Version = serde_json::from_value(v).unwrap();
        assert_eq!(old.files_base, "https://f/");
        assert!(serde_json::to_string(&old).unwrap().contains("\"files_base\""));
    }
}
