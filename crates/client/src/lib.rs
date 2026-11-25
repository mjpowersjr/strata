//! The entry point for an app reading published indexes (the strata format,
//! docs/format.md): one [`Library`] per base URL and cache folder, one
//! [`Index`] per published index name.
//!
//! - Online by default; when the network fails, the version last opened is
//!   used from the cache (everything read before, or everything when the
//!   index was kept offline).
//! - [`Library::keep_offline`] pins every object of the current version, so
//!   the whole index works without a network and survives cache eviction;
//!   [`Library::sync`] moves to a newer published version, fetching only new
//!   objects (and keeping the offline copy current).
//! - Reads are the index's own: full-text [`Index::search`] with filters and
//!   facets, rows by key ([`Index::get`]), key ranges ([`Index::range`]), and
//!   the item record as published ([`Index::record`]).
//!
//! Files the rows refer to stay where the index says (its `files_base`);
//! downloading them is the app's job.
//!
//! Every call blocks (the reader fetches with a blocking HTTP client); call
//! from a worker thread in an async app.

use anyhow::{Context, Result};
use strata_fetch::{ChunkCache, ChunkSource, HttpSource, RemoteError};
use strata::reader::V2Index;
pub use strata::reader::{Hit, Mode, SearchQuery, SearchResult};
use strata::reader::ValueRef;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub type Row = serde_json::Map<String, serde_json::Value>;

/// A key value for [`Index::get`] / [`Index::range`].
#[derive(Debug, Clone, PartialEq)]
pub enum Key {
    Int(i64),
    Text(String),
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Key::Text(s.into())
    }
}
impl From<String> for Key {
    fn from(s: String) -> Self {
        Key::Text(s)
    }
}
impl From<i64> for Key {
    fn from(i: i64) -> Self {
        Key::Int(i)
    }
}

fn value_refs(k: &[Key]) -> Vec<ValueRef<'_>> {
    k.iter()
        .map(|k| match k {
            Key::Int(i) => ValueRef::Integer(*i),
            Key::Text(t) => ValueRef::Text(t.as_bytes()),
        })
        .collect()
}

/// Where the open version came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// The published manifest was fetched just now.
    Online,
    /// The network failed; the version last opened, from the cache.
    Cached,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub index: String,
    pub seq: u64,
    pub built_at: String,
    pub version: String,
    pub origin: Origin,
    /// Every object of this version is pinned (usable with no network).
    pub offline: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncReport {
    pub updated: bool,
    pub from_seq: u64,
    pub to_seq: u64,
    /// Objects fetched to keep the offline copy complete (0 when not kept).
    pub fetched: usize,
    pub bytes_fetched: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Kept {
    indexes: BTreeSet<String>,
}

pub struct Library {
    root: PathBuf,
    source: Arc<dyn ChunkSource>,
    cache: Arc<ChunkCache>,
    kept: Mutex<Kept>,
}

/// One collection's index, at one version.
pub struct Index {
    ix: V2Index,
    origin: Origin,
    files_base: String,
}

impl Library {
    /// `base`: the public base of the published tree (`{base}/v3/…`);
    /// `root`: a folder of the app's for the cache and its state;
    /// `cache_bytes`: the cache's disk cap (pinned objects aren't evicted).
    pub fn open(base: &str, root: &Path, cache_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let source: Arc<dyn ChunkSource> = Arc::new(
            HttpSource::new(&format!("{}/{}/", base.trim_end_matches('/'), strata::version::TREE))?
                .with_object_prefix("objects/"),
        );
        let cache = ChunkCache::open(&root.join("cache"), cache_bytes, 64 << 20)?;
        let kept = std::fs::read(root.join("kept.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        Ok(Self {
            root: root.to_path_buf(),
            source,
            cache,
            kept: Mutex::new(kept),
        })
    }

    fn state_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    /// Open the current published version of `index`; if the network fails,
    /// the version last opened (from the cache).
    pub fn index(&self, index: &str) -> Result<Index> {
        match V2Index::open(
            self.source.clone(),
            self.cache.clone(),
            &self.state_dir(),
            index,
        ) {
            Ok(ix) => Ok(Index::new(ix, Origin::Online)),
            Err(e) if is_network(&e) => self.index_offline(index).with_context(|| format!("{e:#}")),
            Err(e) => Err(e),
        }
    }

    /// Open the version last opened, without the network.
    pub fn index_offline(&self, index: &str) -> Result<Index> {
        let ix = V2Index::open_cached(
            self.source.clone(),
            self.cache.clone(),
            &self.state_dir(),
            index,
        )?;
        Ok(Index::new(ix, Origin::Cached))
    }

    pub fn is_kept(&self, index: &str) -> bool {
        self.kept.lock().unwrap().indexes.contains(index)
    }

    fn save_kept(&self, k: &Kept) -> Result<()> {
        let p = self.root.join("kept.json");
        let tmp = p.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(k)?)?;
        std::fs::rename(tmp, p)?;
        Ok(())
    }

    /// Keep this index offline: fetch and pin every object of its version.
    /// Later [`sync`](Self::sync)s keep the copy complete.
    pub fn keep_offline(&self, ix: &Index) -> Result<SyncReport> {
        let r = ix.ix.pin()?;
        let mut k = self.kept.lock().unwrap();
        k.indexes.insert(ix.name().to_string());
        self.save_kept(&k)?;
        Ok(SyncReport {
            updated: false,
            from_seq: ix.ix.manifest.seq,
            to_seq: ix.ix.manifest.seq,
            fetched: r.fetched,
            bytes_fetched: r.bytes_fetched,
        })
    }

    /// Stop keeping it offline: its objects become ordinary cache entries.
    pub fn release_offline(&self, ix: &Index) -> Result<()> {
        ix.ix.unpin_except(None)?;
        let mut k = self.kept.lock().unwrap();
        k.indexes.remove(ix.name());
        self.save_kept(&k)
    }

    /// Move to the newest published version, if there is one. A kept index
    /// stays complete: only objects new in that version are fetched, and
    /// what only the old version used is released.
    pub fn sync(&self, ix: Index) -> Result<(Index, SyncReport)> {
        let from = ix.ix.manifest.seq;
        let Some(m) = ix.ix.check_update()? else {
            return Ok((
                ix,
                SyncReport {
                    updated: false,
                    from_seq: from,
                    to_seq: from,
                    fetched: 0,
                    bytes_fetched: 0,
                },
            ));
        };
        let newer = ix.ix.reopen(m, &self.state_dir())?;
        let (mut fetched, mut bytes) = (0, 0);
        if self.is_kept(ix.name()) {
            let r = ix.ix.sync_pin(&newer)?;
            fetched = r.fetched;
            bytes = r.bytes_fetched;
        }
        let to = newer.manifest.seq;
        Ok((
            Index::new(newer, Origin::Online),
            SyncReport {
                updated: true,
                from_seq: from,
                to_seq: to,
                fetched,
                bytes_fetched: bytes,
            },
        ))
    }

    pub fn status(&self, ix: &Index) -> Status {
        Status {
            index: ix.name().to_string(),
            seq: ix.ix.manifest.seq,
            built_at: ix.ix.manifest.built_at.clone(),
            version: ix.ix.manifest.version.sha256.clone(),
            origin: ix.origin,
            offline: self.is_kept(ix.name()),
        }
    }
}

/// A failure to reach the server (no answer, or a 5xx / unexpected status),
/// as opposed to a bad or unsupported index.
fn is_network(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(
            c.downcast_ref::<RemoteError>(),
            Some(RemoteError::Offline { .. }) | Some(RemoteError::Fetch { .. })
        )
    })
}

impl Index {
    fn new(ix: V2Index, origin: Origin) -> Self {
        // The version says where its audio lives; no collection knowledge here.
        let files_base = ix.version.files_base.clone();
        Self {
            ix,
            origin,
            files_base,
        }
    }

    pub fn name(&self) -> &str {
        &self.ix.manifest.index
    }

    pub fn seq(&self) -> u64 {
        self.ix.manifest.seq
    }

    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// The tables and index maps this version publishes, with their key
    /// and value columns.
    pub fn tables(&self) -> Vec<(String, Vec<String>, Vec<String>)> {
        self.ix
            .version
            .store
            .maps
            .iter()
            .map(|(n, m)| (n.clone(), m.key.clone(), m.columns.clone()))
            .collect()
    }

    pub fn search(&self, q: &SearchQuery) -> Result<SearchResult> {
        self.ix.search(q)
    }

    /// A row by its primary key.
    pub fn get(&self, table: &str, key: &[Key]) -> Result<Option<Row>> {
        self.ix.get(table, &value_refs(key))
    }

    /// Rows whose key starts with `prefix` (a table, or an index map: then
    /// the rows it points to), in key order; with `partial_text` the last
    /// value is a text prefix.
    pub fn range(
        &self,
        map: &str,
        prefix: &[Key],
        partial_text: bool,
        limit: Option<u64>,
    ) -> Result<Vec<Row>> {
        self.ix.range(map, &value_refs(prefix), partial_text, limit)
    }

    /// An item's record as published, by the convention of an `items` table
    /// with a `record` column (JSON), deserialized as `R`.
    pub fn record<R: DeserializeOwned>(&self, id: &str) -> Result<Option<R>> {
        let Some(row) = self.get("items", &[id.into()])? else {
            return Ok(None);
        };
        match row.get("record") {
            Some(serde_json::Value::String(s)) => Ok(Some(serde_json::from_str(s)?)),
            Some(v) if !v.is_null() => Ok(Some(serde_json::from_value(v.clone())?)),
            _ => Ok(None),
        }
    }

    /// Where a file of an item plays from (the index's `files_base`).
    pub fn file_url(&self, item: &str, file: &str) -> String {
        format!(
            "{}{}/{}",
            self.files_base,
            encode(item),
            file.split('/').map(encode).collect::<Vec<_>>().join("/")
        )
    }
}

fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// One hit of a search across indexes: which index it came from, its rank
/// there, and the fused score it was ordered by.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FederatedHit {
    pub index: String,
    pub rank: usize,
    pub score: f64,
    pub hit: Hit,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Federated {
    /// Matches per index (each index's own total).
    pub totals: Vec<(String, usize)>,
    pub hits: Vec<FederatedHit>,
}

/// Reciprocal rank fusion's constant: a hit at rank `r` (from 0) scores
/// `1 / (K + r + 1)`. Ranks, not scores, are combined: BM25 scores from
/// different indexes aren't comparable.
const RRF_K: f64 = 60.0;

/// Search several indexes with one query (in parallel) and merge the hits by
/// reciprocal rank fusion: the first hits of every index come first,
/// interleaved, whatever their raw scores. `q.limit` applies per index and
/// to the merged list.
pub fn search_all(indexes: &[&Index], q: &SearchQuery) -> Result<Federated> {
    let results: Vec<Result<SearchResult>> = std::thread::scope(|s| {
        let hs: Vec<_> = indexes.iter().map(|ix| s.spawn(move || ix.search(q))).collect();
        hs.into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err(anyhow::anyhow!("search thread panicked"))))
            .collect()
    });
    let mut totals = vec![];
    let mut hits = vec![];
    for (ix, r) in indexes.iter().zip(results) {
        let r = r?;
        totals.push((ix.name().to_string(), r.total));
        for (rank, hit) in r.hits.into_iter().enumerate() {
            hits.push(FederatedHit {
                index: ix.name().to_string(),
                rank,
                score: 1.0 / (RRF_K + rank as f64 + 1.0),
                hit,
            });
        }
    }
    // Same fused score (same rank): the index with more matches first.
    let total_of = |n: &str| totals.iter().find(|(x, _)| x == n).map_or(0, |t| t.1);
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(total_of(&b.index).cmp(&total_of(&a.index)))
            .then(a.index.cmp(&b.index))
    });
    hits.truncate(q.limit.max(1));
    Ok(Federated { totals, hits })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_urls_are_encoded_per_segment() {
        assert_eq!(encode("a b/c"), "a%20b%2Fc");
        let base = "https://files.example.org/download/";
        let u = format!(
            "{}{}/{}",
            base,
            encode("it em"),
            "d1/t 1.mp3"
                .split('/')
                .map(encode)
                .collect::<Vec<_>>()
                .join("/")
        );
        assert_eq!(u, "https://files.example.org/download/it%20em/d1/t%201.mp3");
    }
}
