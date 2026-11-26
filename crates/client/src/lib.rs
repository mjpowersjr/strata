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
use strata_fetch::ChunkCache;
pub use strata_fetch::{ChunkSource, FallbackSource, HttpSource, RemoteError};
use strata::reader::V2Index;
pub use strata::reader::{Hit, Mode, SearchQuery, SearchResult};
use strata::reader::ValueRef;
use strata::version::Manifest;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

mod control;
mod errors;
pub use control::{Cancelled, Control, Plan, Progress};
pub use errors::{error_kind, ErrorKind};

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
    /// Records (items) in this version.
    pub records: u64,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Kept {
    indexes: BTreeSet<String>,
    /// The version kept complete, per index (what offline opens and what is
    /// pinned). Missing for copies kept before this was recorded.
    #[serde(default)]
    versions: BTreeMap<String, Manifest>,
}

pub struct Library {
    root: PathBuf,
    source: Arc<dyn ChunkSource>,
    cache: Arc<ChunkCache>,
    kept: Mutex<Kept>,
    /// Index → the version this process has pinned, and its objects.
    pinned: Mutex<HashMap<String, (String, Vec<String>)>>,
}

/// One collection's index, at one version.
pub struct Index {
    ix: V2Index,
    origin: Origin,
    files_base: String,
    members: OnceLock<HashMap<String, u64>>,
}

/// Where a page of [`Index::range_page`] stopped: pass it back for the
/// next page. A key position, so it stays meaningful across versions (the
/// next page of a newer version starts at the same key). `Display` /
/// `FromStr` (hex) for keeping it in app state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cursor(Vec<u8>);

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for Cursor {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        anyhow::ensure!(s.len().is_multiple_of(2), "cursor: odd length");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).context("cursor: not hex"))
            .collect::<Result<Vec<u8>>>()
            .map(Cursor)
    }
}

/// One page of rows and where the next starts (`None`: this was the last).
#[derive(Debug, Clone)]
pub struct Page {
    pub rows: Vec<Row>,
    pub next: Option<Cursor>,
}

/// Every row of a range, fetched a page at a time
/// ([`Index::range_iter`]).
pub struct RangeIter<'a> {
    ix: &'a Index,
    map: String,
    prefix: Vec<Key>,
    partial_text: bool,
    page: u64,
    buf: std::vec::IntoIter<Row>,
    next: Option<Option<Cursor>>,
}

impl Iterator for RangeIter<'_> {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Result<Row>> {
        loop {
            if let Some(r) = self.buf.next() {
                return Some(Ok(r));
            }
            // None: not started; Some(None): finished.
            let after = match &self.next {
                Some(None) => return None,
                Some(Some(c)) => Some(c.clone()),
                None => None,
            };
            match self.ix.range_page(
                &self.map,
                &self.prefix,
                self.partial_text,
                after.as_ref(),
                self.page,
            ) {
                Ok(p) => {
                    self.buf = p.rows.into_iter();
                    self.next = Some(p.next);
                }
                Err(e) => {
                    self.next = Some(None);
                    return Some(Err(e));
                }
            }
        }
    }
}

impl Library {
    /// `base`: the public base of the published tree (`{base}/v3/…`);
    /// `root`: a folder of the app's for the cache and its state;
    /// `cache_bytes`: the cache's disk cap (pinned objects aren't evicted).
    pub fn open(base: &str, root: &Path, cache_bytes: u64) -> Result<Self> {
        Self::open_with(root, cache_bytes, Self::http_source(base)?)
    }

    /// The default source for a public base: `{base}/v3/`, objects under
    /// `objects/`. Chain several with [`FallbackSource`] (a computer on the
    /// home network first, the public tree second) for
    /// [`open_with`](Self::open_with).
    pub fn http_source(base: &str) -> Result<Arc<dyn ChunkSource>> {
        Ok(Arc::new(
            HttpSource::new(&format!("{}/{}/", base.trim_end_matches('/'), strata::version::TREE))?
                .with_object_prefix("objects/"),
        ))
    }

    /// Like [`open`](Self::open), reading through any source. Objects are
    /// verified by name whatever serves them.
    ///
    /// Kept indexes are pinned here (reading their internal tree nodes from
    /// the cache), so they survive eviction from the first fetch on.
    pub fn open_with(root: &Path, cache_bytes: u64, source: Arc<dyn ChunkSource>) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let cache = ChunkCache::open(&root.join("cache"), cache_bytes, 64 << 20)?;
        let lib = Self {
            root: root.to_path_buf(),
            source,
            cache,
            kept: Mutex::new(Kept::default()),
            pinned: Mutex::new(HashMap::new()),
        };
        let kept = lib.load_kept();
        for (name, m) in &kept.versions {
            // Best effort: an incomplete copy pins what it can next keep/sync.
            let _ = lib.pin_version(name, m);
        }
        Ok(lib)
    }

    fn load_kept(&self) -> Kept {
        let k: Kept = std::fs::read(self.root.join("kept.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        *self.kept.lock().unwrap() = k.clone();
        k
    }

    /// Pin every object of a version (refcounted in the cache), once per
    /// index and version in this process.
    fn pin_version(&self, name: &str, m: &Manifest) -> Result<()> {
        if self.pinned_version(name).as_deref() == Some(m.version.sha256.as_str()) {
            return Ok(());
        }
        let (_, objs) = V2Index::objects_of(self.source.clone(), self.cache.clone(), m)?;
        self.pin_objs(&objs);
        self.adopt_pins(name, &m.version.sha256, objs);
        Ok(())
    }

    fn pinned_version(&self, name: &str) -> Option<String> {
        self.pinned.lock().unwrap().get(name).map(|p| p.0.clone())
    }

    fn pin_objs(&self, objs: &[(String, u64)]) {
        for (sha, _) in objs {
            self.cache.pin(sha);
        }
    }

    fn unpin_objs(&self, objs: &[(String, u64)]) {
        for (sha, _) in objs {
            self.cache.unpin(sha);
        }
    }

    /// `objs` (version `sha` of `name`) were just pinned: record them as
    /// the index's pinned version, releasing a previous one.
    fn adopt_pins(&self, name: &str, sha: &str, objs: Vec<(String, u64)>) {
        if self.pinned_version(name).as_deref() == Some(sha) {
            self.unpin_objs(&objs); // pinned twice
            return;
        }
        let keep: HashSet<String> = objs.iter().map(|o| o.0.clone()).collect();
        self.unpin_index_except(name, &keep);
        self.pinned.lock().unwrap().insert(
            name.to_string(),
            (sha.to_string(), objs.into_iter().map(|o| o.0).collect()),
        );
    }

    /// Unpin the version this process pinned for `name`, except `keep`.
    fn unpin_index_except(&self, name: &str, keep: &HashSet<String>) {
        let old = self.pinned.lock().unwrap().remove(name);
        if let Some((_, objs)) = old {
            for o in objs.iter().filter(|o| !keep.contains(*o)) {
                self.cache.unpin(o);
            }
        }
    }

    /// Pin `objs`, fetch what's missing, and on failure or cancel unpin
    /// them again. Pinning first keeps a small cache from evicting the
    /// start of a download before its end arrives.
    fn fetch_pinned(&self, objs: &[(String, u64)], node_size: u64, ctl: &Control) -> Result<(usize, u64)> {
        self.pin_objs(objs);
        let r = self.fetch_all(objs, node_size, ctl);
        if r.is_err() {
            self.unpin_objs(objs);
        }
        r
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
            // Offline fallback; if there's nothing to fall back to, the
            // network error itself (typed: see `error_kind`).
            Err(e) if is_network(&e) => self.index_offline(index).map_err(|_| e),
            Err(e) => Err(e),
        }
    }

    /// Open the version last opened, without the network.
    /// For a kept index, the version kept complete (even when a newer one
    /// was opened online since).
    pub fn index_offline(&self, index: &str) -> Result<Index> {
        let kept = self.load_kept().versions.get(index).cloned();
        let ix = match kept {
            Some(m) => V2Index::open_manifest(self.source.clone(), self.cache.clone(), m)?,
            None => V2Index::open_cached(
                self.source.clone(),
                self.cache.clone(),
                &self.state_dir(),
                index,
            )?,
        };
        Ok(Index::new(ix, Origin::Cached))
    }

    /// Whether the index is kept offline (read from disk, so another
    /// `Library` on the same folder keeping it counts).
    pub fn is_kept(&self, index: &str) -> bool {
        self.load_kept().indexes.contains(index)
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
        self.keep_offline_with(ix, &Control::default())
    }

    /// [`keep_offline`](Self::keep_offline) with progress and cancel. On
    /// cancel ([`Cancelled`]) nothing is marked kept; what was fetched stays
    /// in the cache (unpinned), so a later keep resumes where it stopped.
    pub fn keep_offline_with(&self, ix: &Index, ctl: &Control) -> Result<SyncReport> {
        let objs = ix.ix.objects()?;
        let node_size = node_estimate(&ix.ix.version);
        let (fetched, bytes) = self.fetch_pinned(&objs, node_size, ctl)?;
        let name = ix.name().to_string();
        self.adopt_pins(&name, &ix.ix.manifest.version.sha256, objs);
        {
            let mut k = self.load_kept();
            k.indexes.insert(name.clone());
            k.versions.insert(name.clone(), ix.ix.manifest.clone());
            self.save_kept(&k)?;
        }
        ix.ix.save_manifest(&self.state_dir())?;
        Ok(SyncReport {
            updated: false,
            from_seq: ix.ix.manifest.seq,
            to_seq: ix.ix.manifest.seq,
            fetched,
            bytes_fetched: bytes,
        })
    }

    /// Fetch what isn't cached of `objs` (16 at a time), reporting progress
    /// and checking `ctl` before each object. Returns objects fetched and
    /// bytes downloaded.
    fn fetch_all(&self, objs: &[(String, u64)], node_size: u64, ctl: &Control) -> Result<(usize, u64)> {
        let missing: Vec<&(String, u64)> = objs.iter().filter(|(s, _)| !self.cache.on_disk(s)).collect();
        let est = |size: u64| stored_bound(size, node_size);
        let total = Progress {
            objects_done: 0,
            objects_total: missing.len(),
            bytes_done: 0,
            bytes_total: missing.iter().map(|o| est(o.1)).sum(),
            downloaded: 0,
        };
        ctl.report(&total);
        let next = AtomicUsize::new(0);
        let state = Mutex::new(total);
        let failed: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        let stop = AtomicBool::new(false);
        std::thread::scope(|sc| {
            for _ in 0..16.min(missing.len()) {
                sc.spawn(|| loop {
                    if stop.load(Ordering::SeqCst) || ctl.is_cancelled() {
                        return;
                    }
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    let Some((sha, size)) = missing.get(i) else {
                        return;
                    };
                    match self.cache.fetch_to_disk(sha, *size, &*self.source) {
                        Ok(got) => {
                            let mut p = state.lock().unwrap();
                            p.objects_done += 1;
                            p.bytes_done += est(*size);
                            p.downloaded += got;
                            ctl.report(&p);
                        }
                        Err(e) => {
                            stop.store(true, Ordering::SeqCst);
                            failed.lock().unwrap().get_or_insert(e.into());
                            return;
                        }
                    }
                });
            }
        });
        if let Some(e) = failed.into_inner().unwrap() {
            return Err(e);
        }
        let p = state.into_inner().unwrap();
        if p.objects_done < p.objects_total {
            return Err(Cancelled.into());
        }
        Ok((p.objects_done, p.downloaded))
    }

    /// What [`keep_offline`](Self::keep_offline) would fetch (nothing is
    /// downloaded but the internal tree nodes, usually already cached).
    pub fn plan_keep(&self, ix: &Index) -> Result<Plan> {
        let objs = ix.ix.objects()?;
        Ok(self.plan_of(ix.seq(), &objs, &ix.ix.version))
    }

    /// What [`sync`](Self::sync) would move to and, for a kept index,
    /// fetch; `None` when the open version is the newest. Fetches the newer
    /// manifest, version object, segment objects and internal tree nodes
    /// (small, and needed by the sync anyway).
    pub fn plan_sync(&self, ix: &Index) -> Result<Option<Plan>> {
        let Some(m) = ix.ix.check_update()? else {
            return Ok(None);
        };
        let (v, objs) = V2Index::objects_of(self.source.clone(), self.cache.clone(), &m)?;
        let mut p = self.plan_of(m.seq, &objs, &v);
        if !self.is_kept(ix.name()) {
            // Not kept: opening fetches the segments' hot packs; the rest is
            // read on demand.
            let hot: Vec<(String, u64)> = v
                .search
                .segments
                .iter()
                .map(|s| (s.hot.sha256.clone(), s.hot.size))
                .collect();
            let h = self.plan_of(m.seq, &hot, &v);
            p.missing = h.missing;
            p.missing_bytes = h.missing_bytes;
        }
        Ok(Some(p))
    }

    fn plan_of(&self, seq: u64, objs: &[(String, u64)], v: &strata::version::Version) -> Plan {
        let node_size = node_estimate(v);
        let recorded = node_bytes(v);
        let mut p = Plan {
            seq,
            objects: objs.len(),
            bytes: 0,
            missing: 0,
            missing_bytes: 0,
            estimated: false,
        };
        let (mut nodes, mut nodes_missing, mut nodes_present_bytes) = (0u64, 0u64, 0u64);
        for (sha, size) in objs {
            let have = self.cache.disk_size(sha);
            if *size == 0 {
                nodes += 1;
                match have {
                    Some(b) => nodes_present_bytes += b,
                    None => nodes_missing += 1,
                }
            } else {
                p.bytes += size + 1;
                if have.is_none() {
                    p.missing += 1;
                    p.missing_bytes += size + 1;
                }
            }
        }
        p.missing += nodes_missing as usize;
        match recorded {
            // Every map records its bytes: what's missing is the total less
            // what's here (exact, nodes shared between maps aside).
            Some(total) => {
                p.bytes += total;
                if nodes_missing > 0 {
                    p.missing_bytes += total.saturating_sub(nodes_present_bytes).max(nodes_missing);
                }
            }
            None => {
                p.bytes += nodes * node_size;
                p.missing_bytes += nodes_missing * node_size;
                p.estimated = nodes > 0;
            }
        }
        p
    }

    /// Stop keeping it offline: its objects become ordinary cache entries.
    pub fn release_offline(&self, ix: &Index) -> Result<()> {
        self.unpin_index_except(ix.name(), &HashSet::new());
        let mut k = self.load_kept();
        k.indexes.remove(ix.name());
        k.versions.remove(ix.name());
        self.save_kept(&k)
    }

    /// Move to the newest published version, if there is one. A kept index
    /// stays complete: only objects new in that version are fetched, and
    /// what only the old version used is released.
    pub fn sync(&self, ix: Index) -> Result<(Index, SyncReport)> {
        match self.sync_with(&ix, &Control::default())? {
            Some(r) => Ok(r),
            None => {
                let s = ix.seq();
                Ok((
                    ix,
                    SyncReport {
                        updated: false,
                        from_seq: s,
                        to_seq: s,
                        fetched: 0,
                        bytes_fetched: 0,
                    },
                ))
            }
        }
    }

    /// [`sync`](Self::sync) with progress and cancel, leaving `ix` usable:
    /// `None` when it is the newest. On cancel ([`Cancelled`]) the kept copy
    /// stays the old version (complete) and offline opens still get it;
    /// what was fetched stays cached for the next try.
    pub fn sync_with(&self, ix: &Index, ctl: &Control) -> Result<Option<(Index, SyncReport)>> {
        let from = ix.ix.manifest.seq;
        let Some(m) = ix.ix.check_update()? else {
            return Ok(None);
        };
        let newer = V2Index::open_manifest(self.source.clone(), self.cache.clone(), m)?;
        let (mut fetched, mut bytes) = (0, 0);
        if self.is_kept(ix.name()) {
            let objs = newer.objects()?;
            (fetched, bytes) =
                self.fetch_pinned(&objs, node_estimate(&newer.version), ctl)?;
            // The new version is complete and pinned: record it, release the old.
            let name = ix.name().to_string();
            self.adopt_pins(&name, &newer.manifest.version.sha256, objs);
            let mut k = self.load_kept();
            k.versions.insert(name, newer.manifest.clone());
            self.save_kept(&k)?;
        }
        newer.save_manifest(&self.state_dir())?;
        let to = newer.manifest.seq;
        Ok(Some((
            Index::new(newer, Origin::Online),
            SyncReport {
                updated: true,
                from_seq: from,
                to_seq: to,
                fetched,
                bytes_fetched: bytes,
            },
        )))
    }

    /// An object's stored bytes (as published) from this library's cache,
    /// if `ix`'s version references it and it is here: for serving a kept
    /// index to other devices. Nothing is fetched.
    pub fn stored_object(&self, ix: &Index, sha256: &str) -> Result<Option<Vec<u8>>> {
        let Some(size) = ix.member(sha256)? else {
            return Ok(None);
        };
        Ok(self.cache.stored(sha256, size))
    }

    pub fn status(&self, ix: &Index) -> Status {
        Status {
            index: ix.name().to_string(),
            seq: ix.ix.manifest.seq,
            built_at: ix.ix.manifest.built_at.clone(),
            version: ix.ix.manifest.version.sha256.clone(),
            origin: ix.origin,
            offline: self.is_kept(ix.name()),
            records: ix.records(),
        }
    }
}

/// The stored bytes of all tree nodes, when every map records them (3.2).
fn node_bytes(v: &strata::version::Version) -> Option<u64> {
    let maps = &v.store.maps;
    (!maps.is_empty() && maps.values().all(|m| m.bytes > 0)).then(|| maps.values().map(|m| m.bytes).sum())
}

/// A tree node's expected stored size: the recorded average, or (before
/// 3.2) the target node size, an upper bound.
fn node_estimate(v: &strata::version::Version) -> u64 {
    let nodes: u64 = v.store.maps.values().map(|m| m.levels.iter().sum::<u64>()).sum();
    match node_bytes(v) {
        Some(b) if nodes > 0 => b.div_ceil(nodes),
        _ => v.store.node_target as u64 + 1,
    }
}

/// An upper bound on an object's stored size: its decoded size plus the
/// encoding byte; a tree node (size unrecorded) at the target node size.
fn stored_bound(size: u64, node_size: u64) -> u64 {
    if size == 0 {
        node_size
    } else {
        size + 1
    }
}

/// A failure to reach the server (no answer, or a 5xx / unexpected status),
/// as opposed to a bad or unsupported index.
fn is_network(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(
            c.downcast_ref::<RemoteError>(),
            Some(RemoteError::Offline { .. })
                | Some(RemoteError::Fetch { .. })
                | Some(RemoteError::Status { .. })
                | Some(RemoteError::Truncated { .. })
                | Some(RemoteError::Timeout { .. })
                | Some(RemoteError::Protocol { .. })
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
            members: OnceLock::new(),
        }
    }

    /// Where the rows' files live (a URL prefix; see [`file_url`](Self::file_url)).
    pub fn files_base(&self) -> &str {
        &self.files_base
    }

    /// Records (items) in this version.
    pub fn records(&self) -> u64 {
        self.ix.version.records
    }

    /// Entries in a table or index map (rows of a table).
    pub fn count(&self, map: &str) -> Option<u64> {
        self.ix.version.store.maps.get(map).map(|m| m.count)
    }

    /// Entries per table and index map.
    pub fn counts(&self) -> Vec<(String, u64)> {
        self.ix
            .version
            .store
            .maps
            .iter()
            .map(|(n, m)| (n.clone(), m.count))
            .collect()
    }

    /// The published manifest of this version (what a server passing the
    /// index on serves as `{index}/manifest.json`).
    pub fn manifest_json(&self) -> Vec<u8> {
        serde_json::to_vec(&self.ix.manifest).expect("manifest serializes")
    }

    /// Every object of this version with its decoded size (0 for tree
    /// nodes, whose size isn't recorded).
    pub fn objects(&self) -> Result<Vec<(String, u64)>> {
        self.ix.objects()
    }

    /// Whether this version references object `sha256` (its size if so);
    /// for a server answering a device only for the index it names. The
    /// list is built on first use.
    pub fn member(&self, sha256: &str) -> Result<Option<u64>> {
        if self.members.get().is_none() {
            let m: HashMap<String, u64> = self.ix.objects()?.into_iter().collect();
            let _ = self.members.set(m);
        }
        Ok(self.members.get().unwrap().get(sha256).copied())
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

    /// One page of [`range`](Self::range): at most `limit` rows from `after`
    /// (the previous page's `next`; `None` from the start).
    pub fn range_page(
        &self,
        map: &str,
        prefix: &[Key],
        partial_text: bool,
        after: Option<&Cursor>,
        limit: u64,
    ) -> Result<Page> {
        let (rows, next) = self.ix.range_from(
            map,
            &value_refs(prefix),
            partial_text,
            after.map(|c| c.0.as_slice()),
            Some(limit.max(1)),
        )?;
        Ok(Page {
            rows,
            next: next.map(Cursor),
        })
    }

    /// Every row of a range, `page` rows per fetch.
    pub fn range_iter(&self, map: &str, prefix: &[Key], partial_text: bool, page: u64) -> RangeIter<'_> {
        RangeIter {
            ix: self,
            map: map.to_string(),
            prefix: prefix.to_vec(),
            partial_text,
            page: page.max(1),
            buf: Vec::new().into_iter(),
            next: None,
        }
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
