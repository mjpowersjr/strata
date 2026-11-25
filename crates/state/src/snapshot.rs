//! Snapshots of a working directory into a [`Store`] and back.
//!
//! Everything goes into content-addressed blobs (`objects/{sha256}`, zstd
//! compressed; the name is the hash of the uncompressed bytes), so a blob is
//! written once and never changes. A collection's `HEAD` lists, per
//! component, which blobs make it up:
//!
//! - [`Kind::File`]: one file in 1 MiB chunks (a SQLite catalog changed in a
//!   few pages rewrites a few chunks);
//! - [`Kind::Tree`]: every file under a directory, each chunked (the
//!   publisher's search index: its segment files never change once written);
//! - [`Kind::Pack`]: a directory as one pack (caches of many small files);
//! - [`Kind::Shards`]: each subdirectory as its own pack (the raw response
//!   cache, `{shard}/{id}.json.zst`): a run restores and rewrites only the
//!   shards it touches.

use crate::store::{sha256_hex, Cond, Put, PutOpts, Store};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub const CHUNK: usize = 1 << 20;
pub const HEAD_VERSION: u32 = 1;
const PACK_MAGIC: &[u8; 8] = b"G78PACK1";
const PARALLEL: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Tree,
    Pack,
    Shards,
}

/// One part of a collection's working directory.
#[derive(Debug, Clone)]
pub struct Component {
    pub name: &'static str,
    /// Relative to the working directory.
    pub path: PathBuf,
    pub kind: Kind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSnap {
    pub size: u64,
    pub chunks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackRef {
    pub sha: String,
    pub size: u64,
    pub files: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Snap {
    File(FileSnap),
    Tree { files: BTreeMap<String, FileSnap> },
    Pack(PackRef),
    Shards { shards: BTreeMap<String, PackRef> },
}

impl Snap {
    fn blobs(&self, out: &mut HashSet<String>) {
        match self {
            Snap::File(f) => out.extend(f.chunks.iter().cloned()),
            Snap::Tree { files } => {
                for f in files.values() {
                    out.extend(f.chunks.iter().cloned());
                }
            }
            Snap::Pack(p) => {
                out.insert(p.sha.clone());
            }
            Snap::Shards { shards } => out.extend(shards.values().map(|p| p.sha.clone())),
        }
    }

    pub fn bytes(&self) -> u64 {
        match self {
            Snap::File(f) => f.size,
            Snap::Tree { files } => files.values().map(|f| f.size).sum(),
            Snap::Pack(p) => p.size,
            Snap::Shards { shards } => shards.values().map(|p| p.size).sum(),
        }
    }
}

/// What a run published: the version object and the manifest it wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Published {
    /// sha256 of the version object.
    pub version: String,
    /// Blob holding the manifest bytes (rewritten to the public bucket if a
    /// crash fell between committing `HEAD` and writing the manifest).
    pub manifest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Head {
    pub version: u32,
    pub collection: String,
    /// The catalog's seq.
    pub seq: u64,
    pub published: Option<Published>,
    /// Git commit of the software that wrote it.
    pub software: String,
    pub created: String,
    /// The `created` of the HEAD this one replaced.
    pub previous: Option<String>,
    pub components: BTreeMap<String, Snap>,
}

impl Head {
    pub fn blobs(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        for s in self.components.values() {
            s.blobs(&mut out);
        }
        if let Some(p) = &self.published {
            out.insert(p.manifest.clone());
        }
        out
    }
}

pub fn head_key(collection: &str) -> String {
    format!("{collection}/HEAD")
}

/// The current HEAD and its ETag.
pub async fn read_head(store: &dyn Store, collection: &str) -> Result<Option<(Head, String)>> {
    match store.get(&head_key(collection)).await? {
        Some((b, etag)) => {
            let h: Head = serde_json::from_slice(&b).context("HEAD")?;
            anyhow::ensure!(
                h.version == HEAD_VERSION,
                "HEAD version {} is not {HEAD_VERSION}",
                h.version
            );
            Ok(Some((h, etag)))
        }
        None => Ok(None),
    }
}

/// Commit: write `head` as the collection's HEAD only if HEAD is still
/// `expected` (`None`: only if there is no HEAD yet). A copy goes to
/// `{collection}/heads/` first, for rollback. `Ok(false)`: another writer
/// committed in between; nothing changed.
pub async fn commit(store: &dyn Store, head: &Head, expected: Option<&str>) -> Result<bool> {
    Ok(commit_etag(store, head, expected).await?.is_some())
}

/// [`commit`], returning the new HEAD's ETag (for a later commit in the same
/// run), or `None` when another run committed first.
pub async fn commit_etag(
    store: &dyn Store,
    head: &Head,
    expected: Option<&str>,
) -> Result<Option<String>> {
    let body = serde_json::to_vec_pretty(head)?;
    let o = PutOpts {
        content_type: Some("application/json".into()),
        ..Default::default()
    };
    store
        .put(
            &format!(
                "{}/heads/{}-{}.json",
                head.collection,
                head.created.replace(':', ""),
                head.seq
            ),
            body.clone(),
            Cond::None,
            &o,
        )
        .await?;
    let cond = match expected {
        Some(e) => Cond::Match(e.to_string()),
        None => Cond::Absent,
    };
    Ok(
        match store
            .put(&head_key(&head.collection), body, cond, &o)
            .await?
        {
            Put::Done(e) => Some(e),
            Put::Conflict => None,
        },
    )
}

// ------------------------------------------------------------------ blobs

/// Content-addressed blobs in a store, with the set already known to exist.
#[derive(Clone)]
pub struct Blobs {
    store: Arc<dyn Store>,
    known: Arc<Mutex<HashSet<String>>>,
    stats: Arc<Mutex<Stats>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Stats {
    pub uploaded: u64,
    pub uploaded_bytes: u64,
    pub downloaded: u64,
    pub downloaded_bytes: u64,
}

fn blob_key(sha: &str) -> String {
    format!("objects/{sha}")
}

impl Blobs {
    pub fn new(store: Arc<dyn Store>, known: HashSet<String>) -> Self {
        Self {
            store,
            known: Arc::new(Mutex::new(known)),
            stats: Arc::default(),
        }
    }

    pub fn stats(&self) -> Stats {
        self.stats.lock().unwrap().clone()
    }

    /// Store `data` under its hash unless it's known to exist.
    pub async fn put(&self, sha: String, data: Vec<u8>) -> Result<()> {
        if self.known.lock().unwrap().contains(&sha) {
            return Ok(());
        }
        let z = tokio::task::spawn_blocking(move || zstd::encode_all(&data[..], 3)).await??;
        let n = z.len() as u64;
        // An existing blob (Conflict) has the same content by construction.
        self.store
            .put(&blob_key(&sha), z, Cond::Absent, &PutOpts::default())
            .await?;
        {
            let mut s = self.stats.lock().unwrap();
            s.uploaded += 1;
            s.uploaded_bytes += n;
        }
        self.known.lock().unwrap().insert(sha);
        Ok(())
    }

    pub async fn get(&self, sha: &str) -> Result<Vec<u8>> {
        let (z, _) = self
            .store
            .get(&blob_key(sha))
            .await?
            .with_context(|| format!("blob {sha} is missing from {}", self.store.describe()))?;
        {
            let mut s = self.stats.lock().unwrap();
            s.downloaded += 1;
            s.downloaded_bytes += z.len() as u64;
        }
        let sha = sha.to_string();
        tokio::task::spawn_blocking(move || {
            let b = zstd::decode_all(&z[..])?;
            anyhow::ensure!(sha256_hex(&b) == sha, "blob {sha} fails its hash");
            Ok(b)
        })
        .await?
    }
}

/// Run futures with at most [`PARALLEL`] in flight.
struct Pool<'a> {
    inflight: FuturesUnordered<futures::future::BoxFuture<'a, Result<()>>>,
}

impl<'a> Pool<'a> {
    fn new() -> Self {
        Self {
            inflight: FuturesUnordered::new(),
        }
    }
    async fn push(&mut self, f: futures::future::BoxFuture<'a, Result<()>>) -> Result<()> {
        while self.inflight.len() >= PARALLEL {
            self.inflight.next().await.unwrap()?;
        }
        self.inflight.push(f);
        Ok(())
    }
    async fn finish(mut self) -> Result<()> {
        while let Some(r) = self.inflight.next().await {
            r?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------- snapshot

async fn snap_file(blobs: &Blobs, path: &Path) -> Result<FileSnap> {
    let mut f = std::fs::File::open(path).with_context(|| path.display().to_string())?;
    let mut chunks = vec![];
    let mut size = 0u64;
    let mut pool = Pool::new();
    loop {
        let mut buf = vec![0u8; CHUNK];
        let mut n = 0;
        while n < CHUNK {
            let r = f.read(&mut buf[n..])?;
            if r == 0 {
                break;
            }
            n += r;
        }
        if n == 0 {
            break;
        }
        buf.truncate(n);
        size += n as u64;
        let sha = sha256_hex(&buf);
        chunks.push(sha.clone());
        let b = blobs.clone();
        pool.push(Box::pin(async move { b.put(sha, buf).await }))
            .await?;
        if n < CHUNK {
            break;
        }
    }
    pool.finish().await?;
    Ok(FileSnap { size, chunks })
}

/// Half-written files (`*.tmp`) are never part of a snapshot.
fn skip_name(name: &str) -> bool {
    name.ends_with(".tmp")
}

/// Files under `dir` (recursively), as sorted relative `/` paths.
fn walk(dir: &Path) -> Result<Vec<String>> {
    let mut out = vec![];
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).with_context(|| d.display().to_string())? {
            let e = e?;
            let p = e.path();
            if e.file_type()?.is_dir() {
                stack.push(p);
            } else if !skip_name(&e.file_name().to_string_lossy()) {
                out.push(p.strip_prefix(dir)?.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// A directory as one deterministic byte string: magic, then per file (in
/// path order) a u32 path length, the path, a u64 size and the bytes.
pub fn pack_dir(dir: &Path) -> Result<(Vec<u8>, u64)> {
    let files = walk(dir)?;
    let mut out = PACK_MAGIC.to_vec();
    for rel in &files {
        let b = std::fs::read(dir.join(rel))?;
        out.extend((rel.len() as u32).to_le_bytes());
        out.extend(rel.as_bytes());
        out.extend((b.len() as u64).to_le_bytes());
        out.extend(&b);
    }
    Ok((out, files.len() as u64))
}

pub fn unpack(pack: &[u8], dir: &Path) -> Result<u64> {
    anyhow::ensure!(pack.starts_with(PACK_MAGIC), "not a pack");
    let mut i = PACK_MAGIC.len();
    let mut n = 0u64;
    let take = |i: &mut usize, k: usize| -> Result<std::ops::Range<usize>> {
        anyhow::ensure!(*i + k <= pack.len(), "truncated pack");
        let r = *i..*i + k;
        *i += k;
        Ok(r)
    };
    while i < pack.len() {
        let l = u32::from_le_bytes(pack[take(&mut i, 4)?].try_into()?) as usize;
        let rel = std::str::from_utf8(&pack[take(&mut i, l)?])?.to_string();
        anyhow::ensure!(
            !rel.starts_with('/')
                && !rel.contains('\\')
                && rel
                    .split('/')
                    .all(|s| !s.is_empty() && s != "." && s != ".."),
            "unsafe path {rel:?} in pack"
        );
        let size = u64::from_le_bytes(pack[take(&mut i, 8)?].try_into()?) as usize;
        let body = &pack[take(&mut i, size)?];
        let p = dir.join(&rel);
        if let Some(d) = p.parent() {
            std::fs::create_dir_all(d)?;
        }
        std::fs::write(&p, body)?;
        n += 1;
    }
    Ok(n)
}

async fn snap_pack(blobs: &Blobs, dir: &Path) -> Result<PackRef> {
    let d = dir.to_path_buf();
    let (pack, files) = tokio::task::spawn_blocking(move || pack_dir(&d)).await??;
    let sha = sha256_hex(&pack);
    let size = pack.len() as u64;
    blobs.put(sha.clone(), pack).await?;
    Ok(PackRef { sha, size, files })
}

/// Which shards of a [`Kind::Shards`] component a run restored (and so may
/// have changed). The others are carried over from the previous HEAD.
pub type Restored = BTreeMap<&'static str, BTreeSet<String>>;

/// Snapshot `components` of `work` into blobs. `prev` supplies what a run
/// didn't restore: shards outside `restored`, and components whose path is
/// absent are dropped. `restored` = `None` snapshots every shard present.
pub async fn snapshot(
    blobs: &Blobs,
    work: &Path,
    components: &[Component],
    prev: Option<&Head>,
    restored: Option<&Restored>,
) -> Result<BTreeMap<String, Snap>> {
    let mut out = BTreeMap::new();
    for c in components {
        let path = work.join(&c.path);
        if !path.exists() {
            continue;
        }
        let snap = match c.kind {
            Kind::File => Snap::File(snap_file(blobs, &path).await?),
            Kind::Tree => {
                let mut files = BTreeMap::new();
                for rel in walk(&path)? {
                    files.insert(rel.clone(), snap_file(blobs, &path.join(&rel)).await?);
                }
                Snap::Tree { files }
            }
            Kind::Pack => Snap::Pack(snap_pack(blobs, &path).await?),
            Kind::Shards => {
                let mut shards: BTreeMap<String, PackRef> =
                    match prev.and_then(|h| h.components.get(c.name)) {
                        Some(Snap::Shards { shards }) => shards.clone(),
                        _ => BTreeMap::new(),
                    };
                let present: Vec<String> = std::fs::read_dir(&path)?
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect();
                let allowed = restored.map(|r| r.get(c.name).cloned().unwrap_or_default());
                for s in present {
                    if let Some(a) = &allowed {
                        anyhow::ensure!(
                            a.contains(&s),
                            "{}: shard {s} was written without being restored first",
                            c.name
                        );
                    }
                    shards.insert(s.clone(), snap_pack(blobs, &path.join(&s)).await?);
                }
                Snap::Shards { shards }
            }
        };
        out.insert(c.name.to_string(), snap);
    }
    Ok(out)
}

// -------------------------------------------------------------- restore

async fn restore_file(blobs: &Blobs, f: &FileSnap, path: &Path) -> Result<()> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let tmp = path.with_extension("restore.tmp");
    let mut out = std::fs::File::create(&tmp)?;
    // Fetch in parallel, write in order.
    let mut stream =
        futures::stream::iter(f.chunks.iter().map(|sha| blobs.get(sha))).buffered(PARALLEL);
    let mut size = 0u64;
    while let Some(b) = stream.next().await {
        let b = b?;
        size += b.len() as u64;
        std::io::Write::write_all(&mut out, &b)?;
    }
    drop(out);
    anyhow::ensure!(
        size == f.size,
        "{}: restored {size} bytes, expected {}",
        path.display(),
        f.size
    );
    std::fs::rename(&tmp, path)?;
    Ok(())
}

async fn restore_pack(blobs: &Blobs, p: &PackRef, dir: &Path) -> Result<()> {
    let b = blobs.get(&p.sha).await?;
    let d = dir.to_path_buf();
    let n = tokio::task::spawn_blocking(move || unpack(&b, &d)).await??;
    anyhow::ensure!(
        n == p.files,
        "{}: unpacked {n} files, expected {}",
        dir.display(),
        p.files
    );
    Ok(())
}

/// Restore every component of `head` into `work`, except the shards of
/// [`Kind::Shards`] components (see [`restore_shards`]). Existing files are
/// overwritten.
pub async fn restore(
    blobs: &Blobs,
    work: &Path,
    components: &[Component],
    head: &Head,
) -> Result<()> {
    for c in components {
        let path = work.join(&c.path);
        match head.components.get(c.name) {
            None => {}
            Some(Snap::File(f)) => restore_file(blobs, f, &path).await?,
            Some(Snap::Tree { files }) => {
                for (rel, f) in files {
                    restore_file(blobs, f, &path.join(rel)).await?;
                }
            }
            Some(Snap::Pack(p)) => restore_pack(blobs, p, &path).await?,
            Some(Snap::Shards { .. }) => std::fs::create_dir_all(&path)?,
        }
    }
    Ok(())
}

/// Restore these shards (all with `None`) of one [`Kind::Shards`]
/// component. Returns the shards restored (those the HEAD has).
pub async fn restore_shards(
    blobs: &Blobs,
    work: &Path,
    component: &Component,
    head: &Head,
    which: Option<&BTreeSet<String>>,
) -> Result<BTreeSet<String>> {
    let Some(Snap::Shards { shards }) = head.components.get(component.name) else {
        return Ok(BTreeSet::new());
    };
    let dir = work.join(&component.path);
    let todo: Vec<(&String, &PackRef)> = shards
        .iter()
        .filter(|(s, _)| which.map_or(true, |w| w.contains(*s)))
        .collect();
    let mut pool = Pool::new();
    for (s, p) in &todo {
        let (b, d, p) = (blobs.clone(), dir.join(s), (*p).clone());
        pool.push(Box::pin(async move { restore_pack(&b, &p, &d).await }))
            .await?;
    }
    pool.finish().await?;
    Ok(todo.into_iter().map(|(s, _)| s.clone()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::LocalStore;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("state-snap-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn comps() -> Vec<Component> {
        vec![
            Component {
                name: "catalog",
                path: "data/catalog.sqlite".into(),
                kind: Kind::File,
            },
            Component {
                name: "search",
                path: "work/search".into(),
                kind: Kind::Tree,
            },
            Component {
                name: "cache",
                path: "data/cache".into(),
                kind: Kind::Pack,
            },
            Component {
                name: "raw",
                path: "data/raw".into(),
                kind: Kind::Shards,
            },
        ]
    }

    fn write(p: &Path, b: &[u8]) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, b).unwrap();
    }

    fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
        walk(dir)
            .unwrap()
            .into_iter()
            .map(|r| (r.clone(), std::fs::read(dir.join(&r)).unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn round_trip_and_partial_runs() {
        let (w, w2, st) = (tmp("w"), tmp("w2"), tmp("store"));
        let big: Vec<u8> = (0..(CHUNK * 2 + 77)).map(|i| (i * 7 % 251) as u8).collect();
        write(&w.join("data/catalog.sqlite"), &big);
        write(&w.join("work/search/a.idx"), b"segment a");
        write(&w.join("work/search/meta.json"), b"{}");
        write(&w.join("data/cache/x.json.zst"), b"x");
        write(&w.join("data/raw/0a/item1.json.zst"), b"one");
        write(&w.join("data/raw/0b/item2.json.zst"), b"two");
        write(&w.join("data/raw/0b/item3.jpg"), b"three");
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(&st).unwrap());
        let blobs = Blobs::new(store.clone(), HashSet::new());
        let c = comps();
        let snaps = snapshot(&blobs, &w, &c, None, None).await.unwrap();
        let head = Head {
            version: HEAD_VERSION,
            collection: "t".into(),
            seq: 1,
            published: None,
            software: "test".into(),
            created: "2026-11-24T00:00:00Z".into(),
            previous: None,
            components: snaps,
        };
        assert!(commit(&*store, &head, None).await.unwrap());
        assert!(
            !commit(&*store, &head, None).await.unwrap(),
            "second create must conflict"
        );

        // Restore everything elsewhere: identical files.
        let (h, etag) = read_head(&*store, "t").await.unwrap().unwrap();
        let blobs2 = Blobs::new(store.clone(), h.blobs());
        restore(&blobs2, &w2, &c, &h).await.unwrap();
        restore_shards(&blobs2, &w2, &c[3], &h, None).await.unwrap();
        assert_eq!(files(&w), files(&w2));

        // A partial run: only shard 0b restored and changed; 0a carried over.
        let w3 = tmp("w3");
        restore(&blobs2, &w3, &c, &h).await.unwrap();
        let only: BTreeSet<String> = ["0b".to_string()].into();
        let got = restore_shards(&blobs2, &w3, &c[3], &h, Some(&only))
            .await
            .unwrap();
        write(&w3.join("data/raw/0b/item4.json.zst"), b"four");
        let mut big2 = big.clone();
        big2[CHUNK + 5] ^= 1;
        write(&w3.join("data/catalog.sqlite"), &big2);
        let before = blobs2.stats().uploaded;
        let restored: Restored = [("raw", got)].into();
        let snaps = snapshot(&blobs2, &w3, &c, Some(&h), Some(&restored))
            .await
            .unwrap();
        // One changed catalog chunk and one shard pack: two new blobs.
        assert_eq!(blobs2.stats().uploaded - before, 2);
        let Snap::Shards { shards } = &snaps["raw"] else {
            panic!()
        };
        assert_eq!(
            shards["0a"],
            match &h.components["raw"] {
                Snap::Shards { shards } => shards["0a"].clone(),
                _ => panic!(),
            }
        );
        assert_eq!(shards["0b"].files, 3);
        let h2 = Head {
            seq: 2,
            previous: Some(h.created.clone()),
            components: snaps,
            ..h.clone()
        };
        assert!(commit(&*store, &h2, Some(&etag)).await.unwrap());
        assert!(
            !commit(&*store, &h2, Some(&etag)).await.unwrap(),
            "stale etag must conflict"
        );

        // Writing an unrestored shard is refused.
        write(&w3.join("data/raw/0c/item5.json.zst"), b"five");
        assert!(snapshot(&blobs2, &w3, &c, Some(&h), Some(&restored))
            .await
            .is_err());

        for d in [w, w2, w3, st] {
            std::fs::remove_dir_all(d).unwrap();
        }
    }

    #[test]
    fn unpack_rejects_unsafe_paths() {
        let mut p = PACK_MAGIC.to_vec();
        p.extend(5u32.to_le_bytes());
        p.extend(b"../x");
        p.push(b'y');
        p.extend(0u64.to_le_bytes());
        assert!(unpack(&p, &std::env::temp_dir()).is_err());
    }
}

// ------------------------------------------------------------------- gc

#[derive(Debug, Default, Serialize)]
pub struct GcReport {
    pub heads_kept: usize,
    pub heads_deleted: usize,
    pub blobs: usize,
    pub referenced: usize,
    pub new_candidates: usize,
    pub deleted: usize,
    pub dry_run: bool,
}

const GC_KEY: &str = "gc/candidates.json";

/// Delete what no kept HEAD references. Kept: every collection's current
/// HEAD and its `keep` most recent copies under `heads/`. A blob outside
/// them first becomes a candidate (recorded with the time in
/// `gc/candidates.json`); a later run deletes it if it is still
/// unreferenced and was recorded at least `grace` ago, so a run that has
/// uploaded blobs but not yet committed is never cut short.
pub async fn gc(
    store: &dyn Store,
    keep: usize,
    grace: chrono::Duration,
    now: chrono::DateTime<chrono::Utc>,
    dry_run: bool,
) -> Result<GcReport> {
    let mut rep = GcReport {
        dry_run,
        ..Default::default()
    };
    let keys = store.list("").await?;
    let mut referenced = HashSet::new();
    let mut by_collection: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for k in &keys {
        if let Some((c, rest)) = k.split_once('/') {
            if rest == "HEAD" {
                if let Some((h, _)) = read_head(store, c).await? {
                    referenced.extend(h.blobs());
                }
            } else if rest.starts_with("heads/") {
                by_collection
                    .entry(c.to_string())
                    .or_default()
                    .push(k.clone());
            }
        }
    }
    let mut delete_heads = vec![];
    for (_, mut hs) in by_collection {
        hs.sort();
        let cut = hs.len().saturating_sub(keep);
        for k in &hs[cut..] {
            if let Some((b, _)) = store.get(k).await? {
                let h: Head = serde_json::from_slice(&b)?;
                referenced.extend(h.blobs());
                rep.heads_kept += 1;
            }
        }
        delete_heads.extend(hs[..cut].iter().cloned());
    }
    let blobs: Vec<String> = keys
        .iter()
        .filter_map(|k| k.strip_prefix("objects/").map(str::to_string))
        .collect();
    rep.blobs = blobs.len();
    rep.referenced = referenced.len();
    let old: BTreeMap<String, String> = match store.get(GC_KEY).await? {
        Some((b, _)) => serde_json::from_slice(&b)?,
        None => BTreeMap::new(),
    };
    let mut next: BTreeMap<String, String> = BTreeMap::new();
    let mut delete = vec![];
    for b in blobs.iter().filter(|b| !referenced.contains(*b)) {
        match old.get(b) {
            Some(t) => {
                let seen = chrono::DateTime::parse_from_rfc3339(t)?.with_timezone(&chrono::Utc);
                if now - seen >= grace {
                    delete.push(b.clone());
                } else {
                    next.insert(b.clone(), t.clone());
                }
            }
            None => {
                rep.new_candidates += 1;
                next.insert(
                    b.clone(),
                    now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                );
            }
        }
    }
    rep.deleted = delete.len();
    rep.heads_deleted = delete_heads.len();
    if !dry_run {
        for b in &delete {
            store.delete(&format!("objects/{b}")).await?;
        }
        for k in &delete_heads {
            store.delete(k).await?;
        }
        store
            .put(
                GC_KEY,
                serde_json::to_vec_pretty(&next)?,
                Cond::None,
                &PutOpts::default(),
            )
            .await?;
    }
    Ok(rep)
}

#[cfg(test)]
mod gc_tests {
    use super::*;
    use crate::store::LocalStore;

    #[tokio::test]
    async fn gc_waits_out_the_grace_period() {
        let d = std::env::temp_dir().join(format!("state-gc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let store: Arc<dyn Store> = Arc::new(LocalStore::new(&d).unwrap());
        let blobs = Blobs::new(store.clone(), HashSet::new());
        blobs
            .put(sha256_hex(b"kept"), b"kept".to_vec())
            .await
            .unwrap();
        blobs
            .put(sha256_hex(b"orphan"), b"orphan".to_vec())
            .await
            .unwrap();
        let head = Head {
            version: HEAD_VERSION,
            collection: "c".into(),
            seq: 1,
            published: None,
            software: "t".into(),
            created: "2026-11-24T00:00:00Z".into(),
            previous: None,
            components: [(
                "f".to_string(),
                Snap::File(FileSnap {
                    size: 4,
                    chunks: vec![sha256_hex(b"kept")],
                }),
            )]
            .into(),
        };
        assert!(commit(&*store, &head, None).await.unwrap());
        let t0 = chrono::Utc::now();
        let g = chrono::Duration::hours(24);
        let r = gc(&*store, 3, g, t0, false).await.unwrap();
        assert_eq!((r.new_candidates, r.deleted), (1, 0));
        let r = gc(&*store, 3, g, t0 + chrono::Duration::hours(1), false)
            .await
            .unwrap();
        assert_eq!((r.new_candidates, r.deleted), (0, 0));
        let r = gc(&*store, 3, g, t0 + chrono::Duration::hours(25), false)
            .await
            .unwrap();
        assert_eq!(r.deleted, 1);
        assert!(blobs.get(&sha256_hex(b"kept")).await.is_ok());
        assert!(blobs.get(&sha256_hex(b"orphan")).await.is_err());
        std::fs::remove_dir_all(d).unwrap();
    }
}
