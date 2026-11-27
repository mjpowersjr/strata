//! Reading a published v2 index in place (docs/format.md section 8):
//! manifest → version object → hot packs → tantivy over verified chunks,
//! and prolly-tree maps over verified nodes. Native; every object goes
//! through `strata_fetch`'s verified cache, from any `ChunkSource`.
//!
//! Blocking API (like the v1 reader): call from a plain thread or
//! `spawn_blocking`, never directly from async code.

use crate::prolly::{parse_hash, Fetcher, Reader as Nodes, Root};
use strata_core::search::{FileRef, SegmentObj};
use crate::version::{Manifest, Version};
use anyhow::{Context, Result};
use strata_fetch::{ChunkCache, ChunkSource, RemoteError};
pub use strata_core::keys::ValueRef;
use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen, Index};

/// Serves one already-fetched pack part to the cache, which verifies it.
struct Given(Vec<u8>);

impl ChunkSource for Given {
    fn fetch(&self, _sha256: &str, _size: u64) -> Result<Vec<u8>, RemoteError> {
        Ok(self.0.clone())
    }
    fn fetch_path(&self, path: &str) -> Result<Vec<u8>, RemoteError> {
        Err(RemoteError::Io(format!("no path {path}")))
    }
}

#[derive(Clone)]
struct ObjDir {
    files: Arc<BTreeMap<String, FileRef>>,
    /// Files held whole in memory (decompressed delete bitsets).
    inline: Arc<BTreeMap<String, Arc<Vec<u8>>>>,
    chunk: u64,
    cache: Arc<ChunkCache>,
    source: Arc<dyn ChunkSource>,
}

impl std::fmt::Debug for ObjDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjDir")
    }
}

impl ObjDir {
    fn len_of(&self, name: &str, path: &Path) -> Result<u64, OpenReadError> {
        if let Some(b) = self.inline.get(name) {
            return Ok(b.len() as u64);
        }
        Ok(self
            .files
            .get(name)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(PathBuf::from(path)))?
            .size)
    }

    fn chunk_len(&self, f: &FileRef, i: usize) -> u64 {
        (f.size - i as u64 * self.chunk).min(self.chunk)
    }

    fn get(&self, f: &FileRef, i: usize) -> io::Result<Arc<[u8]>> {
        self.cache
            .get(&f.chunks[i], self.chunk_len(f, i), &*self.source)
            .map_err(io::Error::other)
    }

    fn read(&self, name: &str, range: Range<u64>) -> io::Result<Vec<u8>> {
        if let Some(b) = self.inline.get(name) {
            let end = (range.end as usize).min(b.len());
            return Ok(b[(range.start as usize).min(end)..end].to_vec());
        }
        let f = self
            .files
            .get(name)
            .ok_or_else(|| io::Error::other(format!("no file {name}")))?;
        if range.start >= range.end {
            return Ok(vec![]);
        }
        let (a, b) = (
            (range.start / self.chunk) as usize,
            ((range.end - 1) / self.chunk) as usize,
        );
        // Fetch the missing chunks of one read in parallel.
        let missing: Vec<usize> = (a..=b)
            .filter(|&i| !self.cache.contains(&f.chunks[i]))
            .collect();
        if missing.len() > 1 {
            // At most 32 requests in flight (a column can be hundreds of chunks).
            for group in missing.chunks(32) {
                std::thread::scope(|s| {
                    let hs: Vec<_> = group
                        .iter()
                        .map(|&i| s.spawn(move || self.get(f, i)))
                        .collect();
                    for h in hs {
                        h.join().map_err(|_| io::Error::other("fetch thread"))??;
                    }
                    Ok::<(), io::Error>(())
                })?;
            }
        }
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for i in a..=b {
            let d = self.get(f, i)?;
            let start = i as u64 * self.chunk;
            let lo = range.start.max(start) - start;
            let hi = range.end.min(start + d.len() as u64) - start;
            out.extend_from_slice(&d[lo as usize..hi as usize]);
        }
        Ok(out)
    }
}

#[derive(Debug)]
struct Handle {
    dir: ObjDir,
    name: String,
    len: u64,
}

impl HasLen for Handle {
    fn len(&self) -> usize {
        self.len as usize
    }
}

#[async_trait::async_trait]
impl FileHandle for Handle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(OwnedBytes::new(
            self.dir
                .read(&self.name, range.start as u64..range.end as u64)?,
        ))
    }
    /// Each async read runs on its own thread, so the warm-up's joined
    /// futures fetch in parallel.
    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let (dir, name) = (self.dir.clone(), self.name.clone());
        let (tx, rx) = futures::channel::oneshot::channel();
        std::thread::spawn(move || {
            let _ = tx.send(dir.read(&name, range.start as u64..range.end as u64));
        });
        Ok(OwnedBytes::new(
            rx.await.map_err(|_| io::Error::other("reader thread"))??,
        ))
    }
}

impl Directory for ObjDir {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let name = path.to_string_lossy().to_string();
        let len = self.len_of(&name, path)?;
        Ok(Arc::new(Handle {
            dir: self.clone(),
            name,
            len,
        }))
    }
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(io::Error::other("read-only")),
            filepath: path.to_path_buf(),
        })
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let n = path.to_string_lossy();
        Ok(self.files.contains_key(&*n) || self.inline.contains_key(&*n))
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::IoError {
            io_error: Arc::new(io::Error::other("read-only")),
            filepath: path.to_path_buf(),
        })
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let name = path.to_string_lossy().to_string();
        let len = self.len_of(&name, path)?;
        self.read(&name, 0..len)
            .map_err(|e| OpenReadError::IoError {
                io_error: Arc::new(e),
                filepath: path.to_path_buf(),
            })
    }
    fn atomic_write(&self, _: &Path, _: &[u8]) -> io::Result<()> {
        Err(io::Error::other("read-only"))
    }
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
    fn watch(&self, _cb: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
    fn acquire_lock(
        &self,
        _l: &tantivy::directory::Lock,
    ) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        Ok(tantivy::directory::DirectoryLock::from(Box::new(())))
    }
}

pub use strata_core::query::{Hit, Mode, Opened, SearchQuery, SearchResult, EXPANSION_CAP};

/// One published index version, open for reading.
pub struct V2Index {
    pub manifest: Manifest,
    pub version: Version,
    source: Arc<dyn ChunkSource>,
    cache: Arc<ChunkCache>,
    q: Opened,
    /// The version's segment objects, in `version.search.segments` order.
    segments: Vec<SegmentObj>,
}

impl V2Index {
    /// Fetch `{index}/manifest.json` and open that version.
    pub fn open(
        source: Arc<dyn ChunkSource>,
        cache: Arc<ChunkCache>,
        state_dir: &Path,
        index: &str,
    ) -> Result<Self> {
        let mbytes = source.fetch_path(&format!("{index}/manifest.json"))?;
        let manifest: Manifest = serde_json::from_slice(&mbytes).context("manifest")?;
        let ix = Self::from_manifest(source, cache, manifest)?;
        ix.save_manifest(state_dir)?;
        Ok(ix)
    }

    /// Open the version last opened for `index`, without the network: the
    /// version object and hot packs come from the cache. Anything else not
    /// cached fails with "not available offline" when read.
    pub fn open_cached(
        source: Arc<dyn ChunkSource>,
        cache: Arc<ChunkCache>,
        state_dir: &Path,
        index: &str,
    ) -> Result<Self> {
        let p = saved_path(state_dir, index);
        let b = std::fs::read(&p).map_err(|_| RemoteError::Offline {
            what: format!("no saved version of {index}"),
        })?;
        let manifest: Manifest = serde_json::from_slice(&b).context("saved manifest")?;
        Self::from_manifest(source, cache, manifest)
    }

    /// Record this version as the one last opened (what
    /// [`open_cached`](Self::open_cached) opens).
    pub fn save_manifest(&self, state_dir: &Path) -> Result<()> {
        let p = saved_path(state_dir, &self.manifest.index);
        std::fs::create_dir_all(p.parent().unwrap())?;
        let tmp = p.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(&self.manifest)?)?;
        std::fs::rename(&tmp, &p)?;
        Ok(())
    }

    /// The published manifest, if it names a different version than the
    /// one open (one small, 60-second-cached request).
    pub fn check_update(&self) -> Result<Option<Manifest>> {
        let b = self
            .source
            .fetch_path(&format!("{}/manifest.json", self.manifest.index))?;
        let m: Manifest = serde_json::from_slice(&b).context("manifest")?;
        Ok((m.version != self.manifest.version).then_some(m))
    }

    /// Open a newer version in the same cache: unchanged objects (old
    /// segments, unchanged tree nodes) are already there.
    pub fn reopen(&self, manifest: Manifest, state_dir: &Path) -> Result<Self> {
        let ix = Self::from_manifest(self.source.clone(), self.cache.clone(), manifest)?;
        ix.save_manifest(state_dir)?;
        Ok(ix)
    }

    /// Every object this version references, with its size when known (0
    /// for tree nodes). Reads the internal tree nodes, not the leaves,
    /// through a reader of its own: a whole index's internal nodes, decoded,
    /// aren't kept in this version's node cache for as long as it is open.
    pub fn objects(&self) -> Result<Vec<(String, u64)>> {
        let nodes = Nodes::with_fetcher(node_fetcher(&self.cache, &self.source));
        list_objects(&self.manifest, &self.version, &self.segments, &nodes)
    }

    /// The objects of the version a manifest names, without opening it:
    /// reads its version object, segment objects and internal tree nodes
    /// (through the cache, so offline when they're cached).
    pub fn objects_of(
        source: Arc<dyn ChunkSource>,
        cache: Arc<ChunkCache>,
        manifest: &Manifest,
    ) -> Result<(Version, Vec<(String, u64)>)> {
        let get = |sha: &str, size: u64| cache.get(sha, size, &*source);
        let vb = get(&manifest.version.sha256, manifest.version.size)?;
        let version: Version = serde_json::from_slice(&vb).context("version object")?;
        let mut segments = vec![];
        for seg in &version.search.segments {
            let b = get(&seg.object.sha256, seg.object.size)?;
            segments.push(serde_json::from_slice(&b).context("segment object")?);
        }
        let nodes = Nodes::with_fetcher(node_fetcher(&cache, &source));
        let objs = list_objects(manifest, &version, &segments, &nodes)?;
        Ok((version, objs))
    }

    /// Fetch every object of this version (only those not cached, in
    /// parallel) and pin them, so the whole index works offline and
    /// survives cache eviction.
    pub fn pin(&self) -> Result<PinReport> {
        let objs = self.objects()?;
        let missing: Vec<&(String, u64)> = objs
            .iter()
            .filter(|(s, _)| !self.cache.on_disk(s))
            .collect();
        for (sha, _) in &objs {
            self.cache.pin(sha);
        }
        let before = self.cache.stats().bytes_fetched;
        for group in missing.chunks(32) {
            std::thread::scope(|sc| -> Result<()> {
                let hs: Vec<_> = group
                    .iter()
                    .map(|(sha, size)| {
                        sc.spawn(move || self.cache.fetch_to_disk(sha, *size, &*self.source))
                    })
                    .collect();
                for h in hs {
                    h.join().map_err(|_| anyhow::anyhow!("pin thread"))??;
                }
                Ok(())
            })?;
        }
        Ok(PinReport {
            objects: objs.len(),
            fetched: missing.len(),
            bytes_fetched: self.cache.stats().bytes_fetched - before,
        })
    }

    /// Unpin this version's objects, except those `keep` (a newer version)
    /// still references.
    pub fn unpin_except(&self, keep: Option<&V2Index>) -> Result<usize> {
        let keep: std::collections::HashSet<String> = match keep {
            Some(k) => k.objects()?.into_iter().map(|o| o.0).collect(),
            None => Default::default(),
        };
        let mut n = 0;
        for (sha, _) in self.objects()? {
            if !keep.contains(&sha) {
                self.cache.unpin(&sha);
                n += 1;
            }
        }
        Ok(n)
    }

    /// Move a pinned copy to `newer`: pin it (fetching only what is new),
    /// then release what only this version used.
    pub fn sync_pin(&self, newer: &V2Index) -> Result<PinReport> {
        let r = newer.pin()?;
        self.unpin_except(Some(newer))?;
        Ok(r)
    }

    /// Open the version a manifest names, without recording it as the one
    /// last opened ([`save_manifest`](Self::save_manifest) does).
    pub fn open_manifest(
        source: Arc<dyn ChunkSource>,
        cache: Arc<ChunkCache>,
        manifest: Manifest,
    ) -> Result<Self> {
        Self::from_manifest(source, cache, manifest)
    }

    fn from_manifest(
        source: Arc<dyn ChunkSource>,
        cache: Arc<ChunkCache>,
        manifest: Manifest,
    ) -> Result<Self> {
        if !crate::version::readable(&manifest) {
            return Err(RemoteError::SpecUnsupported(format!(
                "unsupported index format {} {}",
                manifest.format, manifest.spec_version
            ))
            .into());
        }
        let vb = cache.get(&manifest.version.sha256, manifest.version.size, &*source)?;
        let version: Version = serde_json::from_slice(&vb).context("version object")?;
        // Per segment, in parallel: its object, hot pack and delete bitset.
        // The pack is split into its parts (each verified into the cache);
        // when the object is cached and every part too, nothing is fetched.
        type Loaded = (SegmentObj, Option<(String, Arc<Vec<u8>>)>);
        let loaded: Vec<Loaded> = std::thread::scope(|s| -> Result<Vec<Loaded>> {
            let hs: Vec<_> = version
                .search
                .segments
                .iter()
                .map(|seg| {
                    let (cache, source) = (cache.clone(), source.clone());
                    s.spawn(move || -> Result<Loaded> {
                        let get =
                            |r: &crate::version::ObjRef| cache.get(&r.sha256, r.size, &*source);
                        let (obj, pack, del) = std::thread::scope(|s2| {
                            let obj_cached = cache.contains(&seg.object.sha256);
                            let o = s2.spawn(|| get(&seg.object));
                            // Cold: fetch the pack alongside the object.
                            let p = (!obj_cached).then(|| s2.spawn(|| get(&seg.hot)));
                            let d = seg.del.as_ref().map(|d| s2.spawn(move || get(&d.object)));
                            (
                                o.join().unwrap(),
                                p.map(|h| h.join().unwrap()),
                                d.map(|h| h.join().unwrap()),
                            )
                        });
                        let obj: SegmentObj =
                            serde_json::from_slice(&obj?).context("segment object")?;
                        let pack = match pack {
                            Some(p) => Some(p?),
                            None if obj.hot_parts.iter().any(|(s, _)| !cache.contains(s)) => {
                                Some(get(&seg.hot)?)
                            }
                            None => None,
                        };
                        if let Some(pack) = pack {
                            let mut off = 0usize;
                            for (psha, len) in &obj.hot_parts {
                                let part = pack[off..off + *len as usize].to_vec();
                                off += *len as usize;
                                // Parts are cached in stored form, like
                                // any object fetched on its own.
                                cache.get(
                                    psha,
                                    *len,
                                    &Given(strata_object::encode(&part, strata_object::Codec::Raw)),
                                )?;
                            }
                        }
                        let del = match (del, &seg.del) {
                            (Some(raw), Some(d)) => {
                                let raw = raw?;
                                anyhow::ensure!(raw.len() as u64 == d.size, "delete bitset size");
                                Some((d.name.clone(), Arc::new(raw.to_vec())))
                            }
                            _ => None,
                        };
                        Ok((obj, del))
                    })
                })
                .collect();
            hs.into_iter()
                .map(|h| h.join().map_err(|_| anyhow::anyhow!("segment thread"))?)
                .collect()
        })?;
        let mut files = version.search.files.clone();
        let mut inline = BTreeMap::new();
        let mut segments = vec![];
        for (obj, del) in loaded {
            files.extend(obj.files.iter().map(|(n, f)| (n.clone(), f.clone())));
            if let Some((n, b)) = del {
                inline.insert(n, b);
            }
            segments.push(obj);
        }
        let dir = ObjDir {
            files: Arc::new(files),
            inline: Arc::new(inline),
            chunk: version.search.chunk_size,
            cache: cache.clone(),
            source: source.clone(),
        };
        let tindex = Index::open(dir)?;
        let q = Opened::new(
            version.clone(),
            tindex,
            Nodes::with_fetcher(node_fetcher(&cache, &source)),
        )?;
        Ok(Self {
            manifest,
            version,
            source,
            cache,
            q,
            segments,
        })
    }

    /// Search: BM25 over the text fields with their boosts, filters as
    /// required clauses; typo matching as a fallback under 5 hits.
    pub fn search(&self, q: &SearchQuery) -> Result<SearchResult> {
        self.q.search(q)
    }

    /// A table row by its primary key values, as column → value.
    pub fn get(
        &self,
        table: &str,
        key: &[ValueRef<'_>],
    ) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
        self.q.get(table, key)
    }

    /// Rows of a table (or, for an index map, the rows it points to) whose
    /// key starts with `prefix`; the last value may be a text prefix.
    pub fn range(
        &self,
        map: &str,
        prefix: &[ValueRef<'_>],
        partial_text: bool,
        limit: Option<u64>,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        self.q.range(map, prefix, partial_text, limit)
    }

    /// [`range`](Self::range) from a cursor (`Opened::range_from`): rows,
    /// and the cursor of the next page (`None` at the end).
    #[allow(clippy::type_complexity)]
    pub fn range_from(
        &self,
        map: &str,
        prefix: &[ValueRef<'_>],
        partial_text: bool,
        from: Option<&[u8]>,
        limit: Option<u64>,
    ) -> Result<(
        Vec<serde_json::Map<String, serde_json::Value>>,
        Option<Vec<u8>>,
    )> {
        self.q.range_from(map, prefix, partial_text, from, limit)
    }

    /// The cache and source this index reads through.
    pub fn cache(&self) -> &Arc<ChunkCache> {
        &self.cache
    }
    pub fn source(&self) -> &Arc<dyn ChunkSource> {
        &self.source
    }

    /// Drop the decoded tree nodes this version holds in memory.
    pub fn clear_memory(&self) {
        self.q.nodes.clear_cache();
    }

    /// Round trips, nodes and bytes the store reader has fetched so far.
    pub fn store_counts(&self) -> crate::prolly::Counts {
        self.q.nodes.counts()
    }
}

fn node_fetcher(cache: &Arc<ChunkCache>, source: &Arc<dyn ChunkSource>) -> Fetcher {
    let (cache, source) = (cache.clone(), source.clone());
    Arc::new(move |h| {
        Ok(cache
            .get(&crate::prolly::hash_hex(h), 0, &*source)?
            .to_vec())
    })
}

fn list_objects(
    manifest: &Manifest,
    version: &Version,
    segments: &[SegmentObj],
    nodes: &Nodes,
) -> Result<Vec<(String, u64)>> {
    let mut out: Vec<(String, u64)> = vec![(
        manifest.version.sha256.clone(),
        manifest.version.size,
    )];
    let cs = version.search.chunk_size;
    let files = version
        .search
        .files
        .values()
        .chain(segments.iter().flat_map(|s| s.files.values()));
    for f in files {
        for (i, c) in f.chunks.iter().enumerate() {
            out.push((c.clone(), (f.size - i as u64 * cs).min(cs)));
        }
    }
    for s in &version.search.segments {
        out.push((s.object.sha256.clone(), s.object.size));
        out.push((s.hot.sha256.clone(), s.hot.size));
        if let Some(d) = &s.del {
            out.push((d.object.sha256.clone(), d.object.size));
        }
    }
    for m in version.store.maps.values() {
        let root = Root {
            hash: parse_hash(&m.root)?,
            depth: m.depth,
            count: m.count,
            levels: vec![],
        };
        for h in nodes.node_hashes(&root)? {
            out.push((crate::prolly::hash_hex(&h), 0));
        }
    }
    out.sort();
    out.dedup_by(|a, b| a.0 == b.0);
    Ok(out)
}

/// What a pin did.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PinReport {
    pub objects: usize,
    pub fetched: usize,
    pub bytes_fetched: u64,
}

fn saved_path(state_dir: &Path, index: &str) -> PathBuf {
    let safe: String = index
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    state_dir.join("v2-manifests").join(format!("{safe}.json"))
}
