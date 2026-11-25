//! The v2 reader without threads (a browser, a Web Worker, Node): the
//! query code is `strata_core::query`, run synchronously over objects
//! already fetched. A read of anything not fetched yet is recorded and
//! fails; the reader then fetches everything recorded, in parallel and
//! verified by hash, and runs the query again. A search takes a few passes
//! (dictionary blocks, then postings, then stored documents), each one
//! round of parallel requests.
//!
//! Objects are kept in memory for the life of the reader (an offline cache
//! in IndexedDB or the Cache API would sit behind [`Fetch`]).

use anyhow::{Context, Result};
use strata_core::keys::{Val, ValueRef};
use strata_core::prolly::{Fetcher, Reader as Nodes};
use strata_core::query::{Opened, SearchQuery, SearchResult};
use strata_core::search::{FileRef, SegmentObj};
use strata_core::version::{Manifest, Version};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen, Index};

/// Where bytes come from: paths under the published root (`{index}/manifest.json`,
/// `objects/{sha256}`).
#[async_trait::async_trait(?Send)]
pub trait Fetch {
    async fn get(&self, path: &str) -> Result<Vec<u8>>;
}

/// Fetched objects, and what the last pass found missing.
#[derive(Default)]
pub struct Store {
    objects: Mutex<HashMap<String, Arc<Vec<u8>>>>,
    missing: Mutex<BTreeSet<String>>,
    stats: Mutex<Stats>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Stats {
    pub passes: u64,
    pub requests: u64,
    pub bytes: u64,
}

impl Store {
    fn get(&self, sha: &str) -> Option<Arc<Vec<u8>>> {
        let o = self.objects.lock().unwrap().get(sha).cloned();
        if o.is_none() {
            self.missing.lock().unwrap().insert(sha.to_string());
        }
        o
    }
    fn put(&self, sha: String, b: Vec<u8>) {
        self.objects.lock().unwrap().insert(sha, Arc::new(b));
    }
    fn has(&self, sha: &str) -> bool {
        self.objects.lock().unwrap().contains_key(sha)
    }
    fn take_missing(&self) -> BTreeSet<String> {
        std::mem::take(&mut *self.missing.lock().unwrap())
    }
}

fn sha_hex(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

/// Fetch objects (concurrently), verify each against its name, keep them.
async fn fetch_all(f: &dyn Fetch, store: &Store, shas: &BTreeSet<String>) -> Result<()> {
    let got = futures::future::try_join_all(shas.iter().map(|sha| async move {
        let stored = f.get(&format!("objects/{sha}")).await?;
        let n = stored.len() as u64;
        let b = strata_object::decode_verified(&stored, sha, 0)
            .map_err(|e| anyhow::anyhow!("object {sha}: {e}"))?;
        Ok::<_, anyhow::Error>((sha.clone(), b, n))
    }))
    .await?;
    let mut st = store.stats.lock().unwrap();
    st.requests += got.len() as u64;
    for (sha, b, n) in got {
        st.bytes += n;
        store.put(sha, b);
    }
    Ok(())
}

/// The index files over the store: chunks that are there are served,
/// others recorded as missing (the read fails).
#[derive(Clone)]
struct PassDir {
    files: Arc<BTreeMap<String, FileRef>>,
    inline: Arc<BTreeMap<String, Arc<Vec<u8>>>>,
    chunk: u64,
    store: Arc<Store>,
}

impl std::fmt::Debug for PassDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PassDir")
    }
}

fn missing_err() -> io::Error {
    io::Error::other("not fetched yet")
}

impl PassDir {
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
        // Look every chunk up first, so one pass records them all.
        let parts: Vec<Option<Arc<Vec<u8>>>> =
            (a..=b).map(|i| self.store.get(&f.chunks[i])).collect();
        if parts.iter().any(|p| p.is_none()) {
            return Err(missing_err());
        }
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for (i, d) in (a..=b).zip(parts) {
            let d = d.unwrap();
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
    dir: PassDir,
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
    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_bytes(range)
    }
}

impl Directory for PassDir {
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

/// Passes allowed per call before giving up.
pub const MAX_PASSES: usize = 30;

/// An open version.
pub struct WebReader<F: Fetch> {
    fetch: F,
    store: Arc<Store>,
    pub manifest: Manifest,
    opened: Opened,
}

impl<F: Fetch> WebReader<F> {
    /// Fetch `{index}/manifest.json`, the version object, every segment's
    /// object, hot pack and delete object (in parallel), then open.
    pub async fn open(fetch: F, index: &str) -> Result<Self> {
        let store = Arc::new(Store::default());
        let mb = fetch.get(&format!("{index}/manifest.json")).await?;
        let manifest: Manifest = serde_json::from_slice(&mb).context("manifest")?;
        anyhow::ensure!(
            strata_core::version::readable(&manifest),
            "unsupported index format {} {}",
            manifest.format,
            manifest.spec_version
        );
        fetch_all(&fetch, &store, &[manifest.version.sha256.clone()].into()).await?;
        let version: Version =
            serde_json::from_slice(&store.get(&manifest.version.sha256).unwrap())
                .context("version object")?;
        let mut first: BTreeSet<String> = BTreeSet::new();
        for s in &version.search.segments {
            first.insert(s.object.sha256.clone());
            first.insert(s.hot.sha256.clone());
            if let Some(d) = &s.del {
                first.insert(d.object.sha256.clone());
            }
        }
        for f in version.search.files.values() {
            first.extend(f.chunks.iter().cloned());
        }
        fetch_all(&fetch, &store, &first).await?;
        let mut files = version.search.files.clone();
        let mut inline = BTreeMap::new();
        for s in &version.search.segments {
            let obj: SegmentObj = serde_json::from_slice(&store.get(&s.object.sha256).unwrap())?;
            // The hot pack's parts, verified one by one.
            let pack = store.get(&s.hot.sha256).unwrap();
            let mut off = 0usize;
            for (psha, len) in &obj.hot_parts {
                let part = pack[off..off + *len as usize].to_vec();
                off += *len as usize;
                anyhow::ensure!(
                    sha_hex(&part) == *psha,
                    "hot pack part {psha} failed verification"
                );
                if !store.has(psha) {
                    store.put(psha.clone(), part);
                }
            }
            if let Some(d) = &s.del {
                let raw = store.get(&d.object.sha256).unwrap().to_vec();
                anyhow::ensure!(raw.len() as u64 == d.size, "delete bitset size");
                inline.insert(d.name.clone(), Arc::new(raw));
            }
            files.extend(obj.files.into_iter());
        }
        store.take_missing();
        let dir = PassDir {
            files: Arc::new(files),
            inline: Arc::new(inline),
            chunk: version.search.chunk_size,
            store: store.clone(),
        };
        let nodes = {
            let st = store.clone();
            let f: Fetcher = Arc::new(move |h| {
                let name = strata_core::prolly::hash_hex(h);
                st.get(&name)
                    .map(|b| b.to_vec())
                    .ok_or_else(|| anyhow::anyhow!("not fetched yet"))
            });
            Nodes::with_fetcher(f)
        };
        // Opening reads only the hot packs; if it misses, fetch and retry.
        let mut nodes = Some(nodes);
        let mut opened = None;
        for _ in 0..MAX_PASSES {
            store.stats.lock().unwrap().passes += 1;
            match Index::open(dir.clone())
                .map_err(anyhow::Error::from)
                .and_then(|ix| Opened::new(version.clone(), ix, nodes.clone().unwrap()))
            {
                Ok(o) => {
                    opened = Some(o);
                    break;
                }
                Err(e) => {
                    let miss = store.take_missing();
                    if miss.is_empty() {
                        return Err(e);
                    }
                    fetch_all(&fetch, &store, &miss).await?;
                }
            }
        }
        let _ = nodes.take();
        Ok(Self {
            fetch,
            store,
            manifest,
            opened: opened.context("could not open")?,
        })
    }

    /// Run `f` until nothing it reads is missing.
    async fn passes<T>(&self, f: impl Fn(&Opened) -> Result<T>) -> Result<T> {
        for _ in 0..MAX_PASSES {
            self.store.stats.lock().unwrap().passes += 1;
            match f(&self.opened) {
                Ok(v) => {
                    self.store.take_missing();
                    return Ok(v);
                }
                Err(e) => {
                    let miss = self.store.take_missing();
                    if miss.is_empty() {
                        return Err(e);
                    }
                    fetch_all(&self.fetch, &self.store, &miss).await?;
                }
            }
        }
        anyhow::bail!("gave up after {MAX_PASSES} passes")
    }

    pub async fn search(&self, q: &SearchQuery) -> Result<SearchResult> {
        self.passes(|o| o.search(q)).await
    }

    pub async fn get(
        &self,
        table: &str,
        key: &[Val],
    ) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
        let k: Vec<ValueRef> = key.iter().map(|v| v.as_ref()).collect();
        self.passes(|o| o.get(table, &k)).await
    }

    pub async fn range(
        &self,
        map: &str,
        prefix: &[Val],
        partial: bool,
        limit: Option<u64>,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        let k: Vec<ValueRef> = prefix.iter().map(|v| v.as_ref()).collect();
        self.passes(|o| o.range(map, &k, partial, limit)).await
    }

    /// [`range`](Self::range) from a cursor or seek key (raw, inclusive);
    /// also the cursor of the next page.
    pub async fn range_from(
        &self,
        map: &str,
        prefix: &[Val],
        partial: bool,
        from: Option<&[u8]>,
        limit: Option<u64>,
    ) -> Result<(
        Vec<serde_json::Map<String, serde_json::Value>>,
        Option<Vec<u8>>,
    )> {
        let k: Vec<ValueRef> = prefix.iter().map(|v| v.as_ref()).collect();
        self.passes(|o| o.range_from(map, &k, partial, from, limit))
            .await
    }

    /// Whether the published manifest names another version.
    pub async fn check_update(&self) -> Result<bool> {
        let b = self
            .fetch
            .get(&format!("{}/manifest.json", self.manifest.index))
            .await?;
        let m: Manifest = serde_json::from_slice(&b)?;
        Ok(m.version != self.manifest.version)
    }

    pub fn stats(&self) -> Stats {
        self.store.stats.lock().unwrap().clone()
    }

    /// The open version object.
    pub fn version(&self) -> &Version {
        &self.opened.version
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use js_sys::{Function, Promise, Reflect, Uint8Array};
    use std::rc::Rc;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::{future_to_promise, JsFuture};

    /// `fetch` from the global scope (a page, a Web Worker, Node 18+).
    pub struct JsFetch {
        base: String,
    }

    fn js_err(e: JsValue) -> anyhow::Error {
        anyhow::anyhow!("{e:?}")
    }

    #[async_trait::async_trait(?Send)]
    impl Fetch for JsFetch {
        async fn get(&self, path: &str) -> Result<Vec<u8>> {
            let url = format!("{}/{path}", self.base.trim_end_matches('/'));
            let g = js_sys::global();
            let f: Function = Reflect::get(&g, &"fetch".into())
                .map_err(js_err)?
                .dyn_into()
                .map_err(js_err)?;
            let p: Promise = f
                .call1(&g, &url.clone().into())
                .map_err(js_err)?
                .dyn_into()
                .map_err(js_err)?;
            let resp = JsFuture::from(p).await.map_err(js_err)?;
            let ok = Reflect::get(&resp, &"ok".into())
                .map_err(js_err)?
                .as_bool()
                .unwrap_or(false);
            if !ok {
                let status = Reflect::get(&resp, &"status".into())
                    .map_err(js_err)?
                    .as_f64()
                    .unwrap_or(0.0);
                anyhow::bail!("GET {url}: HTTP {status}");
            }
            let ab: Function = Reflect::get(&resp, &"arrayBuffer".into())
                .map_err(js_err)?
                .dyn_into()
                .map_err(js_err)?;
            let p: Promise = ab
                .call0(&resp)
                .map_err(js_err)?
                .dyn_into()
                .map_err(js_err)?;
            let buf = JsFuture::from(p).await.map_err(js_err)?;
            Ok(Uint8Array::new(&buf).to_vec())
        }
    }

    fn err(e: anyhow::Error) -> JsValue {
        JsValue::from_str(&format!("{e:#}"))
    }

    /// The raw key of a tuple (JSON array of values), hex: a `rangePage`
    /// starting point (e.g. past a prefix's NULLs).
    #[wasm_bindgen(js_name = keyHex)]
    pub fn key_hex(values: String) -> Result<String, JsValue> {
        let k: Vec<Val> = serde_json::from_str(&values).map_err(|e| err(e.into()))?;
        let refs: Vec<ValueRef> = k.iter().map(|v| v.as_ref()).collect();
        Ok(hex::encode(strata_core::keys::tuple(&refs)))
    }

    /// A v2 index open in the browser. Every method returns a Promise.
    #[wasm_bindgen]
    pub struct V2Reader {
        inner: Rc<WebReader<JsFetch>>,
    }

    #[wasm_bindgen]
    impl V2Reader {
        /// `base`: the published tree's root (`https://…/v3`); `index`: a published index name.
        pub fn open(base: String, index: String) -> Promise {
            future_to_promise(async move {
                let r = WebReader::open(JsFetch { base }, &index)
                    .await
                    .map_err(err)?;
                Ok(V2Reader { inner: Rc::new(r) }.into())
            })
        }

        /// `query`: JSON of a `SearchQuery` (`text`, `mode`, `terms`, `years`, `limit`, `facets`).
        /// Resolves to the result as JSON.
        pub fn search(&self, query: String) -> Promise {
            let r = self.inner.clone();
            future_to_promise(async move {
                let q: SearchQuery = serde_json::from_str(&query).map_err(|e| err(e.into()))?;
                let res = r.search(&q).await.map_err(err)?;
                Ok(serde_json::to_string(&res)
                    .map_err(|e| err(e.into()))?
                    .into())
            })
        }

        /// A table row by primary key (`key`: JSON array of values); JSON or `null`.
        pub fn get(&self, table: String, key: String) -> Promise {
            let r = self.inner.clone();
            future_to_promise(async move {
                let k: Vec<Val> = serde_json::from_str(&key).map_err(|e| err(e.into()))?;
                let res = r.get(&table, &k).await.map_err(err)?;
                Ok(serde_json::to_string(&res)
                    .map_err(|e| err(e.into()))?
                    .into())
            })
        }

        /// Rows under a key prefix (`prefix`: JSON array; the last value may
        /// be a text prefix with `partial`); JSON array.
        pub fn range(
            &self,
            map: String,
            prefix: String,
            partial: bool,
            limit: Option<u32>,
        ) -> Promise {
            let r = self.inner.clone();
            future_to_promise(async move {
                let k: Vec<Val> = serde_json::from_str(&prefix).map_err(|e| err(e.into()))?;
                let res = r
                    .range(&map, &k, partial, limit.map(|l| l as u64))
                    .await
                    .map_err(err)?;
                Ok(serde_json::to_string(&res)
                    .map_err(|e| err(e.into()))?
                    .into())
            })
        }

        /// A page of rows under a key prefix starting at a cursor: `from` is
        /// the previous page's `next` (hex), or `null` for the first page.
        /// Resolves to JSON `{"rows": [...], "next": hex | null}`.
        #[wasm_bindgen(js_name = rangePage)]
        pub fn range_page(
            &self,
            map: String,
            prefix: String,
            partial: bool,
            from: Option<String>,
            limit: u32,
        ) -> Promise {
            let r = self.inner.clone();
            future_to_promise(async move {
                let k: Vec<Val> = serde_json::from_str(&prefix).map_err(|e| err(e.into()))?;
                let from = match from {
                    Some(h) => Some(hex::decode(h).map_err(|e| err(e.into()))?),
                    None => None,
                };
                let (rows, next) = r
                    .range_from(&map, &k, partial, from.as_deref(), Some(limit as u64))
                    .await
                    .map_err(err)?;
                let v = serde_json::json!({"rows": rows, "next": next.map(hex::encode)});
                Ok(v.to_string().into())
            })
        }

        /// Rows under `prefix` (JSON array, may be empty) from the first key
        /// at or after the tuple `start` (JSON array of values, which should
        /// begin with the prefix's): a seek, e.g. to a random point.
        pub fn seek(&self, map: String, prefix: String, start: String, limit: u32) -> Promise {
            let r = self.inner.clone();
            future_to_promise(async move {
                let p: Vec<Val> = serde_json::from_str(&prefix).map_err(|e| err(e.into()))?;
                let k: Vec<Val> = serde_json::from_str(&start).map_err(|e| err(e.into()))?;
                let refs: Vec<ValueRef> = k.iter().map(|v| v.as_ref()).collect();
                let from = strata_core::keys::tuple(&refs);
                let (rows, _) = r
                    .range_from(&map, &p, false, Some(&from), Some(limit as u64))
                    .await
                    .map_err(err)?;
                Ok(serde_json::to_string(&rows)
                    .map_err(|e| err(e.into()))?
                    .into())
            })
        }

        /// Resolves to true when a newer version is published (open it again to read it).
        #[wasm_bindgen(js_name = checkUpdate)]
        pub fn check_update(&self) -> Promise {
            let r = self.inner.clone();
            future_to_promise(async move { Ok(r.check_update().await.map_err(err)?.into()) })
        }

        /// Passes, requests and bytes so far (JSON).
        pub fn stats(&self) -> String {
            serde_json::to_string(&self.inner.stats()).unwrap_or_default()
        }

        /// The open version: manifest, profile, `audio_base`, attribution,
        /// record count and each store map's row count (JSON).
        pub fn info(&self) -> String {
            let v = self.inner.version();
            let counts: BTreeMap<&String, u64> =
                v.store.maps.iter().map(|(n, m)| (n, m.count)).collect();
            serde_json::json!({
                "manifest": self.inner.manifest,
                "profile": v.profile,
                "audio_base": v.audio_base,
                "attribution": v.attribution,
                "records": v.records,
                "counts": counts,
            })
            .to_string()
        }

        /// The open version's seq.
        pub fn seq(&self) -> f64 {
            self.inner.manifest.seq as f64
        }
    }
}
