//! The content-addressed chunk cache (section 5.2): verified chunks on disk
//! named by sha256 and shared by every catalog version and profile, a small
//! in-memory LRU, a disk cap with least-recently-used eviction that never
//! drops pinned chunks, and single-flight fetching.

use crate::{ChunkSource, RemoteError};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub mem_hits: u64,
    pub disk_hits: u64,
    pub misses: u64,
    pub bytes_fetched: u64,
    pub fetch_ms: u64,
    pub bytes_on_disk: u64,
    pub integrity_failures: u64,
}

/// An object's name as its 32 bytes: the key of the maps that hold an
/// entry per object on disk or pinned (a whole kept index is tens of
/// thousands of objects; 64-character strings for each cost several times
/// more).
type Id = [u8; 32];

/// The id of a name (64 lowercase hex characters, as objects are named);
/// `None` for anything else, which no object can be.
fn id(sha256: &str) -> Option<Id> {
    let b = sha256.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = nib(b[2 * i])? << 4 | nib(b[2 * i + 1])?;
    }
    Some(out)
}

fn hex_of(id: &Id) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in id {
        s.push(D[(b >> 4) as usize] as char);
        s.push(D[(b & 15) as usize] as char);
    }
    s
}

type Slot = Arc<(Mutex<Option<Result<Arc<[u8]>, RemoteError>>>, Condvar)>;

#[derive(Default)]
struct State {
    tick: u64,
    mem: HashMap<String, (Arc<[u8]>, u64)>,
    mem_bytes: u64,
    disk: HashMap<Id, (u64, u64)>,
    disk_bytes: u64,
    pins: HashMap<Id, u32>,
    inflight: HashMap<String, Slot>,
    stats: CacheStats,
}

pub struct ChunkCache {
    dir: PathBuf,
    /// fsync each chunk before its rename. Off by default: disk hits are
    /// re-verified against their sha256, so a write torn by a crash is
    /// detected and fetched again; fsync per chunk cost about 3 ms each
    /// (measured: 1.2 s to open a 371-chunk hot cache on loopback).
    fsync: bool,
    disk_cap: Mutex<u64>,
    /// Decoded objects kept in memory, at most (bytes; 0 keeps none).
    mem_cap: AtomicU64,
    st: Mutex<State>,
}

impl ChunkCache {
    /// Open (creating) a cache directory. Existing entries are indexed from
    /// the directory; their last use is their modification time order.
    pub fn open(dir: &Path, disk_cap: u64, mem_cap: u64) -> Result<Arc<Self>, RemoteError> {
        let io = |e: std::io::Error| RemoteError::Io(e.to_string());
        std::fs::create_dir_all(dir).map_err(io)?;
        let mut found = vec![];
        for sub in std::fs::read_dir(dir).map_err(io)? {
            let sub = sub.map_err(io)?.path();
            if !sub.is_dir() || sub.file_name().is_some_and(|n| n.len() != 2) {
                continue;
            }
            for e in std::fs::read_dir(&sub).map_err(io)? {
                let e = e.map_err(io)?;
                let name = e.file_name().to_string_lossy().to_string();
                if name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
                    let m = e.metadata().map_err(io)?;
                    found.push((m.modified().ok(), name, m.len()));
                } else if name.ends_with(".tmp") {
                    // An interrupted write: never a valid entry.
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        found.sort();
        let mut st = State::default();
        for (_, name, len) in found {
            let Some(k) = id(&name) else { continue };
            st.tick += 1;
            st.disk.insert(k, (len, st.tick));
            st.disk_bytes += len;
        }
        st.stats.bytes_on_disk = st.disk_bytes;
        Ok(Arc::new(Self {
            dir: dir.to_path_buf(),
            fsync: false,
            disk_cap: Mutex::new(disk_cap),
            mem_cap: AtomicU64::new(mem_cap),
            st: Mutex::new(st),
        }))
    }

    /// Like [`ChunkCache::open`], with an fsync before every rename (for
    /// storage where a torn write could survive as a full-length file).
    pub fn open_durable(dir: &Path, disk_cap: u64, mem_cap: u64) -> Result<Arc<Self>, RemoteError> {
        let c = Self::open(dir, disk_cap, mem_cap)?;
        let mut c = Arc::try_unwrap(c).ok().expect("fresh cache");
        c.fsync = true;
        Ok(Arc::new(c))
    }

    pub fn stats(&self) -> CacheStats {
        self.st.lock().unwrap().stats.clone()
    }

    pub fn reset_stats(&self) {
        let mut st = self.st.lock().unwrap();
        let on_disk = st.disk_bytes;
        st.stats = CacheStats {
            bytes_on_disk: on_disk,
            ..Default::default()
        };
    }

    pub fn set_disk_cap(&self, bytes: u64) {
        *self.disk_cap.lock().unwrap() = bytes;
        let mut st = self.st.lock().unwrap();
        self.evict(&mut st);
    }

    /// Drop the in-memory copies (the disk stays).
    pub fn clear_memory(&self) {
        let mut st = self.st.lock().unwrap();
        st.mem = HashMap::new();
        st.mem_bytes = 0;
    }

    /// Change the in-memory cap (bytes of decoded objects; 0 keeps none),
    /// evicting the least recently used down to it.
    pub fn set_memory_cap(&self, bytes: u64) {
        self.mem_cap.store(bytes, Ordering::Relaxed);
        let mut st = self.st.lock().unwrap();
        self.evict_mem(&mut st);
    }

    /// The in-memory cap, and the decoded bytes held in memory now.
    pub fn memory(&self) -> (u64, u64) {
        let cap = self.mem_cap.load(Ordering::Relaxed);
        (cap, self.st.lock().unwrap().mem_bytes)
    }

    /// Whether the object is here, in memory or on disk. A file another
    /// cache on the same folder wrote since this one opened counts (and is
    /// adopted).
    pub fn contains(&self, sha256: &str) -> bool {
        {
            let st = self.st.lock().unwrap();
            if st.mem.contains_key(sha256) || id(sha256).is_some_and(|k| st.disk.contains_key(&k)) {
                return true;
            }
        }
        self.adopt(sha256)
    }

    /// Whether the object is on disk here (memory alone doesn't count:
    /// what's kept offline must survive a restart).
    pub fn on_disk(&self, sha256: &str) -> bool {
        self.indexed(sha256) || self.adopt(sha256)
    }

    /// Whether the object is in this cache's index of the disk.
    fn indexed(&self, sha256: &str) -> bool {
        id(sha256).is_some_and(|k| self.st.lock().unwrap().disk.contains_key(&k))
    }

    /// The object's stored size on disk here, if it is.
    pub fn disk_size(&self, sha256: &str) -> Option<u64> {
        if !self.on_disk(sha256) {
            return None;
        }
        let k = id(sha256)?;
        self.st.lock().unwrap().disk.get(&k).map(|e| e.0)
    }

    /// Make sure the object is on disk, fetching it if it isn't (even when
    /// a copy is in memory). Returns the stored bytes fetched (0 if it was
    /// already there).
    pub fn fetch_to_disk(
        &self,
        sha256: &str,
        size: u64,
        source: &dyn ChunkSource,
    ) -> Result<u64, RemoteError> {
        if self.on_disk(sha256) {
            return Ok(0);
        }
        self.fetch_verified(sha256, size, source)
    }

    /// Fetch an object, verify it and write it to disk, without keeping
    /// (or allocating for each object) its decoded bytes; the stored bytes
    /// fetched.
    fn fetch_verified(
        &self,
        sha256: &str,
        size: u64,
        source: &dyn ChunkSource,
    ) -> Result<u64, RemoteError> {
        let t0 = Instant::now();
        let stored = source.fetch(sha256, size)?;
        let ms = t0.elapsed().as_millis() as u64;
        {
            let mut st = self.st.lock().unwrap();
            st.stats.misses += 1;
            st.stats.bytes_fetched += stored.len() as u64;
            st.stats.fetch_ms += ms;
        }
        if strata_object::verify(&stored, sha256, size).is_err() {
            self.st.lock().unwrap().stats.integrity_failures += 1;
            return Err(RemoteError::Integrity {
                chunk: sha256.into(),
            });
        }
        self.store(sha256, &stored)?;
        Ok(stored.len() as u64)
    }

    /// Index an entry written to the folder by another cache (another
    /// process, or a second cache opened on it). It's verified when read.
    fn adopt(&self, sha256: &str) -> bool {
        let Some(k) = id(sha256) else {
            return false;
        };
        let Ok(m) = std::fs::metadata(self.path(sha256)) else {
            return false;
        };
        let mut st = self.st.lock().unwrap();
        st.tick += 1;
        let t = st.tick;
        if st.disk.insert(k, (m.len(), t)).is_none() {
            st.disk_bytes += m.len();
        }
        st.stats.bytes_on_disk = st.disk_bytes;
        true
    }

    /// An object's stored form (as published: encoding byte and payload),
    /// verified, if it's on disk here; for serving the cache to other
    /// devices. Nothing is fetched.
    pub fn stored(&self, sha256: &str, size: u64) -> Option<Vec<u8>> {
        if !self.contains(sha256) {
            return None;
        }
        let b = std::fs::read(self.path(sha256)).ok()?;
        strata_object::decode_verified(&b, sha256, size).ok()?;
        Some(b)
    }

    fn path(&self, sha256: &str) -> PathBuf {
        self.dir.join(&sha256[..2]).join(sha256)
    }

    pub fn pin(&self, sha256: &str) {
        // Anything that isn't an object's name can never be stored here.
        let Some(k) = id(sha256) else { return };
        *self.st.lock().unwrap().pins.entry(k).or_default() += 1;
    }

    pub fn unpin(&self, sha256: &str) {
        let mut st = self.st.lock().unwrap();
        if let Some(k) = id(sha256) {
            if let Some(n) = st.pins.get_mut(&k) {
                *n -= 1;
                if *n == 0 {
                    st.pins.remove(&k);
                }
            }
        }
        self.evict(&mut st);
    }

    /// An object's decoded, verified bytes, from memory, disk, or `source`
    /// (one fetch per object however many threads ask at once). `source`
    /// returns the stored form (`strata_object`), which is what's kept on
    /// disk. `size` is the decoded size; 0 means "unknown" (tree nodes).
    pub fn get(
        &self,
        sha256: &str,
        size: u64,
        source: &dyn ChunkSource,
    ) -> Result<Arc<[u8]>, RemoteError> {
        let slot = {
            let mut st = self.st.lock().unwrap();
            st.tick += 1;
            let t = st.tick;
            if let Some((d, used)) = st.mem.get_mut(sha256) {
                *used = t;
                let d = d.clone();
                st.stats.mem_hits += 1;
                return Ok(d);
            }
            if let Some(s) = st.inflight.get(sha256) {
                let s = s.clone();
                drop(st);
                return Self::wait(&s);
            }
            let s: Slot = Arc::new((Mutex::new(None), Condvar::new()));
            st.inflight.insert(sha256.to_string(), s.clone());
            s
        };
        let result = self
            .load(sha256, size, source)
            .map(|r| Arc::<[u8]>::from(r.0));
        let mut st = self.st.lock().unwrap();
        st.inflight.remove(sha256);
        if let Ok(d) = &result {
            st.tick += 1;
            let t = st.tick;
            st.mem_bytes += d.len() as u64;
            st.mem.insert(sha256.to_string(), (d.clone(), t));
            self.evict_mem(&mut st);
        }
        drop(st);
        *slot.0.lock().unwrap() = Some(result.clone());
        slot.1.notify_all();
        result
    }

    fn wait(s: &Slot) -> Result<Arc<[u8]>, RemoteError> {
        let mut g = s.0.lock().unwrap();
        while g.is_none() {
            g = s.1.wait(g).unwrap();
        }
        g.clone().unwrap()
    }

    /// The object's decoded bytes, and the stored bytes fetched for it (0
    /// when read from disk).
    fn load(
        &self,
        sha256: &str,
        size: u64,
        source: &dyn ChunkSource,
    ) -> Result<(Vec<u8>, u64), RemoteError> {
        let on_disk = self.indexed(sha256) || self.adopt(sha256);
        if on_disk {
            if let Ok(stored) = std::fs::read(self.path(sha256)) {
                // Entries were verified before they were written; decode and
                // check again (cheap next to a fetch; catches disk corruption
                // and entries of an older object format).
                if let Ok(d) = strata_object::decode_verified(&stored, sha256, size) {
                    let mut st = self.st.lock().unwrap();
                    st.stats.disk_hits += 1;
                    st.tick += 1;
                    let t = st.tick;
                    if let Some(e) = id(sha256).and_then(|k| st.disk.get_mut(&k)) {
                        e.1 = t;
                    }
                    return Ok((d, 0));
                }
            }
            let mut st = self.st.lock().unwrap();
            if let Some((len, _)) = id(sha256).and_then(|k| st.disk.remove(&k)) {
                st.disk_bytes -= len;
            }
            let _ = std::fs::remove_file(self.path(sha256));
        }
        let t0 = Instant::now();
        let stored = source.fetch(sha256, size)?;
        let ms = t0.elapsed().as_millis() as u64;
        {
            let mut st = self.st.lock().unwrap();
            st.stats.misses += 1;
            st.stats.bytes_fetched += stored.len() as u64;
            st.stats.fetch_ms += ms;
        }
        let d = match strata_object::decode_verified(&stored, sha256, size) {
            Ok(d) => d,
            Err(_) => {
                self.st.lock().unwrap().stats.integrity_failures += 1;
                return Err(RemoteError::Integrity {
                    chunk: sha256.into(),
                });
            }
        };
        // On disk in stored form (compact); in memory decoded.
        self.store(sha256, &stored)?;
        Ok((d, stored.len() as u64))
    }

    /// Write verified bytes: temporary name, fsync, rename.
    fn store(&self, sha256: &str, d: &[u8]) -> Result<(), RemoteError> {
        let io = |e: std::io::Error| RemoteError::Io(e.to_string());
        let p = self.path(sha256);
        std::fs::create_dir_all(p.parent().unwrap()).map_err(io)?;
        let tmp = p.with_extension(format!("{}.tmp", std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp).map_err(io)?;
            f.write_all(d).map_err(io)?;
            if self.fsync {
                f.sync_all().map_err(io)?;
            }
        }
        std::fs::rename(&tmp, &p).map_err(io)?;
        // Only verified objects are stored, so the name is an object's.
        let Some(k) = id(sha256) else { return Ok(()) };
        let mut st = self.st.lock().unwrap();
        st.tick += 1;
        let t = st.tick;
        if st.disk.insert(k, (d.len() as u64, t)).is_none() {
            st.disk_bytes += d.len() as u64;
        }
        self.evict(&mut st);
        Ok(())
    }

    fn evict_mem(&self, st: &mut State) {
        let cap = self.mem_cap.load(Ordering::Relaxed);
        if cap == 0 {
            st.mem = HashMap::new();
            st.mem_bytes = 0;
            return;
        }
        while st.mem_bytes > cap && !st.mem.is_empty() {
            let oldest = st
                .mem
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| k.clone())
                .unwrap();
            if let Some((d, _)) = st.mem.remove(&oldest) {
                st.mem_bytes -= d.len() as u64;
            }
        }
    }

    fn evict(&self, st: &mut State) {
        let cap = *self.disk_cap.lock().unwrap();
        if st.disk_bytes > cap {
            let mut order: Vec<(u64, Id, u64)> = st
                .disk
                .iter()
                .filter(|(k, _)| !st.pins.contains_key(*k))
                .map(|(k, (len, t))| (*t, *k, *len))
                .collect();
            order.sort();
            for (_, k, len) in order {
                if st.disk_bytes <= cap {
                    break;
                }
                let _ = std::fs::remove_file(self.path(&hex_of(&k)));
                st.disk.remove(&k);
                st.disk_bytes -= len;
            }
        }
        st.stats.bytes_on_disk = st.disk_bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Count(std::sync::atomic::AtomicUsize, Vec<u8>);
    impl ChunkSource for Count {
        fn fetch(&self, _: &str, _: u64) -> Result<Vec<u8>, RemoteError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.1.clone())
        }
        fn fetch_path(&self, p: &str) -> Result<Vec<u8>, RemoteError> {
            Err(RemoteError::Io(p.into()))
        }
    }

    #[test]
    fn a_second_cache_on_one_folder_sees_new_objects() {
        let dir = tempfile::tempdir().unwrap();
        let a = ChunkCache::open(dir.path(), 1 << 20, 1 << 20).unwrap();
        let b = ChunkCache::open(dir.path(), 1 << 20, 1 << 20).unwrap();
        let raw = b"some object".to_vec();
        let name = strata_object::name(&raw);
        let src = Count(Default::default(), strata_object::encode(&raw, strata_object::Codec::Zstd(3)));
        assert!(!b.contains(&name));
        assert_eq!(&*a.get(&name, raw.len() as u64, &src).unwrap(), &raw[..]);
        // b opened before a fetched it: it finds the file instead of fetching.
        assert!(b.contains(&name));
        assert_eq!(&*b.get(&name, raw.len() as u64, &src).unwrap(), &raw[..]);
        assert_eq!(src.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        let stored = b.stored(&name, raw.len() as u64).unwrap();
        assert_eq!(strata_object::decode(&stored, 1 << 20).unwrap(), raw);
        assert!(b.stored(&strata_object::name(b"absent"), 0).is_none());
        // Bytes fetched are the object's own stored size, once.
        let c = ChunkCache::open(&dir.path().join("c"), 1 << 20, 1 << 20).unwrap();
        assert_eq!(c.fetch_to_disk(&name, raw.len() as u64, &src).unwrap(), src.1.len() as u64);
        assert_eq!(c.fetch_to_disk(&name, raw.len() as u64, &src).unwrap(), 0);
    }

    #[test]
    fn ids_round_trip_and_only_object_names_have_one() {
        let n = strata_object::name(b"x");
        assert_eq!(hex_of(&id(&n).unwrap()), n);
        assert!(id(&n.to_uppercase()).is_none());
        assert!(id(&n[1..]).is_none());
        assert!(id("absent").is_none());
    }

    #[test]
    fn the_memory_cap_holds_the_most_recent_and_can_change() {
        let dir = tempfile::tempdir().unwrap();
        let c = ChunkCache::open(dir.path(), 1 << 30, 2500).unwrap();
        let objs: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 1000]).collect();
        let get = |o: &Vec<u8>| {
            let src = Count(
                Default::default(),
                strata_object::encode(o, strata_object::Codec::Raw),
            );
            c.get(&strata_object::name(o), o.len() as u64, &src)
                .unwrap();
        };
        for o in &objs {
            get(o);
            assert!(c.memory().1 <= 2500);
        }
        assert_eq!(c.memory(), (2500, 2000));
        let before = c.stats();
        get(&objs[4]);
        assert_eq!(c.stats().mem_hits, before.mem_hits + 1);
        get(&objs[0]);
        assert_eq!(c.stats().disk_hits, before.disk_hits + 1);
        c.set_memory_cap(1000);
        assert_eq!(c.memory(), (1000, 1000));
        c.set_memory_cap(0);
        assert_eq!(c.memory(), (0, 0));
        get(&objs[1]);
        assert_eq!(c.memory(), (0, 0));
        c.set_memory_cap(5000);
        get(&objs[2]);
        c.clear_memory();
        assert_eq!(c.memory(), (5000, 0));
    }

    #[test]
    fn objects_kept_whole_are_verified_without_their_decoded_bytes_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let c = ChunkCache::open(dir.path(), 1 << 30, 1 << 20).unwrap();
        let raw = vec![5u8; 300_000];
        let name = strata_object::name(&raw);
        let good = Count(
            Default::default(),
            strata_object::encode(&raw, strata_object::Codec::Zstd(3)),
        );
        assert!(c.fetch_to_disk(&name, raw.len() as u64, &good).unwrap() > 0);
        assert!(c.on_disk(&name));
        assert_eq!(c.memory().1, 0);
        // A wrong body is refused and not written.
        let other = strata_object::name(b"other");
        assert!(matches!(
            c.fetch_to_disk(&other, raw.len() as u64, &good),
            Err(RemoteError::Integrity { .. })
        ));
        assert!(!c.on_disk(&other));
        assert_eq!(c.stats().integrity_failures, 1);
    }
}
