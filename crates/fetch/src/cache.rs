//! The content-addressed chunk cache (section 5.2): verified chunks on disk
//! named by sha256 and shared by every catalog version and profile, a small
//! in-memory LRU, a disk cap with least-recently-used eviction that never
//! drops pinned chunks, and single-flight fetching.

use crate::{ChunkSource, RemoteError};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
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

type Slot = Arc<(Mutex<Option<Result<Arc<[u8]>, RemoteError>>>, Condvar)>;

#[derive(Default)]
struct State {
    tick: u64,
    mem: HashMap<String, (Arc<[u8]>, u64)>,
    mem_bytes: u64,
    disk: HashMap<String, (u64, u64)>,
    disk_bytes: u64,
    pins: HashMap<String, u32>,
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
    mem_cap: u64,
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
            st.tick += 1;
            st.disk.insert(name, (len, st.tick));
            st.disk_bytes += len;
        }
        st.stats.bytes_on_disk = st.disk_bytes;
        Ok(Arc::new(Self {
            dir: dir.to_path_buf(),
            fsync: false,
            disk_cap: Mutex::new(disk_cap),
            mem_cap,
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
        st.mem.clear();
        st.mem_bytes = 0;
    }

    /// Whether the object is here, in memory or on disk. A file another
    /// cache on the same folder wrote since this one opened counts (and is
    /// adopted).
    pub fn contains(&self, sha256: &str) -> bool {
        {
            let st = self.st.lock().unwrap();
            if st.mem.contains_key(sha256) || st.disk.contains_key(sha256) {
                return true;
            }
        }
        self.adopt(sha256)
    }

    /// Whether the object is on disk here (memory alone doesn't count:
    /// what's kept offline must survive a restart).
    pub fn on_disk(&self, sha256: &str) -> bool {
        self.st.lock().unwrap().disk.contains_key(sha256) || self.adopt(sha256)
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
        Ok(self.load(sha256, size, source)?.1)
    }

    /// Index an entry written to the folder by another cache (another
    /// process, or a second cache opened on it). It's verified when read.
    fn adopt(&self, sha256: &str) -> bool {
        if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return false;
        }
        let Ok(m) = std::fs::metadata(self.path(sha256)) else {
            return false;
        };
        let mut st = self.st.lock().unwrap();
        st.tick += 1;
        let t = st.tick;
        if st.disk.insert(sha256.to_string(), (m.len(), t)).is_none() {
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
        *self
            .st
            .lock()
            .unwrap()
            .pins
            .entry(sha256.to_string())
            .or_default() += 1;
    }

    pub fn unpin(&self, sha256: &str) {
        let mut st = self.st.lock().unwrap();
        if let Some(n) = st.pins.get_mut(sha256) {
            *n -= 1;
            if *n == 0 {
                st.pins.remove(sha256);
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
        let result = self.load(sha256, size, source).map(|r| r.0);
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
    ) -> Result<(Arc<[u8]>, u64), RemoteError> {
        let on_disk = self.st.lock().unwrap().disk.contains_key(sha256) || self.adopt(sha256);
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
                    if let Some(e) = st.disk.get_mut(sha256) {
                        e.1 = t;
                    }
                    return Ok((d.into(), 0));
                }
            }
            let mut st = self.st.lock().unwrap();
            if let Some((len, _)) = st.disk.remove(sha256) {
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
        Ok((d.into(), stored.len() as u64))
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
        let mut st = self.st.lock().unwrap();
        st.tick += 1;
        let t = st.tick;
        if st
            .disk
            .insert(sha256.to_string(), (d.len() as u64, t))
            .is_none()
        {
            st.disk_bytes += d.len() as u64;
        }
        self.evict(&mut st);
        Ok(())
    }

    fn evict_mem(&self, st: &mut State) {
        while st.mem_bytes > self.mem_cap && !st.mem.is_empty() {
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
            let mut order: Vec<(u64, String, u64)> = st
                .disk
                .iter()
                .filter(|(k, _)| !st.pins.contains_key(*k))
                .map(|(k, (len, t))| (*t, k.clone(), *len))
                .collect();
            order.sort();
            for (_, k, len) in order {
                if st.disk_bytes <= cap {
                    break;
                }
                let _ = std::fs::remove_file(self.path(&k));
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
}
