//! Candidate B: prolly trees (content-addressed, history-independent sorted
//! maps). Prototype quality: a builder and a counting reader, no SQL.
//!
//! - A map is a sorted list of `(key, value)` byte pairs cut into leaf nodes.
//!   An entry ends a node when the first 4 bytes of sha256(key), read as a
//!   u32, fall below `2^32 × entry_bytes / target_bytes`. So nodes average
//!   about `target_bytes`, and where a node ends depends only on that entry,
//!   never on insertion order or position: the same data gives the same tree.
//!   A hard cap at 4 × target bounds the worst case.
//! - Internal nodes list `(first key of child, child sha256, entries under
//!   it)` and are cut the same way, up to a single root.
//! - Each node is one object (`strata_object`): named by the sha256 of its
//!   encoded bytes, stored zstd-compressed, verified by its name after
//!   decoding. The compressor never enters the name.
//! - Reading walks one level at a time. All the nodes a query needs at one
//!   level are known from the level above, so each level is one round trip
//!   (fetched in parallel), and a range scan or a batch of lookups costs
//!   `depth` round trips.

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub type Hash = [u8; 32];

fn hex(h: &Hash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

/// One node: level 0 is a leaf.
#[derive(Debug, Clone)]
pub struct Node {
    pub level: u8,
    pub keys: Vec<Vec<u8>>,
    /// Leaves: values. Internal: 32-byte child hash + 8-byte entry count.
    pub vals: Vec<Vec<u8>>,
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn get_varint(b: &[u8], pos: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let x = *b.get(*pos).context("truncated")?;
        *pos += 1;
        v |= ((x & 0x7f) as u64) << shift;
        if x & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
    }
}

impl Node {
    fn encode(&self) -> Vec<u8> {
        let mut raw = vec![self.level];
        put_varint(&mut raw, self.keys.len() as u64);
        // Keys are prefix-compressed against the previous key.
        let mut prev: &[u8] = &[];
        for (k, v) in self.keys.iter().zip(&self.vals) {
            let common = prev
                .iter()
                .zip(k.iter())
                .take_while(|(a, b)| a == b)
                .count();
            put_varint(&mut raw, common as u64);
            put_varint(&mut raw, (k.len() - common) as u64);
            raw.extend_from_slice(&k[common..]);
            put_varint(&mut raw, v.len() as u64);
            raw.extend_from_slice(v);
            prev = k;
        }
        raw
    }

    /// A node from its encoded (decoded-object) bytes.
    pub fn decode(raw: &[u8]) -> Result<Self> {
        let level = raw[0];
        let mut pos = 1;
        let n = get_varint(&raw, &mut pos)? as usize;
        let (mut keys, mut vals) = (Vec::with_capacity(n), Vec::with_capacity(n));
        let mut prev: Vec<u8> = vec![];
        for _ in 0..n {
            let common = get_varint(&raw, &mut pos)? as usize;
            let rest = get_varint(&raw, &mut pos)? as usize;
            let mut k = prev[..common].to_vec();
            k.extend_from_slice(&raw[pos..pos + rest]);
            pos += rest;
            let vl = get_varint(&raw, &mut pos)? as usize;
            vals.push(raw[pos..pos + vl].to_vec());
            pos += vl;
            prev = k.clone();
            keys.push(k);
        }
        Ok(Node { level, keys, vals })
    }

    fn child(&self, i: usize) -> (Hash, u64) {
        let v = &self.vals[i];
        (
            v[..32].try_into().unwrap(),
            u64::from_le_bytes(v[32..40].try_into().unwrap()),
        )
    }
}

/// How nodes are stored (free to change: it never enters a node's name).
pub const NODE_CODEC: strata_object::Codec = strata_object::Codec::Zstd(3);

/// Where nodes go: files (stored form) named by hash in a directory, or
/// memory.
enum Sink {
    Dir(PathBuf),
    Mem(BTreeMap<Hash, Vec<u8>>),
}

/// Where nodes are written; returns the set of node hashes it wrote.
pub struct Writer {
    sink: Sink,
    pub written: HashSet<Hash>,
    pub bytes: u64,
}

impl Writer {
    pub fn new(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            sink: Sink::Dir(dir.to_path_buf()),
            written: HashSet::new(),
            bytes: 0,
        })
    }
    /// Keep nodes in memory ([`Writer::take_nodes`]).
    pub fn memory() -> Self {
        Self {
            sink: Sink::Mem(BTreeMap::new()),
            written: HashSet::new(),
            bytes: 0,
        }
    }
    /// The nodes a memory writer holds, by hash.
    pub fn take_nodes(&mut self) -> BTreeMap<Hash, Vec<u8>> {
        match &mut self.sink {
            Sink::Mem(m) => std::mem::take(m),
            Sink::Dir(_) => BTreeMap::new(),
        }
    }
    fn put(&mut self, n: &Node) -> Result<Hash> {
        let raw = n.encode();
        let h: Hash = Sha256::digest(&raw).into();
        if self.written.insert(h) {
            let b = strata_object::encode(&raw, NODE_CODEC);
            anyhow::ensure!(
                self.written.len() < 2_000_000,
                "tree writer passed 2M nodes: aborting"
            );
            match &mut self.sink {
                Sink::Dir(dir) => {
                    let p = dir.join(hex(&h));
                    if !p.exists() {
                        std::fs::write(&p, &b)?;
                        // Count only nodes that weren't published already.
                        self.bytes += b.len() as u64;
                    }
                }
                Sink::Mem(m) => {
                    self.bytes += b.len() as u64;
                    m.insert(h, b);
                }
            }
        }
        Ok(h)
    }
}

/// Upper levels repeat their children's first keys, so a level's boundary
/// hash is salted with the level: otherwise the same keys make the same
/// decisions at every level and the tree never converges.
fn boundary(key: &[u8], entry_bytes: usize, target: usize, level: u8) -> bool {
    let h = if level == 0 {
        Sha256::digest(key)
    } else {
        let mut x = Sha256::new();
        x.update([b'L', level]);
        x.update(key);
        x.finalize()
    };
    let x = u32::from_le_bytes(h[..4].try_into().unwrap()) as u64;
    x < ((1u64 << 32) * entry_bytes as u64 / target as u64).min(1u64 << 32)
}

#[derive(Debug, Clone, Default)]
pub struct Root {
    pub hash: Hash,
    pub depth: u8,
    pub count: u64,
    /// Nodes per level, leaves first (`levels.len() == depth`). Lets an
    /// in-place edit see a change of height without reading whole levels;
    /// empty when unknown (readers don't need it).
    pub levels: Vec<u64>,
}

type Entry = (Vec<u8>, Vec<u8>);

/// Cuts one level's entry stream into nodes (the only place nodes are cut,
/// so a full build and an in-place edit cut identically).
struct Chunker {
    level: u8,
    target: usize,
    node: Node,
    size: usize,
    under: u64,
    out: Vec<Entry>,
}

impl Chunker {
    fn new(level: u8, target: usize) -> Self {
        Self {
            level,
            target,
            node: Node {
                level,
                keys: vec![],
                vals: vec![],
            },
            size: 0,
            under: 0,
            out: vec![],
        }
    }
    /// True when this entry ended a node.
    fn push(&mut self, k: Vec<u8>, v: Vec<u8>, w: &mut Writer) -> Result<bool> {
        let eb = k.len() + v.len() + 4;
        self.under += if self.level == 0 {
            1
        } else {
            u64::from_le_bytes(v[32..40].try_into().unwrap())
        };
        self.size += eb;
        let cut = boundary(&k, eb, self.target, self.level) || self.size >= 4 * self.target;
        self.node.keys.push(k);
        self.node.vals.push(v);
        if cut {
            self.flush(w)?;
        }
        Ok(cut)
    }
    fn flush(&mut self, w: &mut Writer) -> Result<()> {
        if self.node.keys.is_empty() {
            return Ok(());
        }
        let first = self.node.keys[0].clone();
        let h = w.put(&self.node)?;
        let mut v = h.to_vec();
        v.extend_from_slice(&self.under.to_le_bytes());
        self.out.push((first, v));
        self.node.keys.clear();
        self.node.vals.clear();
        self.under = 0;
        self.size = 0;
        Ok(())
    }
    fn is_empty(&self) -> bool {
        self.node.keys.is_empty()
    }
}

fn chunk_level(items: Vec<Entry>, level: u8, target: usize, w: &mut Writer) -> Result<Vec<Entry>> {
    let mut c = Chunker::new(level, target);
    for (k, v) in items {
        c.push(k, v, w)?;
    }
    c.flush(w)?;
    Ok(c.out)
}

/// Finish a tree from the nodes just cut at `level` (`out`, from
/// `n_items` entries at that level); `levels` holds the node counts of the
/// levels below.
fn finish_from(
    mut out: Vec<Entry>,
    mut n_items: usize,
    mut level: u8,
    count: u64,
    target: usize,
    w: &mut Writer,
    mut levels: Vec<u64>,
) -> Result<Root> {
    loop {
        levels.push(out.len() as u64);
        if out.len() == 1 {
            let hash: Hash = out[0].1[..32].try_into().unwrap();
            return Ok(Root {
                hash,
                depth: level + 1,
                count,
                levels,
            });
        }
        if out.is_empty() {
            let h = w.put(&Node {
                level: 0,
                keys: vec![],
                vals: vec![],
            })?;
            return Ok(Root {
                hash: h,
                depth: 1,
                count: 0,
                levels: vec![1],
            });
        }
        // A level must shrink. If (rarely) it doesn't, its entries become a
        // single root node: still a function of the content alone.
        if out.len() >= n_items {
            let mut root = Node {
                level: level + 1,
                keys: vec![],
                vals: vec![],
            };
            for (k, v) in out {
                root.keys.push(k);
                root.vals.push(v);
            }
            let hash = w.put(&root)?;
            levels.push(1);
            return Ok(Root {
                hash,
                depth: level + 2,
                count,
                levels,
            });
        }
        anyhow::ensure!(level < 40, "tree deeper than 40 levels");
        level += 1;
        n_items = out.len();
        out = chunk_level(out, level, target, w)?;
    }
}

/// Build a tree from sorted, unique entries.
pub fn build(entries: Vec<Entry>, target: usize, w: &mut Writer) -> Result<Root> {
    for p in entries.windows(2) {
        if p[0].0 >= p[1].0 {
            bail!("entries not sorted and unique");
        }
    }
    let count = entries.len() as u64;
    let n = entries.len();
    let out = chunk_level(entries, 0, target, w)?;
    finish_from(out, n, 0, count, target, w, vec![])
}

/// What an in-place edit read and wrote.
#[derive(Debug, Default, Clone)]
pub struct ApplyStats {
    /// Round trips (batches of parallel fetches).
    pub rounds: u64,
    /// The edit read every internal node (no level counts, a depth-1 tree,
    /// or a change of height).
    pub read_all_internal: bool,
    pub internal_nodes_read: u64,
    pub leaves_read: u64,
    pub leaves_written: u64,
}

/// A position at one level: the path from the root to the parent of the
/// current node, each with the index of the child taken.
#[derive(Clone)]
struct Cursor {
    level: u8,
    stack: Vec<(Arc<Node>, usize)>,
}

impl Cursor {
    /// The node at `level` that holds `k` (the last whose first key <= k;
    /// the first node when k sorts before everything).
    fn seek(r: &Reader, root: &Arc<Node>, level: u8, k: &[u8]) -> Result<Self> {
        let mut stack = vec![];
        let mut n = root.clone();
        loop {
            let i = n
                .keys
                .partition_point(|x| x.as_slice() <= k)
                .saturating_sub(1);
            let down = n.level > level + 1;
            let child = n.child(i).0;
            stack.push((n, i));
            if !down {
                return Ok(Self { level, stack });
            }
            n = r.fetch(&[child])?.remove(0);
        }
    }
    fn current(&self) -> (Vec<u8>, Hash) {
        let (n, i) = self.stack.last().unwrap();
        (n.keys[*i].clone(), n.child(*i).0)
    }
    /// Move to the next node at this level; false at the end.
    fn advance(&mut self, r: &Reader) -> Result<bool> {
        while let Some((n, i)) = self.stack.last() {
            if i + 1 < n.keys.len() {
                break;
            }
            self.stack.pop();
        }
        let Some(top) = self.stack.last_mut() else {
            return Ok(false);
        };
        top.1 += 1;
        while self.stack.last().unwrap().0.level > self.level + 1 {
            let (n, i) = self.stack.last().unwrap();
            let child = r.fetch(&[n.child(*i).0])?.remove(0);
            self.stack.push((child, 0));
        }
        Ok(true)
    }
}

type Edits = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// Re-cut the nodes of one level around `edits` (to that level's entries).
/// Returns the edits to the level above (old nodes' first keys deleted,
/// new nodes' entries set), nodes removed and added, and the change in
/// entries at this level.
fn recut_level(
    r: &Reader,
    root: &Arc<Node>,
    level: u8,
    edits: &Edits,
    target: usize,
    w: &mut Writer,
    st: &mut ApplyStats,
) -> Result<(Edits, u64, u64, i64)> {
    let edits: Vec<(&Vec<u8>, &Option<Vec<u8>>)> = edits.iter().collect();
    let mut up: Edits = BTreeMap::new();
    let (mut removed, mut added, mut dcount) = (0u64, 0u64, 0i64);
    let mut e = 0usize;
    while e < edits.len() {
        let mut cur = Cursor::seek(r, root, level, edits[e].0)?;
        let mut c = Chunker::new(level, target);
        loop {
            let (first, h) = cur.current();
            let node = r.fetch(&[h])?.remove(0);
            if level == 0 {
                st.leaves_read += 1;
            }
            let mut next = cur.clone();
            let has_next = next.advance(r)?;
            let next_first = has_next.then(|| next.current().0);
            let mut j = 0;
            loop {
                let ek = edits
                    .get(e)
                    .map(|(k, _)| k.as_slice())
                    .filter(|k| next_first.as_deref().map_or(true, |n| *k < n));
                let lk = node.keys.get(j).map(|k| k.as_slice());
                match (lk, ek) {
                    (None, None) => break,
                    (Some(l), Some(x)) if x <= l => {
                        match edits[e].1 {
                            Some(v) => {
                                c.push(edits[e].0.clone(), v.clone(), w)?;
                                if x < l {
                                    dcount += 1;
                                }
                            }
                            None if x == l => dcount -= 1,
                            None => {}
                        }
                        if x == l {
                            j += 1;
                        }
                        e += 1;
                    }
                    (None, Some(_)) => {
                        if let Some(v) = edits[e].1 {
                            c.push(edits[e].0.clone(), v.clone(), w)?;
                            dcount += 1;
                        }
                        e += 1;
                    }
                    (Some(_), _) => {
                        c.push(node.keys[j].clone(), node.vals[j].clone(), w)?;
                        j += 1;
                    }
                }
            }
            up.insert(first, None);
            removed += 1;
            // In step with the old cuts: the rest of the level is unchanged.
            if c.is_empty() {
                break;
            }
            if !has_next {
                c.flush(w)?;
                break;
            }
            cur = next;
        }
        if level == 0 {
            st.leaves_written += c.out.len() as u64;
        }
        added += c.out.len() as u64;
        for (k, v) in c.out.drain(..) {
            up.insert(k, Some(v));
        }
    }
    Ok((up, removed, added, dcount))
}

/// Apply sorted edits (`Some(value)` upsert, `None` delete) to a tree in
/// place. Only the paths to the edits are read: at each level the nodes
/// holding edits are re-cut from their start (a known cut state) until a
/// new cut lands on an old node's end, and the changed nodes become edits
/// to the level above. The root's level is finished by the same code as
/// [`build`], so the result is the tree a full build of the edited data
/// gives, node for node. A tree without level counts, of depth 1, or whose
/// height would shrink is edited by reading every internal node instead.
pub fn apply(
    r: &Reader,
    root: &Root,
    edits: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    target: usize,
    w: &mut Writer,
) -> Result<(Root, ApplyStats)> {
    let before = r.counts().fetches;
    let rounds_before = r.counts().rounds;
    let fallback = |w: &mut Writer, st0: ApplyStats| -> Result<(Root, ApplyStats)> {
        let (root, mut st) = apply_reading_all(r, root, edits, target, w)?;
        st.read_all_internal = true;
        st.leaves_read += st0.leaves_read;
        Ok((root, st))
    };
    if edits.is_empty() {
        return Ok((root.clone(), ApplyStats::default()));
    }
    let d = root.depth;
    if d < 2 || root.levels.len() != d as usize {
        return fallback(w, ApplyStats::default());
    }
    let mut st = ApplyStats::default();
    // Warm every edit's path a level at a time, each level's nodes fetched
    // in parallel (one round trip per level); the cursors then mostly hit
    // the cache, missing only for a sibling read to fall back into step.
    let keys: Vec<Vec<u8>> = edits.keys().cloned().collect();
    r.get_many(root, &keys)?;
    let top = r.fetch(&[root.hash])?.remove(0);
    let mut levels = root.levels.clone();
    let mut count = root.count as i64;
    let mut cur: Edits = edits.clone();
    for level in 0..=(d - 2) {
        let (up, removed, added, dcount) = recut_level(r, &top, level, &cur, target, w, &mut st)?;
        if level == 0 {
            count += dcount;
        }
        let l = level as usize;
        levels[l] = levels[l] + added - removed;
        let n_items = if level == 0 {
            count as u64
        } else {
            levels[l - 1]
        };
        if level < d - 2 {
            // Where a full build would stop or fall back below the old root.
            if levels[l] <= 1 || levels[l] >= n_items {
                return fallback(w, st);
            }
            cur = up;
        } else {
            // The root's entries are this level's nodes: all in hand.
            let mut all: BTreeMap<Vec<u8>, Vec<u8>> = top
                .keys
                .iter()
                .cloned()
                .zip(top.vals.iter().cloned())
                .collect();
            for (k, v) in up {
                match v {
                    Some(v) => {
                        all.insert(k, v);
                    }
                    None => {
                        all.remove(&k);
                    }
                }
            }
            anyhow::ensure!(all.len() as u64 == levels[l], "level {level} node count");
            let out: Vec<Entry> = all.into_iter().collect();
            st.internal_nodes_read = r
                .counts()
                .fetches
                .saturating_sub(before)
                .saturating_sub(st.leaves_read);
            st.rounds = r.counts().rounds - rounds_before;
            let root = finish_from(
                out,
                n_items as usize,
                level,
                count as u64,
                target,
                w,
                levels[..l].to_vec(),
            )?;
            return Ok((root, st));
        }
    }
    unreachable!()
}

/// Apply sorted edits (`Some(value)` upsert, `None` delete) to a tree in
/// place: every internal node is read (they are a small fraction of a
/// tree), only the leaves around the edits are read and re-cut, and the
/// internal levels are rebuilt in memory with the same code as [`build`].
/// The result is the tree a full build of the edited data gives, node for
/// node: leaves are re-cut from a node start (a known cut state) until the
/// new cuts land on an old leaf's end with no edits left before it, after
/// which the old leaves are what a full build would cut again.
fn apply_reading_all(
    r: &Reader,
    root: &Root,
    edits: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    target: usize,
    w: &mut Writer,
) -> Result<(Root, ApplyStats)> {
    let mut st = ApplyStats::default();
    if edits.is_empty() {
        return Ok((root.clone(), st));
    }
    // The leaf level as (first key, hash + count), from the internal nodes.
    let leaves: Vec<Entry> = if root.depth == 1 {
        let n = r.fetch(&[root.hash])?;
        st.leaves_read += 1;
        if n[0].keys.is_empty() {
            vec![]
        } else {
            let mut v = root.hash.to_vec();
            v.extend_from_slice(&(n[0].keys.len() as u64).to_le_bytes());
            vec![(n[0].keys[0].clone(), v)]
        }
    } else {
        let mut level = vec![root.hash];
        loop {
            let nodes = r.fetch(&level)?;
            st.internal_nodes_read += nodes.len() as u64;
            if nodes[0].level == 1 {
                break nodes
                    .iter()
                    .flat_map(|n| n.keys.iter().cloned().zip(n.vals.iter().cloned()))
                    .collect();
            }
            level = nodes
                .iter()
                .flat_map(|n| (0..n.keys.len()).map(|i| n.child(i).0).collect::<Vec<_>>())
                .collect();
        }
    };
    // Which leaf each edit falls in: the last whose first key <= the key.
    let leaf_of = |k: &[u8]| -> usize {
        leaves
            .partition_point(|(f, _)| f.as_slice() <= k)
            .saturating_sub(1)
    };
    let edits: Vec<(&Vec<u8>, &Option<Vec<u8>>)> = edits.iter().collect();
    let mut e = 0usize; // next edit
    let mut new_leaves: Vec<Entry> = vec![];
    let mut count = root.count as i64;
    let mut i = 0usize;
    if leaves.is_empty() {
        // An empty tree: the edits are the data.
        let mut c = Chunker::new(0, target);
        for (k, v) in &edits {
            if let Some(v) = v {
                c.push((*k).clone(), v.clone(), w)?;
                count += 1;
            }
        }
        c.flush(w)?;
        st.leaves_written += c.out.len() as u64;
        new_leaves = c.out;
        let n = count as usize;
        return Ok((
            finish_from(new_leaves, n, 0, count as u64, target, w, vec![])?,
            st,
        ));
    }
    while i < leaves.len() {
        if e >= edits.len() || leaf_of(edits[e].0) > i {
            new_leaves.push(leaves[i].clone());
            i += 1;
            continue;
        }
        // Re-cut from the start of leaf i.
        let mut c = Chunker::new(0, target);
        loop {
            let h: Hash = leaves[i].1[..32].try_into().unwrap();
            let leaf = r.fetch(&[h])?.remove(0);
            st.leaves_read += 1;
            let mut j = 0;
            let next_first = leaves.get(i + 1).map(|(k, _)| k.as_slice());
            // Merge the leaf's entries with the edits that fall in it.
            loop {
                let ek = edits
                    .get(e)
                    .map(|(k, _)| k.as_slice())
                    .filter(|k| next_first.map_or(true, |n| *k < n));
                let lk = leaf.keys.get(j).map(|k| k.as_slice());
                match (lk, ek) {
                    (None, None) => break,
                    (Some(l), Some(x)) if x <= l => {
                        if let Some(v) = edits[e].1 {
                            c.push(edits[e].0.clone(), v.clone(), w)?;
                            if x < l {
                                count += 1;
                            }
                        } else if x == l {
                            count -= 1;
                        }
                        if x == l {
                            j += 1;
                        }
                        e += 1;
                    }
                    (None, Some(_)) => {
                        if let Some(v) = edits[e].1 {
                            c.push(edits[e].0.clone(), v.clone(), w)?;
                            count += 1;
                        }
                        e += 1;
                    }
                    (Some(_), _) => {
                        c.push(leaf.keys[j].clone(), leaf.vals[j].clone(), w)?;
                        j += 1;
                    }
                }
            }
            i += 1;
            let edits_ahead = e < edits.len() && leaf_of(edits[e].0) < i;
            // In step with the old cuts: the rest is unchanged.
            if c.is_empty() && !edits_ahead {
                break;
            }
            if i >= leaves.len() {
                c.flush(w)?;
                break;
            }
        }
        st.leaves_written += c.out.len() as u64;
        new_leaves.extend(c.out);
    }
    anyhow::ensure!(count >= 0, "edit count went negative");
    // Level 0 had `count` entries; finish exactly as a full build does.
    Ok((
        finish_from(
            new_leaves,
            count as usize,
            0,
            count as u64,
            target,
            w,
            vec![],
        )?,
        st,
    ))
}

// ---------- reading ----------

#[derive(Debug, Default, Clone)]
pub struct Counts {
    pub rounds: u64,
    pub fetches: u64,
    pub bytes: u64,
}

#[derive(Default)]
struct RState {
    cache: HashMap<Hash, Arc<Node>>,
    counts: Counts,
}

/// Fetches one node's decoded bytes by hash (verified by the reader).
pub type Fetcher = Arc<dyn Fn(&Hash) -> Result<Vec<u8>> + Send + Sync>;

/// A reader over a node directory (or any [`Fetcher`]) that counts fetches.
/// Nodes are cached without limit (a cache with room). The nodes of one
/// level are fetched in parallel.
#[derive(Clone)]
pub struct Reader {
    dir: PathBuf,
    fetcher: Option<Fetcher>,
    st: Arc<Mutex<RState>>,
}

impl Reader {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            fetcher: None,
            st: Arc::default(),
        }
    }
    pub fn with_fetcher(f: Fetcher) -> Self {
        Self {
            dir: PathBuf::new(),
            fetcher: Some(f),
            st: Arc::default(),
        }
    }
    /// A node's decoded bytes (from the fetcher, or a directory of stored
    /// objects).
    fn load(&self, h: &Hash) -> Result<Vec<u8>> {
        match &self.fetcher {
            Some(f) => f(h),
            None => Ok(strata_object::decode(
                &std::fs::read(self.dir.join(hex(h)))?,
                strata_object::MAX_UNSIZED,
            )?),
        }
    }
    pub fn counts(&self) -> Counts {
        self.st.lock().unwrap().counts.clone()
    }
    pub fn reset_counts(&self) {
        self.st.lock().unwrap().counts = Counts::default();
    }
    /// Fetch a set of nodes in one round trip (those not cached, in parallel).
    fn fetch(&self, hs: &[Hash]) -> Result<Vec<Arc<Node>>> {
        let missing: Vec<Hash> = {
            let st = self.st.lock().unwrap();
            let mut m: Vec<Hash> = hs
                .iter()
                .filter(|h| !st.cache.contains_key(*h))
                .copied()
                .collect();
            m.sort();
            m.dedup();
            m
        };
        if !missing.is_empty() {
            let mut got: Vec<(Hash, Result<Vec<u8>>)> = vec![];
            // wasm32 has no threads: one at a time (a Worker prefetches
            // asynchronously and serves these from memory).
            #[cfg(target_arch = "wasm32")]
            for h in &missing {
                got.push((*h, self.load(h)));
            }
            #[cfg(not(target_arch = "wasm32"))]
            for group in missing.chunks(32) {
                std::thread::scope(|s| {
                    let hs: Vec<_> = group
                        .iter()
                        .map(|h| (*h, s.spawn(move || self.load(h))))
                        .collect();
                    for (h, j) in hs {
                        got.push((h, j.join().unwrap()));
                    }
                });
            }
            let mut st = self.st.lock().unwrap();
            for (h, b) in got {
                let b = b?;
                let sum: Hash = Sha256::digest(&b).into();
                if sum != h {
                    bail!("node {} failed verification", hex(&h));
                }
                st.counts.fetches += 1;
                st.counts.bytes += b.len() as u64;
                st.cache.insert(h, Arc::new(Node::decode(&b)?));
            }
            st.counts.rounds += 1;
        }
        let st = self.st.lock().unwrap();
        Ok(hs.iter().map(|h| st.cache[h].clone()).collect())
    }

    /// Every node of a tree (internal and leaves), fetched a level at a time
    /// with each level in parallel; for pinning a whole map.
    pub fn all_nodes(&self, root: &Root) -> Result<Vec<Hash>> {
        let mut out = vec![];
        let mut level = vec![root.hash];
        while !level.is_empty() {
            let nodes = self.fetch(&level)?;
            out.extend(level.iter().copied());
            level = nodes
                .iter()
                .filter(|n| n.level > 0)
                .flat_map(|n| (0..n.keys.len()).map(|i| n.child(i).0).collect::<Vec<_>>())
                .collect();
            level.sort();
            level.dedup();
        }
        Ok(out)
    }

    /// Every node of a tree (internal and leaves) by reading only the
    /// internal ones: leaves are named by their parents and not fetched.
    /// For listing (and sizing) what a whole map would download.
    pub fn node_hashes(&self, root: &Root) -> Result<Vec<Hash>> {
        let mut out = vec![];
        let mut level = vec![root.hash];
        loop {
            let nodes = self.fetch(&level)?;
            out.extend(level.iter().copied());
            if nodes.iter().all(|n| n.level == 0) {
                return Ok(out);
            }
            let mut next: Vec<Hash> = nodes
                .iter()
                .filter(|n| n.level > 0)
                .flat_map(|n| (0..n.keys.len()).map(|i| n.child(i).0).collect::<Vec<_>>())
                .collect();
            next.sort();
            next.dedup();
            if nodes.iter().all(|n| n.level == 1) {
                out.extend(next);
                return Ok(out);
            }
            level = next;
        }
    }

    /// The first key of every leaf, in order (reads the internal nodes).
    pub fn leaf_first_keys(&self, root: &Root) -> Result<Vec<Vec<u8>>> {
        if root.depth == 1 {
            let n = self.fetch(&[root.hash])?;
            return Ok(n[0].keys.first().cloned().into_iter().collect());
        }
        let mut level = vec![root.hash];
        loop {
            let nodes = self.fetch(&level)?;
            if nodes[0].level == 1 {
                return Ok(nodes.iter().flat_map(|n| n.keys.iter().cloned()).collect());
            }
            level = nodes
                .iter()
                .flat_map(|n| (0..n.keys.len()).map(|i| n.child(i).0).collect::<Vec<_>>())
                .collect();
        }
    }

    /// Preload every internal (non-leaf) node of a tree, a level at a time
    /// with each level fetched in parallel; not counted. Returns their total
    /// compressed size.
    pub fn preload_internal(&self, root: &Root) -> Result<u64> {
        let saved = self.counts();
        let mut level = vec![root.hash];
        for _ in 1..root.depth {
            let nodes = self.fetch(&level)?;
            level = nodes
                .iter()
                .filter(|n| n.level > 1)
                .flat_map(|n| (0..n.keys.len()).map(|i| n.child(i).0).collect::<Vec<_>>())
                .collect();
        }
        let after = self.counts();
        self.st.lock().unwrap().counts = saved;
        Ok(after.bytes - self.counts().bytes)
    }

    /// Entries with `lo <= key < hi`, at most `limit`, a level at a time.
    pub fn range(
        &self,
        root: &Root,
        lo: &[u8],
        hi: &[u8],
        limit: Option<u64>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut frontier = vec![root.hash];
        loop {
            let nodes = self.fetch(&frontier)?;
            if nodes.iter().all(|n| n.level == 0) {
                let mut out = vec![];
                for n in nodes {
                    for (k, v) in n.keys.iter().zip(&n.vals) {
                        if k.as_slice() >= lo && k.as_slice() < hi {
                            out.push((k.clone(), v.clone()));
                        }
                    }
                }
                out.sort();
                if let Some(l) = limit {
                    out.truncate(l as usize);
                }
                return Ok(out);
            }
            // Children whose key span can intersect [lo, hi), in order,
            // stopping once `limit` entries are surely covered. A child's
            // span ends at the next child's first key (across nodes too).
            let flat: Vec<(&Vec<u8>, Hash, u64)> = nodes
                .iter()
                .flat_map(|n| {
                    (0..n.keys.len()).map(move |i| (&n.keys[i], n.child(i).0, n.child(i).1))
                })
                .collect();
            let mut next = vec![];
            let mut covered = 0u64;
            for (j, (start, h, cnt)) in flat.iter().enumerate() {
                if start.as_slice() >= hi {
                    break;
                }
                if flat.get(j + 1).is_some_and(|e| e.0.as_slice() <= lo) {
                    continue;
                }
                next.push(*h);
                if start.as_slice() >= lo {
                    covered += cnt;
                }
                if limit.is_some_and(|l| covered >= l) {
                    break;
                }
            }
            frontier = next;
        }
    }

    /// Point lookups, batched: one round trip per level.
    pub fn get_many(&self, root: &Root, keys: &[Vec<u8>]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut want: Vec<Vec<u8>> = keys.to_vec();
        want.sort();
        want.dedup();
        let mut frontier = vec![root.hash];
        loop {
            let nodes = self.fetch(&frontier)?;
            if nodes.iter().all(|n| n.level == 0) {
                let mut out = BTreeMap::new();
                for n in nodes {
                    for (k, v) in n.keys.iter().zip(&n.vals) {
                        if want.binary_search(k).is_ok() {
                            out.insert(k.clone(), v.clone());
                        }
                    }
                }
                return Ok(out);
            }
            let flat: Vec<(&Vec<u8>, Hash)> = nodes
                .iter()
                .flat_map(|n| (0..n.keys.len()).map(move |i| (&n.keys[i], n.child(i).0)))
                .collect();
            let mut next: Vec<Hash> = vec![];
            for w in &want {
                // The last child whose first key <= w holds w, if anything does.
                let j = flat.partition_point(|(k, _)| k.as_slice() <= w.as_slice());
                if j == 0 {
                    continue;
                }
                let h = flat[j - 1].1;
                if !next.contains(&h) {
                    next.push(h);
                }
            }
            frontier = next;
        }
    }
}

// ---------- keys ----------

/// Composite key: text parts separated by 0x00 (parts must not contain it).
pub fn key(parts: &[&[u8]]) -> Vec<u8> {
    let mut k = vec![];
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            k.push(0);
        }
        k.extend_from_slice(p);
    }
    k
}

/// Big-endian i64 with the sign bit flipped: byte order = numeric order.
pub fn int_key(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1 << 63)).to_be_bytes()
}

/// All keys with this prefix: `[prefix, next prefix)`, where the next prefix
/// increments the last byte that is not 0xff (keys hold binary integers, so
/// "prefix + 0xff" is not an upper bound).
pub fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut hi = prefix.to_vec();
    while let Some(&last) = hi.last() {
        if last == 0xff {
            hi.pop();
        } else {
            *hi.last_mut().unwrap() += 1;
            return (prefix.to_vec(), hi);
        }
    }
    (prefix.to_vec(), vec![0xff; prefix.len() + 9])
}

/// Nodes reachable from a root (for update-cost comparisons).
pub fn reachable(dir: &Path, root: &Hash) -> Result<HashMap<Hash, u64>> {
    let mut out = HashMap::new();
    let mut stack = vec![*root];
    while let Some(h) = stack.pop() {
        if out.contains_key(&h) {
            continue;
        }
        let b = std::fs::read(dir.join(hex(&h)))?;
        out.insert(h, b.len() as u64);
        let n = Node::decode(&strata_object::decode(&b, strata_object::MAX_UNSIZED)?)?;
        if n.level > 0 {
            for i in 0..n.keys.len() {
                stack.push(n.child(i).0);
            }
        }
    }
    Ok(out)
}

pub fn hash_hex(h: &Hash) -> String {
    hex(h)
}

pub fn parse_hash(s: &str) -> Result<Hash> {
    let mut h = [0u8; 32];
    for i in 0..32 {
        h[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)?;
    }
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(n: u32, skip: Option<u32>) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .filter(|i| Some(*i) != skip)
            .map(|i| {
                (
                    format!("k{i:08}").into_bytes(),
                    format!("value {i} {}", "x".repeat((i % 50) as usize)).into_bytes(),
                )
            })
            .collect()
    }

    #[test]
    fn range_and_lookups_match_a_btreemap() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Writer::new(dir.path()).unwrap();
        let e = entries(50_000, None);
        let root = build(e.clone(), 2048, &mut w).unwrap();
        assert!(root.depth >= 3);
        let r = Reader::new(dir.path());
        let m: BTreeMap<_, _> = e.into_iter().collect();
        let (lo, hi) = (b"k00012345".to_vec(), b"k00013000".to_vec());
        let want: Vec<_> = m
            .range(lo.clone()..hi.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(r.range(&root, &lo, &hi, None).unwrap(), want);
        assert_eq!(
            r.range(&root, &lo, &hi, Some(10)).unwrap(),
            want[..10].to_vec()
        );
        let keys: Vec<Vec<u8>> = [7u32, 49_999, 123, 30_001]
            .iter()
            .map(|i| format!("k{i:08}").into_bytes())
            .chain([b"nope".to_vec()])
            .collect();
        let got = r.get_many(&root, &keys).unwrap();
        assert_eq!(got.len(), 4);
        for k in &keys[..4] {
            assert_eq!(got[k], m[k]);
        }
        // A batch of lookups costs one round trip per level.
        let r2 = Reader::new(dir.path());
        r2.get_many(&root, &keys).unwrap();
        assert_eq!(r2.counts().rounds, root.depth as u64);
    }

    #[test]
    fn node_hashes_lists_every_node_without_reading_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Writer::new(dir.path()).unwrap();
        for n in [10u32, 50_000] {
            let root = build(entries(n, None), 2048, &mut w).unwrap();
            let mut all = Reader::new(dir.path()).all_nodes(&root).unwrap();
            let r = Reader::new(dir.path());
            let mut listed = r.node_hashes(&root).unwrap();
            all.sort();
            listed.sort();
            assert_eq!(listed, all);
            let leaves = root.levels.first().copied().unwrap_or(1);
            let read = r.counts().fetches;
            assert_eq!(read, all.len() as u64 - if root.depth > 1 { leaves } else { 0 });
        }
    }

    #[test]
    fn prefix_range_covers_binary_suffixes() {
        let (lo, hi) = prefix_range(b"abc\0");
        assert_eq!(hi, b"abc\x01".to_vec());
        let k = [b"abc\0".as_slice(), &[0xff, 0xff]].concat();
        assert!(k >= lo && k < hi);
    }

    #[test]
    fn same_data_same_tree_and_small_changes_share_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let mut a = Writer::new(dir.path()).unwrap();
        let ra = build(entries(20_000, Some(777)), 2048, &mut a).unwrap();
        let mut a2 = Writer::new(dir.path()).unwrap();
        assert_eq!(
            build(entries(20_000, Some(777)), 2048, &mut a2)
                .unwrap()
                .hash,
            ra.hash
        );
        let mut b = Writer::new(dir.path()).unwrap();
        build(entries(20_000, None), 2048, &mut b).unwrap();
        let new: Vec<_> = b.written.difference(&a.written).collect();
        assert!(
            new.len() <= 2 * ra.depth as usize,
            "one insert rewrote {} of {} nodes",
            new.len(),
            b.written.len()
        );
    }

    #[test]
    fn every_small_tree_converges() {
        // Regression: upper levels repeat keys; unsalted boundaries made a
        // 2-entry level never shrink and the build loop wrote nodes forever.
        let dir = tempfile::tempdir().unwrap();
        for n in 0..400u32 {
            let mut w = Writer::new(dir.path()).unwrap();
            let r = build(entries(n, None), 512, &mut w).unwrap();
            assert!(r.depth <= 6, "n={n} depth {}", r.depth);
            assert!(
                w.written.len() as u32 <= n.max(1) * 2 + 2,
                "n={n}: {} nodes",
                w.written.len()
            );
            assert_eq!(r.count, n as u64);
        }
    }

    /// In-place edits give the tree a full build of the edited data gives
    /// (same root hash), across sizes, node targets (tiny targets exercise
    /// the 4x cap and the single-root fallback) and edit mixes.
    #[test]
    fn apply_equals_full_build() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut rnd = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n.max(1)
        };
        let dir = tempfile::tempdir().unwrap();
        let (mut cases, mut fallbacks) = (0, 0);
        for &target in &[128usize, 200, 1024, 4096] {
            for &n in &[0u64, 1, 2, 5, 40, 300, 3000, 20_000] {
                for round in 0..6 {
                    let mut w = Writer::new(dir.path()).unwrap();
                    let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
                    for i in 0..n {
                        let k = format!("k{:07}", i * 3).into_bytes();
                        let v = format!("v{i}{}", "y".repeat(rnd(60) as usize)).into_bytes();
                        m.insert(k, v);
                    }
                    let root = build(m.clone().into_iter().collect(), target, &mut w).unwrap();
                    // Edits: updates, deletes and inserts, clustered or spread.
                    let n_edits = match round {
                        0 => 1,
                        1 => 3,
                        2 => 20,
                        3 => 200,
                        4 => (n / 2).max(1),
                        _ => n + 50,
                    };
                    let mut edits: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
                    let span = n * 3 + 30;
                    let base = rnd(span);
                    for _ in 0..n_edits {
                        let at = if round % 2 == 0 {
                            base + rnd(40)
                        } else {
                            rnd(span)
                        };
                        let k = format!("k{at:07}").into_bytes();
                        let v = match rnd(3) {
                            0 => None,
                            _ => Some(
                                format!("new{at}{}", "z".repeat(rnd(80) as usize)).into_bytes(),
                            ),
                        };
                        edits.insert(k, v);
                    }
                    let mut want = m.clone();
                    for (k, v) in &edits {
                        match v {
                            Some(v) => {
                                want.insert(k.clone(), v.clone());
                            }
                            None => {
                                want.remove(k);
                            }
                        }
                    }
                    let full = build(want.clone().into_iter().collect(), target, &mut w).unwrap();
                    let r = Reader::new(dir.path());
                    let (got, st) = apply(&r, &root, &edits, target, &mut w).unwrap();
                    assert_eq!(
                        (hex(&got.hash), got.depth, got.count, &got.levels),
                        (hex(&full.hash), full.depth, full.count, &full.levels),
                        "target {target}, n {n}, round {round}"
                    );
                    if st.read_all_internal {
                        fallbacks += 1;
                    }
                    // A second round on the edited tree (its level counts
                    // came from the edit, not a build).
                    let mut edits2: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
                    for _ in 0..(n_edits / 2).max(1) {
                        let at = rnd(span + 60);
                        let v = (rnd(4) > 0).then(|| format!("again{at}").into_bytes());
                        edits2.insert(format!("k{at:07}").into_bytes(), v);
                    }
                    for (k, v) in &edits2 {
                        match v {
                            Some(v) => {
                                want.insert(k.clone(), v.clone());
                            }
                            None => {
                                want.remove(k);
                            }
                        }
                    }
                    let full2 = build(want.into_iter().collect(), target, &mut w).unwrap();
                    let (got2, _) = apply(&r, &got, &edits2, target, &mut w).unwrap();
                    assert_eq!(
                        (hex(&got2.hash), got2.depth, got2.count, &got2.levels),
                        (hex(&full2.hash), full2.depth, full2.count, &full2.levels),
                        "second round: target {target}, n {n}, round {round}"
                    );
                    cases += 1;
                }
            }
        }
        assert_eq!(cases, 4 * 8 * 6);
        eprintln!("{cases} cases, {fallbacks} read every internal node");
        assert!(fallbacks < cases / 2, "the path-only edit is the usual one");
    }
}
