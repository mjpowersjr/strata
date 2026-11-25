//! Publishing a tantivy index as v2 objects (docs/format.md sections 3
//! and 4): content-addressed chunks, one hot pack per segment (what opening
//! reads, plus field norms), zstd delete objects, a record-key map and an
//! immutable object per segment. Pure computation over in-memory files.

use crate::keys::ValueRef;
use crate::ObjRef;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use strata_object::Codec;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr};
use tantivy::schema::*;
use tantivy::tokenizer::{AsciiFoldingFilter, LowerCaser, SimpleTokenizer, TextAnalyzer};
use tantivy::{Directory, HasLen, Index, TantivyDocument};

pub const FOLD: &str = "fold";

pub fn fold_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build()
}

/// A field as readers see it (published in the version object).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldInfo {
    pub name: String,
    /// "text", "keyword", "year", "stored", "sort".
    pub kind: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub boost: f32,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stored: bool,
}

fn is_zero(x: &f32) -> bool {
    *x == 0.0
}

/// The record key of a group (first 8 bytes of its sha256, little-endian).
pub fn record_key(group: &str) -> u64 {
    u64::from_le_bytes(Sha256::digest(group.as_bytes())[..8].try_into().unwrap())
}

/// In-memory index files that record which chunks a reader touches (to
/// build hot packs from what opening reads).
#[derive(Debug, Clone)]
struct Recorder {
    files: Arc<BTreeMap<String, Arc<Vec<u8>>>>,
    chunk: u64,
    seen: Arc<Mutex<HashSet<(String, u64)>>>,
}

#[derive(Debug)]
struct RecHandle {
    rec: Recorder,
    name: String,
    data: Arc<Vec<u8>>,
}

impl HasLen for RecHandle {
    fn len(&self) -> usize {
        self.data.len()
    }
}

impl FileHandle for RecHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > range.start {
            let mut s = self.rec.seen.lock().unwrap();
            for i in
                (range.start as u64 / self.rec.chunk)..=((range.end as u64 - 1) / self.rec.chunk)
            {
                s.insert((self.name.clone(), i));
            }
        }
        let d = self
            .data
            .get(range)
            .ok_or_else(|| io::Error::other("read past end"))?;
        Ok(OwnedBytes::new(d.to_vec()))
    }
}

impl Directory for Recorder {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let name = path.to_string_lossy().to_string();
        let data = self
            .files
            .get(&name)
            .cloned()
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(Arc::new(RecHandle {
            rec: self.clone(),
            name,
            data,
        }))
    }
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(io::Error::other("read-only")),
            filepath: path.to_path_buf(),
        })
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(&*path.to_string_lossy()))
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::IoError {
            io_error: Arc::new(io::Error::other("read-only")),
            filepath: path.to_path_buf(),
        })
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.files
            .get(&*path.to_string_lossy())
            .map(|d| d.to_vec())
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileRef {
    pub size: u64,
    pub chunks: Vec<String>,
}

/// A segment's deleted documents: its `.del` file (tantivy's alive bitset),
/// zstd-compressed as one object. It changes whenever the segment loses
/// documents, so it isn't part of the immutable segment object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelRef {
    pub name: String,
    /// Uncompressed size.
    pub size: u64,
    pub object: ObjRef,
}

/// One segment in a version: three object references.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentRef {
    pub id: String,
    pub max_doc: u32,
    /// The [`SegmentObj`] (JSON).
    pub object: ObjRef,
    /// What opening the segment reads, plus its field norms, concatenated.
    pub hot: ObjRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub del: Option<DelRef>,
}

/// Which documents each record key has in a segment: a prolly map from the
/// key (8 bytes, big-endian) to its doc ids (delta varints). For publishers
/// that apply deletes without reading the whole `record_key` column (a
/// Worker); readers never need it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeysRef {
    pub root: String,
    pub depth: u8,
    pub count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub levels: Vec<u64>,
}

/// A segment's immutable description, stored as an object: the same
/// segment in a later version is the same object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentObj {
    pub id: String,
    pub files: BTreeMap<String, FileRef>,
    /// The hot pack's parts in order: (chunk sha256, length).
    pub hot_parts: Vec<(String, u64)>,
    pub keys: KeysRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchRef {
    pub engine: String,
    /// Fields, kinds and boosts (so readers build queries without the profile).
    #[serde(default)]
    pub fields: Vec<FieldInfo>,
    pub chunk_size: u64,
    /// Index-wide files (`meta.json`, `.managed.json`): small, new each version.
    pub files: BTreeMap<String, FileRef>,
    pub segments: Vec<SegmentRef>,
}

/// The segment a file belongs to (`{32 hex}.{ext}`), if any.
pub fn segment_of(n: &str) -> Option<String> {
    let stem = n.split('.').next()?;
    (stem.len() == 32 && stem.bytes().all(|b| b.is_ascii_hexdigit())).then(|| stem.to_string())
}

pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
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

/// Decode a [`KeysRef`] value: the doc ids, ascending.
pub fn decode_doc_ids(v: &[u8]) -> Vec<u32> {
    let (mut out, mut cur, mut acc, mut shift) = (vec![], 0u64, 0u64, 0);
    for &b in v {
        acc |= ((b & 0x7f) as u64) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            cur += acc;
            out.push(cur as u32);
            acc = 0;
            shift = 0;
        }
    }
    out
}

/// Chunk an index (all its files, by name) into content-addressed objects
/// (`put(name, stored form)`, see `strata_object`), build each segment's hot pack, delete object, key map (nodes
/// written by `w`) and segment object, and return the version's search
/// reference. Runs the same natively and in wasm32, so a Worker publishing
/// one new segment writes exactly the objects the native publisher would.
pub fn publish_index(
    index_files: BTreeMap<String, Vec<u8>>,
    chunk_size: u64,
    put: &mut dyn FnMut(&str, &[u8]) -> Result<()>,
    w: &mut crate::prolly::Writer,
) -> Result<SearchRef> {
    let mut files = BTreeMap::new();
    let mut chunk_bytes: HashMap<String, Vec<u8>> = HashMap::new();
    let index_files: BTreeMap<String, Arc<Vec<u8>>> = index_files
        .into_iter()
        .filter(|(n, _)| !n.ends_with(".lock"))
        .map(|(n, d)| (n, Arc::new(d)))
        .collect();
    let mut dels: BTreeMap<String, DelRef> = BTreeMap::new();
    for (name, data) in &index_files {
        if name.ends_with(".del") {
            let seg = segment_of(name).context("delete file name")?;
            let (sha, stored) = strata_object::encode_named(data, Codec::Zstd(9));
            put(&sha, &stored)?;
            dels.insert(
                seg,
                DelRef {
                    name: name.clone(),
                    size: data.len() as u64,
                    object: ObjRef {
                        sha256: sha,
                        size: data.len() as u64,
                    },
                },
            );
            continue;
        }
        let mut chunks = vec![];
        for c in data.chunks(chunk_size as usize) {
            // Index files are stored as they are (tantivy compresses what
            // compresses, and chunks are read whole and often).
            let (sha, stored) = strata_object::encode_named(c, Codec::Raw);
            put(&sha, &stored)?;
            chunk_bytes.entry(sha.clone()).or_insert_with(|| c.to_vec());
            chunks.push(sha);
        }
        files.insert(
            name.clone(),
            FileRef {
                size: data.len() as u64,
                chunks,
            },
        );
    }
    // What opening reads, per file and chunk.
    let rec = Recorder {
        files: Arc::new(index_files.clone()),
        chunk: chunk_size,
        seen: Arc::default(),
    };
    let index = Index::open(rec.clone())?;
    index.tokenizers().register(FOLD, fold_analyzer());
    let reader: tantivy::IndexReader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()?;
    let seen = rec.seen.lock().unwrap().clone();
    let mut by_seg: BTreeMap<String, Vec<(String, u64)>> = BTreeMap::new();
    let mut add = |seg: &str, sha: &str, len: u64| {
        let v = by_seg.entry(seg.to_string()).or_default();
        if !v.iter().any(|x| x.0 == sha) {
            v.push((sha.to_string(), len));
        }
    };
    let mut seen: Vec<(String, u64)> = seen.into_iter().collect();
    seen.sort();
    for (name, i) in &seen {
        if let (Some(seg), Some(f)) = (segment_of(name), files.get(name)) {
            if let Some(sha) = f.chunks.get(*i as usize) {
                add(&seg, sha, chunk_bytes[sha].len() as u64);
            }
        }
    }
    for (name, f) in &files {
        if name.ends_with(".fieldnorm") {
            if let Some(seg) = segment_of(name) {
                for sha in &f.chunks {
                    add(&seg, sha, chunk_bytes[sha].len() as u64);
                }
            }
        }
    }
    let searcher = reader.searcher();
    let mut segments = vec![];
    for (sr, meta) in searcher
        .segment_readers()
        .iter()
        .zip(index.searchable_segment_metas()?)
    {
        let seg = meta.id().uuid_string();
        let parts = by_seg.remove(&seg).unwrap_or_default();
        let mut pack = vec![];
        for (sha, _) in &parts {
            pack.extend_from_slice(&chunk_bytes[sha]);
        }
        let (hot_sha, stored) = strata_object::encode_named(&pack, Codec::Raw);
        put(&hot_sha, &stored)?;
        // Record key -> doc ids, in key order.
        let col = sr.fast_fields().u64("record_key")?;
        let mut by_key: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for d in 0..sr.max_doc() {
            if let Some(k) = col.first(d) {
                by_key.entry(k).or_default().push(d);
            }
        }
        let entries: Vec<(Vec<u8>, Vec<u8>)> = by_key
            .into_iter()
            .map(|(k, ds)| {
                let mut v = vec![];
                let mut prev = 0u64;
                for d in ds {
                    put_varint(&mut v, d as u64 - prev);
                    prev = d as u64;
                }
                (k.to_be_bytes().to_vec(), v)
            })
            .collect();
        let keys = crate::prolly::build(entries, 16384, w)?;
        let obj = SegmentObj {
            id: seg.clone(),
            files: files
                .iter()
                .filter(|(n, _)| segment_of(n).as_deref() == Some(&seg))
                .map(|(n, f)| (n.clone(), f.clone()))
                .collect(),
            hot_parts: parts,
            keys: KeysRef {
                root: crate::prolly::hash_hex(&keys.hash),
                depth: keys.depth,
                count: keys.count,
                levels: keys.levels.clone(),
            },
        };
        let ob = serde_json::to_vec(&obj)?;
        let (osha, stored) = strata_object::encode_named(&ob, Codec::Zstd(3));
        put(&osha, &stored)?;
        segments.push(SegmentRef {
            id: seg.clone(),
            max_doc: sr.max_doc(),
            object: ObjRef {
                sha256: osha,
                size: ob.len() as u64,
            },
            hot: ObjRef {
                sha256: hot_sha,
                size: pack.len() as u64,
            },
            del: dels.remove(&seg),
        });
    }
    Ok(SearchRef {
        engine: "tantivy-0.27-sstable".into(),
        fields: vec![],
        chunk_size,
        files: files
            .into_iter()
            .filter(|(n, _)| segment_of(n).is_none())
            .collect(),
        segments,
    })
}

/// How a column of the search query becomes a field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    /// Searchable text with field norms (BM25 length normalization).
    Text,
    /// Searchable text without norms (long or repetitive fields).
    TextNoNorms,
    /// Long text searched only when a query asks for it (`also`), such as
    /// lyrics transcripts: with norms, without positions.
    TextOptional,
    /// Raw term (exact filter) plus fast column, no norms.
    Keyword,
    /// Integer year: `year` (postings, for ranges), `year_f` (column, for
    /// facets and sorting) and `dated`. NULL means undated.
    Year,
    /// Stored for display only.
    Stored,
    /// u64 fast column (sorting), also stored.
    SortU64,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub column: &'static str,
    pub kind: Kind,
    /// Also store the value for display (text and keyword fields).
    pub stored: bool,
    /// BM25 boost for text fields.
    pub boost: f32,
}

pub fn field_infos(spec: &SearchSpec) -> Vec<FieldInfo> {
    spec.fields
        .iter()
        .map(|f| FieldInfo {
            name: f.column.to_string(),
            kind: match f.kind {
                Kind::Text | Kind::TextNoNorms => "text",
                Kind::TextOptional => "text_optional",
                Kind::Keyword => "keyword",
                Kind::Year => "year",
                Kind::Stored => "stored",
                Kind::SortU64 => "sort",
            }
            .into(),
            boost: if matches!(f.kind, Kind::Text | Kind::TextNoNorms | Kind::TextOptional) {
                f.boost
            } else {
                0.0
            },
            stored: f.stored || matches!(f.kind, Kind::Stored),
        })
        .collect()
}

/// A collection's search documents.
#[derive(Debug, Clone)]
pub struct SearchSpec {
    /// One row per document. Column 0 is the group key: the engine item id
    /// whose change replaces all of its documents. The SQL contains `{WHERE}`,
    /// replaced by `1=1` for a full build or by `<group> IN (…)` for an update
    /// (`group_column` names the column to filter on).
    pub sql: &'static str,
    pub group_column: &'static str,
    pub fields: Vec<Field>,
}

fn text_opts(norms: bool, stored: bool) -> TextOptions {
    let o = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(FOLD)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
            .set_fieldnorms(norms),
    );
    if stored {
        o.set_stored()
    } else {
        o
    }
}

pub fn schema(spec: &SearchSpec) -> Schema {
    let mut s = Schema::builder();
    for f in &spec.fields {
        match f.kind {
            Kind::Text => {
                s.add_text_field(f.column, text_opts(true, f.stored));
            }
            Kind::TextNoNorms => {
                s.add_text_field(f.column, text_opts(false, f.stored));
            }
            Kind::TextOptional => {
                let o = TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer(FOLD)
                        .set_index_option(IndexRecordOption::WithFreqs)
                        .set_fieldnorms(true),
                );
                s.add_text_field(f.column, if f.stored { o.set_stored() } else { o });
            }
            Kind::Keyword => {
                let o = TextOptions::default()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("raw")
                            .set_index_option(IndexRecordOption::Basic)
                            .set_fieldnorms(false),
                    )
                    .set_fast("raw");
                s.add_text_field(f.column, if f.stored { o.set_stored() } else { o });
            }
            Kind::Year => {
                s.add_i64_field("year", NumericOptions::default().set_indexed());
                s.add_i64_field("year_f", NumericOptions::default().set_fast().set_stored());
                s.add_bool_field("dated", NumericOptions::default().set_indexed());
            }
            Kind::Stored => {
                s.add_text_field(f.column, STORED);
            }
            Kind::SortU64 => {
                s.add_u64_field(f.column, NumericOptions::default().set_fast().set_stored());
            }
        }
    }
    s.add_u64_field("record_key", NumericOptions::default().set_fast());
    s.build()
}

/// One row's contribution to its group's hash (order-independent: group
/// hashes are wrapping sums of row hashes). `vals` are the field values in
/// field order.
pub fn row_hash(vals: &[ValueRef<'_>]) -> u64 {
    let mut h = Sha256::new();
    for v in vals {
        match v {
            ValueRef::Null => h.update([0u8]),
            ValueRef::Integer(x) => {
                h.update([1u8]);
                h.update(x.to_le_bytes());
            }
            ValueRef::Real(x) => {
                h.update([2u8]);
                h.update(x.to_le_bytes());
            }
            ValueRef::Text(t) | ValueRef::Blob(t) => {
                h.update([3u8]);
                h.update((t.len() as u64).to_le_bytes());
                h.update(t);
            }
        }
    }
    u64::from_le_bytes(h.finalize()[..8].try_into().unwrap())
}

/// A search document from one row: `vals[i]` is the value of `fields[i]`.
pub fn make_doc(
    schema: &Schema,
    group: &str,
    fields: &[&Field],
    vals: &[ValueRef<'_>],
) -> Result<TantivyDocument> {
    let mut d = TantivyDocument::default();
    d.add_u64(schema.get_field("record_key")?, record_key(group));
    for (f, v) in fields.iter().zip(vals) {
        match f.kind {
            Kind::Text | Kind::TextNoNorms | Kind::TextOptional | Kind::Keyword | Kind::Stored => {
                if let ValueRef::Text(t) = v {
                    d.add_text(schema.get_field(f.column)?, String::from_utf8_lossy(t));
                } else if let ValueRef::Integer(x) = v {
                    d.add_text(schema.get_field(f.column)?, x.to_string());
                }
            }
            Kind::Year => {
                let y = match v {
                    ValueRef::Integer(x) => Some(*x),
                    _ => None,
                };
                d.add_i64(schema.get_field("year")?, y.unwrap_or(0));
                d.add_i64(schema.get_field("year_f")?, y.unwrap_or(0));
                d.add_bool(schema.get_field("dated")?, y.is_some());
            }
            Kind::SortU64 => {
                if let ValueRef::Integer(x) = v {
                    d.add_u64(schema.get_field(f.column)?, (*x).max(0) as u64);
                }
            }
        }
    }
    Ok(d)
}
