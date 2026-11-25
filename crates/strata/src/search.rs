//! Search (docs/format.md section 4): one tantivy index per collection,
//! kept by the publisher in a working directory and updated in place
//! (deletes by record key, new documents as a new segment, merges by
//! policy), then published as content-addressed chunks with one hot pack
//! per segment.

use anyhow::{Context, Result};
use strata_core::keys::ValueRef as CoreValueRef;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use tantivy::query::RangeQuery;
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, TantivyDocument, Term};

/// Run the search SQL (all rows, or only `filter`'s groups) and call `f`
/// with each row's group, the row, and the field columns.
fn scan(
    spec: &SearchSpec,
    c: &Connection,
    filter: Option<&[String]>,
    mut f: impl FnMut(&str, &rusqlite::Row<'_>, &[(usize, &Field)]) -> Result<()>,
) -> Result<()> {
    let (sql, params): (String, Vec<String>) = match filter {
        None => (spec.sql.replace("{WHERE}", "1=1"), vec![]),
        Some(keys) => {
            let marks = vec!["?"; keys.len()].join(",");
            (
                spec.sql
                    .replace("{WHERE}", &format!("{} IN ({marks})", spec.group_column)),
                keys.to_vec(),
            )
        }
    };
    let mut st = c.prepare(&sql).with_context(|| sql.clone())?;
    let names: Vec<String> = st.column_names().map(|s| s.to_string()).collect();
    let idx = |col: &str| {
        names
            .iter()
            .position(|n| n == col)
            .with_context(|| format!("search SQL lacks column {col}"))
    };
    let cols: Vec<(usize, &Field)> = spec
        .fields
        .iter()
        .map(|f| Ok((idx(f.column)?, f)))
        .collect::<Result<_>>()?;
    let mut rows = st.query(rusqlite::params_from_iter(params.iter()))?;
    while let Some(r) = rows.next()? {
        let group: String = r.get(0)?;
        f(&group, r, &cols)?;
    }
    Ok(())
}

/// The field values of one row, in field order (for [`make_doc`] and
/// [`row_hash`] in the core crate).
fn field_vals<'r>(
    r: &'r rusqlite::Row<'_>,
    cols: &[(usize, &Field)],
) -> Result<Vec<CoreValueRef<'r>>> {
    cols.iter()
        .map(|(i, _)| Ok(crate::keys::conv(r.get_ref(*i)?)))
        .collect()
}

fn row_hash(r: &rusqlite::Row<'_>, cols: &[(usize, &Field)]) -> Result<u64> {
    Ok(strata_core::search::row_hash(&field_vals(r, cols)?))
}

fn make_doc(
    schema: &Schema,
    group: &str,
    r: &rusqlite::Row<'_>,
    cols: &[(usize, &Field)],
) -> Result<TantivyDocument> {
    let fields: Vec<&Field> = cols.iter().map(|(_, f)| *f).collect();
    strata_core::search::make_doc(schema, group, &fields, &field_vals(r, cols)?)
}

/// The documents of the given groups, as the publisher builds them (for
/// writers other than [`update`], such as the Worker spike).
pub fn docs_for_groups(
    spec: &SearchSpec,
    schema: &Schema,
    c: &Connection,
    groups: &[String],
) -> Result<Vec<TantivyDocument>> {
    let mut out = vec![];
    for batch in groups.chunks(500) {
        scan(spec, c, Some(batch), |g, r, cols| {
            out.push(make_doc(schema, g, r, cols)?);
            Ok(())
        })?;
    }
    Ok(out)
}

/// Group hashes of the last build, kept next to the index directory:
/// `{group}\t{hash}` lines, zstd.
fn groups_path(dir: &Path) -> PathBuf {
    let name = dir
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "search".into());
    dir.with_file_name(format!("{name}-groups.tsv.zst"))
}

fn load_groups(p: &Path) -> Option<HashMap<String, u64>> {
    let b = zstd::decode_all(std::fs::File::open(p).ok()?).ok()?;
    let s = String::from_utf8(b).ok()?;
    let mut m = HashMap::new();
    for line in s.lines() {
        let (g, h) = line.rsplit_once('\t')?;
        m.insert(g.to_string(), h.parse().ok()?);
    }
    Some(m)
}

fn save_groups(p: &Path, m: &HashMap<String, u64>) -> Result<()> {
    let mut v: Vec<(&String, &u64)> = m.iter().collect();
    v.sort();
    let mut s = String::new();
    for (g, h) in v {
        anyhow::ensure!(
            !g.contains('\t') && !g.contains('\n'),
            "group key with a tab or newline: {g:?}"
        );
        s.push_str(g);
        s.push('\t');
        s.push_str(&h.to_string());
        s.push('\n');
    }
    let tmp = p.with_extension("tmp");
    std::fs::write(&tmp, zstd::encode_all(s.as_bytes(), 3)?)?;
    std::fs::rename(&tmp, p)?;
    Ok(())
}

/// What an update did.
#[derive(Debug, Default, Serialize)]
pub struct SearchUpdate {
    pub full_build: bool,
    /// Groups whose documents changed or are new.
    pub groups_changed: usize,
    /// Groups that no longer have documents.
    pub groups_removed: usize,
    pub docs_added: u64,
    pub merged: bool,
    pub segments: usize,
}

/// Merge policy (spec section 4): more than 8 segments, more than 10%
/// deleted documents, or a forced merge.
fn wants_merge(index: &Index, force: bool) -> Result<bool> {
    if force {
        return Ok(true);
    }
    let metas = index.searchable_segment_metas()?;
    let docs: u64 = metas.iter().map(|m| m.max_doc() as u64).sum();
    let deleted: u64 = metas.iter().map(|m| m.num_deleted_docs() as u64).sum();
    Ok(metas.len() > 8 || (docs > 0 && deleted * 10 > docs))
}

/// Build the index in `dir` from scratch (when absent, when `full`, or when
/// the last build's group hashes are missing), or update it: the search SQL
/// runs over the whole catalog, every group's rows are hashed, and the
/// groups whose hash changed, appeared or vanished since the last build are
/// replaced (their documents deleted by record key, the new ones added as
/// one segment). This needs nothing from the catalog's change log, so a
/// derived document (a show built from several recordings) is replaced
/// whenever anything it shows changes.
pub fn update(
    dir: &Path,
    spec: &SearchSpec,
    c: &Connection,
    full: bool,
    force_merge: bool,
) -> Result<SearchUpdate> {
    let mut rep = SearchUpdate::default();
    let mut fresh = !dir.join("meta.json").exists();
    // A full build, or a spec whose fields changed, starts from an empty
    // directory: an index keeps the schema it was created with.
    if !fresh {
        let existing = serde_json::to_string(&Index::open_in_dir(dir)?.schema())?;
        if full || existing != serde_json::to_string(&schema(spec))? {
            std::fs::remove_dir_all(dir)?;
            fresh = true;
        }
    }
    std::fs::create_dir_all(dir)?;
    let gpath = groups_path(dir);
    let old = if fresh || full {
        None
    } else {
        load_groups(&gpath)
    };
    let schema = schema(spec);
    let index = if fresh {
        Index::create_in_dir(dir, schema.clone())?
    } else {
        Index::open_in_dir(dir)?
    };
    index.tokenizers().register(FOLD, fold_analyzer());
    let schema = index.schema();
    // One thread: documents keep the SQL's order (doc ids follow it).
    let mut w: IndexWriter = index.writer_with_num_threads(1, 400_000_000)?;
    let mut new: HashMap<String, u64> = HashMap::new();
    match old {
        None => {
            if !fresh {
                w.delete_all_documents()?;
            }
            rep.full_build = true;
            scan(spec, c, None, |g, r, cols| {
                let h = new.entry(g.to_string()).or_insert(0);
                *h = h.wrapping_add(row_hash(r, cols)?);
                w.add_document(make_doc(&schema, g, r, cols)?)?;
                rep.docs_added += 1;
                Ok(())
            })?;
            rep.groups_changed = new.len();
            w.commit()?;
            let ids = index.searchable_segment_ids()?;
            if ids.len() > 1 {
                w.merge(&ids).wait()?;
                rep.merged = true;
            }
        }
        Some(old) => {
            scan(spec, c, None, |g, r, cols| {
                let h = new.entry(g.to_string()).or_insert(0);
                *h = h.wrapping_add(row_hash(r, cols)?);
                Ok(())
            })?;
            let mut changed: Vec<String> = new
                .iter()
                .filter(|(g, h)| old.get(*g) != Some(h))
                .map(|(g, _)| g.clone())
                .collect();
            let removed: Vec<&String> = old.keys().filter(|g| !new.contains_key(*g)).collect();
            changed.sort();
            rep.groups_changed = changed.len();
            rep.groups_removed = removed.len();
            if !changed.is_empty() || !removed.is_empty() {
                let rk = schema.get_field("record_key")?;
                for k in changed.iter().chain(removed.iter().copied()) {
                    let t = Term::from_field_u64(rk, record_key(k));
                    w.delete_query(Box::new(RangeQuery::new(
                        Bound::Included(t.clone()),
                        Bound::Included(t),
                    )))?;
                }
                for batch in changed.chunks(500) {
                    scan(spec, c, Some(batch), |g, r, cols| {
                        w.add_document(make_doc(&schema, g, r, cols)?)?;
                        rep.docs_added += 1;
                        Ok(())
                    })?;
                }
                w.commit()?;
            }
            if wants_merge(&index, force_merge)? {
                let ids = index.searchable_segment_ids()?;
                if ids.len() > 1 || force_merge {
                    w.merge(&ids).wait()?;
                    rep.merged = true;
                }
            }
        }
    }
    w.garbage_collect_files().wait()?;
    w.wait_merging_threads()?;
    save_groups(&gpath, &new)?;
    rep.segments = index.searchable_segment_ids()?.len();
    Ok(rep)
}

// ---------- publishing the index files ----------

pub use strata_core::search::{
    decode_doc_ids, field_infos, fold_analyzer, put_varint, record_key, schema, DelRef, Field,
    FieldInfo, FileRef, KeysRef, Kind, SearchRef, SearchSpec, SegmentObj, SegmentRef, FOLD,
};

/// Chunk the index in `dir` into content-addressed objects (`put`), build
/// each segment's hot pack, delete object, key map (nodes written by `w`)
/// and segment object, and return the version's search reference
/// ([`strata_core::search::publish_index`] over the directory's files).
pub fn publish_files(
    dir: &Path,
    chunk_size: u64,
    put: &mut dyn FnMut(&str, &[u8]) -> Result<()>,
    w: &mut crate::prolly::Writer,
) -> Result<SearchRef> {
    let _ = tantivy::directory::MmapDirectory::open(dir)?; // validates the directory
    let mut files = BTreeMap::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name().to_string_lossy().to_string();
        if !name.ends_with(".lock") {
            files.insert(name.clone(), std::fs::read(dir.join(&name))?);
        }
    }
    strata_core::search::publish_index(files, chunk_size, put, w)
}
