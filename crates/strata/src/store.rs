//! The record store: every table of the master catalog (or a profile's
//! chosen subset) as a prolly-tree map from its primary key to its row, plus
//! one map per SQLite index and per extra ordering (docs/format.md section 5).
//!
//! Maps are rebuilt from the catalog at every publish. Prolly trees are
//! history-independent, so a rebuild gives exactly the tree an incremental
//! edit would; only nodes not already published are new.

use crate::keys;
use crate::prolly::{build, hash_hex, Writer};
use anyhow::{Context, Result};
pub use strata_core::version::{MapRef, StoreRef};
use rusqlite::{types::ValueRef, Connection};
use std::collections::BTreeMap;

/// An ordering SQLite has no index for (for example "series by number of
/// episodes, descending").
pub use strata_core::profile::{ExtraIndex, View};

fn columns(
    c: &Connection,
    table: &str,
    key: Option<&[&str]>,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut st = c.prepare("SELECT name, pk FROM pragma_table_info(?1) ORDER BY cid")?;
    let rows: Vec<(String, i64)> = st
        .query_map([table], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;
    anyhow::ensure!(!rows.is_empty(), "no table {table}");
    let all: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let mut pk: Vec<(i64, String)> = rows
        .iter()
        .filter(|r| r.1 > 0)
        .map(|r| (r.1, r.0.clone()))
        .collect();
    pk.sort();
    let pk: Vec<String> = if let Some(k) = key {
        for c in k {
            anyhow::ensure!(all.iter().any(|a| a == c), "{table}: no key column {c}");
        }
        k.iter().map(|c| c.to_string()).collect()
    } else if pk.is_empty() {
        // No primary key: key by every column (stable, unlike rowid).
        all.clone()
    } else {
        pk.into_iter().map(|p| p.1).collect()
    };
    Ok((all, pk))
}

fn sqlite_indexes(c: &Connection, table: &str) -> Result<Vec<(String, Vec<String>)>> {
    // Temporary tables (materialized views) keep their indexes in temp's schema.
    let mut st = c.prepare(
        "SELECT name FROM (SELECT name, tbl_name, type, sql FROM sqlite_master
                           UNION ALL SELECT name, tbl_name, type, sql FROM sqlite_temp_master)
         WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL ORDER BY name",
    )?;
    let names: Vec<String> = st
        .query_map([table], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    let mut out = vec![];
    for n in names {
        // `index_xinfo` keeps the sort direction (`index_info` drops DESC):
        // key columns only (`key` = 1), "col desc" when descending.
        let mut st = c.prepare(
            "SELECT name, \"desc\" FROM pragma_index_xinfo(?1) WHERE key = 1 ORDER BY seqno",
        )?;
        let cols: Vec<String> = st
            .query_map([&n], |r| {
                let (name, desc): (String, i64) = (r.get(0)?, r.get(1)?);
                Ok(if desc == 1 {
                    format!("{name} desc")
                } else {
                    name
                })
            })?
            .collect::<Result<_, _>>()?;
        out.push((n, cols));
    }
    Ok(out)
}

fn quote(c: &str) -> String {
    format!("\"{}\"", c.replace('"', "\"\""))
}

fn json(v: ValueRef<'_>) -> serde_json::Value {
    keys::row_json(keys::conv(v))
}

/// Build (or rebuild) the maps of `tables` and their indexes, writing new
/// nodes through `w`.
pub fn build_store(
    c: &Connection,
    tables: &[&str],
    extra: &[ExtraIndex],
    views: &[View],
    node_target: usize,
    w: &mut Writer,
) -> Result<StoreRef> {
    // Views become temporary tables (allowed on a read-only connection),
    // with their orderings as temporary indexes.
    for v in views {
        c.execute_batch(&format!(
            "DROP TABLE IF EXISTS temp.{n}; CREATE TEMP TABLE {n} AS {sql};",
            n = quote(v.name),
            sql = v.sql
        ))
        .with_context(|| format!("view {}", v.name))?;
        for (iname, cols) in v.indexes {
            let cols: Vec<String> = cols
                .iter()
                .map(|c| match c.strip_suffix(" desc") {
                    Some(n) => format!("{} DESC", quote(n)),
                    None => quote(c),
                })
                .collect();
            c.execute_batch(&format!(
                "CREATE INDEX temp.{} ON {}({})",
                quote(iname),
                quote(v.name),
                cols.join(", ")
            ))?;
        }
    }
    let all_tables: Vec<(&str, Option<&[&str]>)> = tables
        .iter()
        .map(|t| (*t, None))
        .chain(views.iter().map(|v| (v.name, Some(v.key))))
        .collect();
    let mut maps = BTreeMap::new();
    for (table, key) in all_tables {
        let (all, pk) = columns(c, table, key)?;
        // Table map: pk -> row (all columns, as a JSON array).
        let sel = all.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ");
        let pk_idx: Vec<usize> = pk
            .iter()
            .map(|p| all.iter().position(|c| c == p).unwrap())
            .collect();
        let mut st = c.prepare(&format!("SELECT {sel} FROM {}", quote(table)))?;
        let mut rows = st.query([])?;
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        while let Some(r) = rows.next()? {
            let key = keys::tuple(
                &pk_idx
                    .iter()
                    .map(|&i| r.get_ref(i))
                    .collect::<Result<Vec<_>, _>>()?,
            );
            let vals: Vec<serde_json::Value> = (0..all.len())
                .map(|i| r.get_ref(i).map(json))
                .collect::<Result<_, _>>()?;
            entries.push((key, serde_json::to_vec(&vals)?));
        }
        entries.sort();
        entries.dedup_by(|a, b| a.0 == b.0);
        let n = entries.len() as u64;
        let root = build(entries, node_target, w).with_context(|| table.to_string())?;
        maps.insert(
            table.to_string(),
            MapRef {
                root: hash_hex(&root.hash),
                depth: root.depth,
                count: n,
                levels: root.levels.clone(),
                key: pk.clone(),
                columns: all.clone(),
                of: None,
            },
        );
        // Index maps: (index columns..., pk...) -> "".
        let mut idx: Vec<(String, Vec<String>)> = sqlite_indexes(c, table)?;
        for e in extra.iter().filter(|e| e.table == table) {
            idx.push((
                e.name.to_string(),
                e.columns.iter().map(|s| s.to_string()).collect(),
            ));
        }
        for (name, cols) in idx {
            let parsed: Vec<(String, bool)> = cols
                .iter()
                .map(|c| match c.strip_suffix(" desc") {
                    Some(n) => (n.to_string(), true),
                    None => (c.clone(), false),
                })
                .collect();
            let sel = parsed
                .iter()
                .map(|(c, _)| quote(c))
                .chain(pk.iter().map(|p| quote(p)))
                .collect::<Vec<_>>()
                .join(", ");
            let mut st = c.prepare(&format!("SELECT {sel} FROM {}", quote(table)))?;
            let mut rows = st.query([])?;
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = vec![];
            while let Some(r) = rows.next()? {
                let mut k = vec![];
                for (i, (_, desc)) in parsed.iter().enumerate() {
                    keys::push(&mut k, r.get_ref(i)?, *desc);
                }
                for j in 0..pk.len() {
                    keys::push(&mut k, r.get_ref(parsed.len() + j)?, false);
                }
                entries.push((k, vec![]));
            }
            entries.sort();
            entries.dedup_by(|a, b| a.0 == b.0);
            let n = entries.len() as u64;
            let root = build(entries, node_target, w).with_context(|| name.clone())?;
            let mut key: Vec<String> = cols.clone();
            key.extend(pk.iter().cloned());
            maps.insert(
                name,
                MapRef {
                    root: hash_hex(&root.hash),
                    depth: root.depth,
                    count: n,
                    levels: root.levels.clone(),
                    key,
                    columns: vec![],
                    of: Some(table.to_string()),
                },
            );
        }
    }
    Ok(StoreRef { node_target, maps })
}
