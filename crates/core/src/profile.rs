//! What a collection publishes in v2 (docs/format.md section 6), and how
//! each store table follows changes in an incremental update (section 7).

use crate::search::SearchSpec;

/// An extra ordering over a table (beyond its SQLite indexes), such as
/// "series by number of episodes, descending".
#[derive(Debug, Clone)]
pub struct ExtraIndex {
    pub name: &'static str,
    pub table: &'static str,
    /// Columns, each optionally followed by " desc".
    pub columns: &'static [&'static str],
}

/// A store table defined by a query over the catalog (a directory listing,
/// a summary, a join the readers would otherwise make row by row): it is
/// materialized as a temporary table at publish time, then published like
/// any table, keyed by `key`, with an ordering per `indexes` entry.
#[derive(Debug, Clone)]
pub struct View {
    pub name: &'static str,
    pub sql: &'static str,
    pub key: &'static [&'static str],
    /// (map name, columns; each optionally followed by " desc").
    pub indexes: &'static [(&'static str, &'static [&'static str])],
}

/// How a changed group's old rows are found in a table map: the first key
/// component equals the group, or is text starting with `{group}{sep}`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GroupKey {
    Exact,
    Prefix(&'static str),
}

/// One part of a derived table's key, taken from a source table's row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KeyPart {
    Lit(&'static str),
    Col(&'static str),
}

/// A source of derived keys: rows of `table` (old and new) dirty the key
/// built from `key` (skipped when a column is NULL).
#[derive(Debug, Clone)]
pub struct KeySource {
    pub table: &'static str,
    pub key: Vec<KeyPart>,
}

/// How an incremental update finds a table's changed rows.
#[derive(Debug, Clone)]
pub enum TableSync {
    /// Rows belong to groups (items). New rows: the map's columns `FROM
    /// table WHERE {where_groups}`, where `{G}` becomes the changed groups'
    /// placeholders. Old rows: the map entries under each group's key.
    ByGroup {
        where_groups: &'static str,
        old: GroupKey,
    },
    /// Rows keyed by values found in other tables (facets): the keys that
    /// changed rows of `sources` (old and new) touch, then new rows by
    /// primary key.
    Derived { sources: Vec<KeySource> },
}

pub struct Profile {
    pub index: &'static str,
    pub profile: &'static str,
    pub audio_base: &'static str,
    pub attribution: Vec<String>,
    pub tables: Vec<&'static str>,
    pub extra_indexes: Vec<ExtraIndex>,
    /// Query-defined tables, published after `tables`.
    pub views: Vec<View>,
    pub search: SearchSpec,
    /// Per table, how an incremental update follows it (in this order:
    /// derived tables after their sources). Empty: the collection is
    /// published only by full builds.
    pub sync: Vec<(&'static str, TableSync)>,
}

/// The master's sequence number (one value).
pub const SEQ_SQL: &str = "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'seq'";
/// Items changed after a sequence number (`?1`), oldest first, at most
/// `?2`: engine ids (`{item}` or `{item}:{sub}`) and their seq.
pub const CHANGES_SQL: &str = "SELECT id, seq FROM items WHERE seq > ?1 ORDER BY seq LIMIT ?2";
/// Active records (the version's `records`).
pub const RECORDS_SQL: &str = "SELECT count(*) FROM items WHERE status = 'active'";
