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

pub struct Profile {
    pub index: &'static str,
    pub profile: &'static str,
    /// Base URL the rows' files are relative to (`Version::files_base`).
    pub files_base: &'static str,
    pub attribution: Vec<String>,
    pub tables: Vec<&'static str>,
    pub extra_indexes: Vec<ExtraIndex>,
    /// Query-defined tables, published after `tables`.
    pub views: Vec<View>,
    pub search: SearchSpec,
}
