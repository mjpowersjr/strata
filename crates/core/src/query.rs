//! Reading a published version (docs/format.md section 8): search
//! (BM25 with the published boosts, filters, year ranges, prefix and typo
//! expansion, facet counts, stored fields) and store lookups. Independent of
//! how bytes arrive: the caller opens the tantivy index over its own
//! `Directory` and gives a tree fetcher. Natively those fetch in parallel on
//! threads; in a browser they serve fetched chunks and record what is
//! missing, so the page fetches it and runs the query again.

use crate::keys::{self, ValueRef};
use crate::prolly::{parse_hash, Reader as Nodes, Root};
use crate::search::fold_analyzer;
use crate::version::{MapRef, Version};
use anyhow::{bail, Context, Result};
use futures::future::{join_all, BoxFuture, FutureExt};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::ops::Bound;
use std::sync::Arc;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{
    BooleanQuery, BoostQuery, FuzzyTermQuery, Occur, Query, RangeQuery, TermQuery,
};
use tantivy::schema::{IndexRecordOption, Schema, Value};
use tantivy::{Index, Searcher, TantivyDocument, Term};

/// How the text of a query is matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Exact tokens, and the last token also as a prefix (as-you-type).
    #[default]
    Auto,
    Exact,
    Prefix,
    /// Tokens of 4+ letters within edit distance 1.
    Fuzzy,
    /// Any of the tokens (the last also as a prefix); documents matching
    /// more of them score higher. The fallback when no document has them
    /// all, e.g. a performer plus a title the collection doesn't hold.
    Any,
}

/// A search request.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(default)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub mode: Mode,
    /// Exact keyword filters: (field, value).
    pub terms: Vec<(String, String)>,
    /// Year range (inclusive), and whether undated documents are included.
    pub years: Option<(i64, i64, bool)>,
    pub limit: usize,
    /// Facet counts over all matches: (field, how many values). A keyword
    /// field, or `year` (undated documents count as "undated").
    pub facets: Vec<(String, usize)>,
    /// Optional text fields to search as well (e.g. `transcript`).
    pub also: Vec<String>,
}

/// A hit: its score and stored fields.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Hit {
    pub score: f32,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchResult {
    pub total: usize,
    pub hits: Vec<Hit>,
    /// The mode actually used (typo matching kicks in as a fallback).
    pub mode: String,
    /// Per requested facet: (value, matching documents), most first.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: BTreeMap<String, Vec<(String, u64)>>,
}

/// Most frequent expansions kept per field and token (spike section 12).
pub const EXPANSION_CAP: usize = 32;

/// `DfaWrapper` is not `Clone`; dictionary searches need it shared.
#[derive(Clone)]
struct SharedDfa(Arc<tantivy::query::DfaWrapper>);

impl tantivy_fst::Automaton for SharedDfa {
    type State = <tantivy::query::DfaWrapper as tantivy_fst::Automaton>::State;
    fn start(&self) -> Self::State {
        self.0.start()
    }
    fn is_match(&self, s: &Self::State) -> bool {
        self.0.is_match(s)
    }
    fn can_match(&self, s: &Self::State) -> bool {
        self.0.can_match(s)
    }
    fn accept(&self, s: &Self::State, b: u8) -> Self::State {
        self.0.accept(s, b)
    }
}

/// An opened version: the search index and the store.
pub struct Opened {
    pub version: Version,
    pub index: Index,
    pub searcher: Searcher,
    pub nodes: Nodes,
    /// Text fields and their boosts (from the version object).
    pub text_fields: Vec<(String, f32)>,
    /// Text fields searched only when a query names them in `also`.
    pub optional_fields: Vec<(String, f32)>,
}

impl Opened {
    /// `index` is open (tokenizers registered by this call); `nodes` fetches
    /// tree nodes.
    pub fn new(version: Version, index: Index, nodes: Nodes) -> Result<Self> {
        index
            .tokenizers()
            .register(crate::search::FOLD, fold_analyzer());
        let searcher = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?
            .searcher();
        let of_kind = |k: &str| -> Vec<(String, f32)> {
            version
                .search
                .fields
                .iter()
                .filter(|f| f.kind == k)
                .map(|f| (f.name.clone(), if f.boost > 0.0 { f.boost } else { 1.0 }))
                .collect()
        };
        let text_fields = of_kind("text");
        let optional_fields = of_kind("text_optional");
        Ok(Self {
            version,
            index,
            searcher,
            nodes,
            text_fields,
            optional_fields,
        })
    }

    fn schema(&self) -> Schema {
        self.index.schema()
    }

    fn tokens(s: &str) -> Vec<String> {
        let mut a = fold_analyzer();
        let mut ts = a.token_stream(s);
        let mut out = vec![];
        while ts.advance() {
            out.push(ts.token().text.clone());
        }
        out
    }

    /// Prefix and typo expansions per (text field, token index), the most
    /// frequent [`EXPANSION_CAP`] each; dictionary blocks fetched together.
    /// The text fields a query searches: the default ones, plus the optional
    /// ones it names.
    fn fields_for(&self, q: &SearchQuery) -> Vec<(String, f32)> {
        let mut v = self.text_fields.clone();
        v.extend(
            self.optional_fields
                .iter()
                .filter(|(n, _)| q.also.contains(n))
                .cloned(),
        );
        v
    }

    fn expand(
        &self,
        fields: &[(String, f32)],
        toks: &[String],
        mode: Mode,
    ) -> Result<HashMap<(String, usize), Vec<String>>> {
        let schema = self.schema();
        let last = toks.len().saturating_sub(1);
        let mut out: HashMap<(String, usize), Vec<String>> = HashMap::new();
        // Errors (reads not fetched yet) are kept until every expansion has
        // been tried, so one pass records all the blocks it needs.
        let mut first_err: Option<anyhow::Error> = None;
        for seg in self.searcher.segment_readers() {
            for (name, _) in fields {
                let field = schema.get_field(name)?;
                let inv = seg.inverted_index(field)?;
                for (i, t) in toks.iter().enumerate() {
                    let prefix = i == last && matches!(mode, Mode::Prefix | Mode::Auto | Mode::Any);
                    let fuzzy = mode == Mode::Fuzzy && t.chars().count() >= 4;
                    if !prefix && !fuzzy {
                        continue;
                    }
                    let mut found: Vec<(String, u32)> = vec![];
                    if prefix {
                        let lo = t.as_bytes().to_vec();
                        let mut hi = lo.clone();
                        hi.push(0xff);
                        let mut st = match futures::executor::block_on(
                            inv.terms().range().ge(&lo).lt(&hi).into_stream_async(),
                        ) {
                            Ok(st) => st,
                            Err(e) => {
                                first_err.get_or_insert(e.into());
                                continue;
                            }
                        };
                        while st.advance() {
                            if let Ok(k) = std::str::from_utf8(st.key()) {
                                found.push((k.to_string(), st.value().doc_freq));
                            }
                        }
                    }
                    if fuzzy {
                        let dfa = SharedDfa(Arc::new(
                            FuzzyTermQuery::new(Term::from_field_text(field, t), 1, true)
                                .automaton()?,
                        ));
                        let mut st = match futures::executor::block_on(
                            inv.terms().search(dfa).into_stream_async(),
                        ) {
                            Ok(st) => st,
                            Err(e) => {
                                first_err.get_or_insert(e.into());
                                continue;
                            }
                        };
                        while st.advance() {
                            if let Ok(k) = std::str::from_utf8(st.key()) {
                                found.push((k.to_string(), st.value().doc_freq));
                            }
                        }
                    }
                    found.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                    found.truncate(EXPANSION_CAP);
                    out.entry((name.clone(), i))
                        .or_default()
                        .extend(found.into_iter().map(|x| x.0));
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(out)
    }

    fn build(
        &self,
        q: &SearchQuery,
        toks: &[String],
        ex: &HashMap<(String, usize), Vec<String>>,
        mode: Mode,
    ) -> Result<Box<dyn Query>> {
        let schema = self.schema();
        let mut must: Vec<(Occur, Box<dyn Query>)> = vec![];
        let mut words: Vec<(Occur, Box<dyn Query>)> = vec![];
        for (i, t) in toks.iter().enumerate() {
            let mut any: Vec<(Occur, Box<dyn Query>)> = vec![];
            for (name, boost) in &self.fields_for(q) {
                let f = schema.get_field(name)?;
                let exact = Box::new(TermQuery::new(
                    Term::from_field_text(f, t),
                    IndexRecordOption::WithFreqs,
                )) as Box<dyn Query>;
                let inner: Box<dyn Query> = match ex.get(&(name.clone(), i)) {
                    Some(list) => {
                        let w = if mode == Mode::Auto { 0.5 } else { 0.6 };
                        let mut alts: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Should, exact)];
                        for k in list.iter().filter(|k| *k != t) {
                            let tq = TermQuery::new(
                                Term::from_field_text(f, k),
                                IndexRecordOption::WithFreqs,
                            );
                            alts.push((Occur::Should, Box::new(BoostQuery::new(Box::new(tq), w))));
                        }
                        Box::new(BooleanQuery::new(alts))
                    }
                    None => exact,
                };
                any.push((Occur::Should, Box::new(BoostQuery::new(inner, *boost))));
            }
            words.push((Occur::Should, Box::new(BooleanQuery::new(any))));
        }
        if mode == Mode::Any {
            if !words.is_empty() {
                must.push((Occur::Must, Box::new(BooleanQuery::new(words))));
            }
        } else {
            must.extend(words.into_iter().map(|(_, q)| (Occur::Must, q)));
        }
        for (field, v) in &q.terms {
            let f = schema
                .get_field(field)
                .with_context(|| format!("no field {field}"))?;
            must.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(f, v),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some((lo, hi, undated)) = q.years {
            let y = schema.get_field("year")?;
            let dated = schema.get_field("dated")?;
            let range: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (
                    Occur::Must,
                    Box::new(RangeQuery::new(
                        Bound::Included(Term::from_field_i64(y, lo)),
                        Bound::Included(Term::from_field_i64(y, hi)),
                    )) as Box<dyn Query>,
                ),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_bool(dated, true),
                        IndexRecordOption::Basic,
                    )),
                ),
            ]));
            if undated {
                let none = Box::new(TermQuery::new(
                    Term::from_field_bool(dated, false),
                    IndexRecordOption::Basic,
                ));
                must.push((
                    Occur::Must,
                    Box::new(BooleanQuery::new(vec![
                        (Occur::Should, range),
                        (Occur::Should, none),
                    ])),
                ));
            } else {
                must.push((Occur::Must, range));
            }
        }
        if must.is_empty() {
            bail!("empty query");
        }
        Ok(Box::new(BooleanQuery::new(must)))
    }

    /// Fetch the postings of every term the query will read, in parallel.
    fn warm(
        &self,
        toks: &[String],
        q: &SearchQuery,
        ex: &HashMap<(String, usize), Vec<String>>,
    ) -> Result<()> {
        let schema = self.schema();
        let mut terms: Vec<Term> = vec![];
        for (name, _) in &self.fields_for(q) {
            let f = schema.get_field(name)?;
            for t in toks {
                terms.push(Term::from_field_text(f, t));
            }
        }
        for ((name, _), list) in ex {
            let f = schema.get_field(name)?;
            terms.extend(list.iter().map(|k| Term::from_field_text(f, k)));
        }
        for (field, v) in &q.terms {
            terms.push(Term::from_field_text(schema.get_field(field)?, v));
        }
        if q.years.is_some() {
            let d = schema.get_field("dated")?;
            terms.push(Term::from_field_bool(d, true));
            terms.push(Term::from_field_bool(d, false));
        }
        let mut jobs: Vec<BoxFuture<'_, io::Result<()>>> = vec![];
        for seg in self.searcher.segment_readers() {
            for t in &terms {
                let inv = seg.inverted_index(t.field())?;
                let t = t.clone();
                jobs.push(async move { inv.warm_postings(&t, false).await.map(|_| ()) }.boxed());
            }
        }
        // Every read runs (none short-circuits the others), then the first
        // error, if any.
        for r in futures::executor::block_on(join_all(jobs)) {
            r?;
        }
        Ok(())
    }

    fn run(
        &self,
        q: &SearchQuery,
        mode: Mode,
    ) -> Result<(usize, Vec<(f32, tantivy::DocAddress)>, Box<dyn Query>)> {
        let toks = q.text.as_deref().map(Self::tokens).unwrap_or_default();
        let ex = if toks.is_empty() || mode == Mode::Exact {
            HashMap::new()
        } else {
            self.expand(&self.fields_for(q), &toks, mode)?
        };
        self.warm(&toks, q, &ex)?;
        let query = self.build(q, &toks, &ex, mode)?;
        let (top, n) = self.searcher.search(
            &*query,
            &(TopDocs::with_limit(q.limit.max(1)).order_by_score(), Count),
        )?;
        Ok((n, top, query))
    }

    /// Terms aggregations over the query's matches (columns are read whole
    /// on first use, then cached).
    fn facets(
        &self,
        query: &dyn Query,
        want: &[(String, usize)],
    ) -> Result<BTreeMap<String, Vec<(String, u64)>>> {
        use tantivy::aggregation::{agg_req::Aggregations, AggregationCollector};
        let mut req = serde_json::Map::new();
        for (f, n) in want {
            let field = if f == "year" { "year_f" } else { f.as_str() };
            anyhow::ensure!(self.schema().get_field(field).is_ok(), "no facet field {f}");
            req.insert(
                f.clone(),
                serde_json::json!({"terms": {"field": field, "size": (*n).max(1)}}),
            );
        }
        let aggs: Aggregations = serde_json::from_value(serde_json::Value::Object(req))?;
        let coll = AggregationCollector::from_aggs(aggs, Default::default());
        let res = self.searcher.search(query, &coll)?;
        let j = serde_json::to_value(&res)?;
        let mut out = BTreeMap::new();
        for (f, _) in want {
            let buckets = j[f]["buckets"].as_array().cloned().unwrap_or_default();
            let v = buckets
                .iter()
                .map(|b| {
                    let key = match &b["key"] {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => {
                            let x = n.as_f64().unwrap_or(0.0) as i64;
                            if f == "year" && x == 0 {
                                "undated".into()
                            } else {
                                x.to_string()
                            }
                        }
                        other => other.to_string(),
                    };
                    (key, b["doc_count"].as_u64().unwrap_or(0))
                })
                .collect();
            out.insert(f.clone(), v);
        }
        Ok(out)
    }

    /// Search: BM25 over the text fields with their boosts, filters as
    /// required clauses; typo matching as a fallback under 5 hits, then any
    /// of the words (mode `any`) when nothing has them all.
    pub fn search(&self, q: &SearchQuery) -> Result<SearchResult> {
        let (mut n, mut top, mut query) = self.run(q, q.mode)?;
        let mut mode = q.mode;
        if n < 5 && q.text.is_some() && matches!(q.mode, Mode::Auto | Mode::Exact) {
            let (n2, t2, q2) = self.run(q, Mode::Fuzzy)?;
            if n2 > n {
                (n, top, mode, query) = (n2, t2, Mode::Fuzzy, q2);
            }
        }
        let several_words = q.text.as_deref().map_or(0, |t| Self::tokens(t).len()) > 1;
        if n == 0 && several_words && q.mode != Mode::Any {
            (n, top, query) = self.run(q, Mode::Any)?;
            mode = Mode::Any;
        }
        let facets = if q.facets.is_empty() {
            BTreeMap::new()
        } else {
            self.facets(&*query, &q.facets)?
        };
        // Stored fields of the hits, read concurrently.
        let docs: Vec<TantivyDocument> = futures::executor::block_on(join_all(
            top.iter()
                .map(|(_, a)| self.searcher.doc_async::<TantivyDocument>(*a)),
        ))
        .into_iter()
        .collect::<Result<_, _>>()?;
        let schema = self.schema();
        let hits = top
            .iter()
            .zip(docs)
            .map(|((score, _), d)| {
                let mut fields = serde_json::Map::new();
                for (f, v) in d.field_values() {
                    let name = schema.get_field_name(f).to_string();
                    let j = if let Some(s) = v.as_str() {
                        serde_json::Value::from(s)
                    } else if let Some(x) = v.as_i64() {
                        x.into()
                    } else if let Some(x) = v.as_u64() {
                        x.into()
                    } else {
                        serde_json::Value::Null
                    };
                    fields.insert(name, j);
                }
                Hit {
                    score: *score,
                    fields,
                }
            })
            .collect();
        Ok(SearchResult {
            total: n,
            hits,
            mode: format!("{mode:?}").to_lowercase(),
            facets,
        })
    }

    fn map(&self, name: &str) -> Result<(&MapRef, Root)> {
        let m = self
            .version
            .store
            .maps
            .get(name)
            .with_context(|| format!("no map {name}"))?;
        Ok((
            m,
            Root {
                hash: parse_hash(&m.root)?,
                depth: m.depth,
                count: m.count,
                levels: vec![],
            },
        ))
    }

    /// A table row by its primary key values, as column → value.
    pub fn get(
        &self,
        table: &str,
        key: &[ValueRef<'_>],
    ) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
        let (m, root) = self.map(table)?;
        let k = keys::tuple(key);
        let got = self.nodes.get_many(&root, std::slice::from_ref(&k))?;
        Ok(match got.get(&k) {
            Some(v) => Some(row(m, v)?),
            None => None,
        })
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
        Ok(self.range_from(map, prefix, partial_text, None, limit)?.0)
    }

    /// [`range`](Self::range) starting at the raw key `from` (inclusive,
    /// clamped to the prefix's range): a page cursor (the `next` of the
    /// previous page) or a seek (any key, e.g. a random one). Also returns
    /// the cursor of the page after this one (`None` at the end).
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
        let (m, root) = self.map(map)?;
        let (mut lo, hi) = if prefix.is_empty() {
            (vec![], vec![0xFF; 1])
        } else {
            keys::prefix_range(prefix, partial_text)
        };
        if let Some(f) = from {
            if f > lo.as_slice() {
                lo = f.to_vec();
            }
        }
        let entries = self.nodes.range(&root, &lo, &hi, limit)?;
        // The next page starts just after this page's last key (when the
        // page was full; a short page is the end).
        let next = match (limit, entries.last()) {
            (Some(l), Some((k, _))) if entries.len() as u64 >= l => {
                let mut n = k.clone();
                n.push(0);
                Some(n)
            }
            _ => None,
        };
        let rows = self.rows_of(m, &entries)?;
        Ok((rows, next))
    }

    fn rows_of(
        &self,
        m: &MapRef,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
        match &m.of {
            None => entries.iter().map(|(_, v)| row(m, v)).collect(),
            Some(table) => {
                // Index map: keys end with the table's primary key.
                let (tm, troot) = self.map(table)?;
                let desc: Vec<bool> = m.key.iter().map(|c| c.ends_with(" desc")).collect();
                let npk = tm.key.len();
                let mut pks = vec![];
                for (k, _) in entries {
                    let parts = keys::decode(k, &desc).context("bad index key")?;
                    let pk = &parts[parts.len() - npk..];
                    let mut kk = vec![];
                    for p in pk {
                        match p {
                            keys::Part::Null => keys::push(&mut kk, ValueRef::Null, false),
                            keys::Part::Int(i) => keys::push(&mut kk, ValueRef::Integer(*i), false),
                            keys::Part::Real(f) => keys::push(&mut kk, ValueRef::Real(*f), false),
                            keys::Part::Text(t) => {
                                keys::push(&mut kk, ValueRef::Text(t.as_bytes()), false)
                            }
                        }
                    }
                    pks.push(kk);
                }
                let got = self.nodes.get_many(&troot, &pks)?;
                pks.iter()
                    .filter_map(|k| got.get(k))
                    .map(|v| row(tm, v))
                    .collect()
            }
        }
    }
}

fn row(m: &MapRef, v: &[u8]) -> Result<serde_json::Map<String, serde_json::Value>> {
    let vals: Vec<serde_json::Value> = serde_json::from_slice(v)?;
    Ok(m.columns.iter().cloned().zip(vals).collect())
}
