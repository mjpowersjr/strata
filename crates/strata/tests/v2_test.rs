//! v2 end to end: publish a small collection, serve it over HTTP, read it,
//! update it incrementally, follow, pin and sync, and go offline.

use strata_fetch::testing::TestServer;
use strata_fetch::{ChunkCache, ChunkSource, HttpSource};
use strata::publish::{publish, Profile};
use strata::reader::{Mode, SearchQuery, V2Index};
use strata::search::{Field, Kind, SearchSpec};
use strata::reader::ValueRef;
use rusqlite::Connection;
use std::path::Path;
use std::sync::Arc;

const N: i64 = 4000;

fn fixture(path: &Path) {
    let c = Connection::open(path).unwrap();
    c.execute_batch(
        "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
         CREATE TABLE items(id TEXT PRIMARY KEY, status TEXT, seq INTEGER);
         CREATE TABLE things(id TEXT PRIMARY KEY, name TEXT, year INTEGER, kind TEXT);
         CREATE INDEX things_kind ON things(kind);
         INSERT INTO meta VALUES ('seq', '1');",
    )
    .unwrap();
    let tx = c.unchecked_transaction().unwrap();
    for i in 0..N {
        let id = format!("t{i:05}");
        let kind = ["red", "green", "blue"][(i % 3) as usize];
        let name = format!(
            "thing number {i} {} {}",
            if i % 7 == 0 { "alpha" } else { "beta" },
            "x".repeat((i % 40) as usize)
        );
        let year: Option<i64> = if i % 10 == 0 {
            None
        } else {
            Some(1900 + i % 100)
        };
        tx.execute(
            "INSERT INTO things VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, name, year, kind],
        )
        .unwrap();
        tx.execute("INSERT INTO items VALUES (?1, 'active', 1)", [&id])
            .unwrap();
    }
    tx.commit().unwrap();
}

fn profile() -> Profile {
    let f = |column, kind, stored| Field {
        column,
        kind,
        stored,
        boost: 1.0,
    };
    Profile {
        index: "things",
        profile: "test",
        audio_base: "https://example.invalid/",
        attribution: vec![],
        tables: vec!["things"],
        extra_indexes: vec![],
        views: vec![],
        sync: vec![],
        search: SearchSpec {
            sql: "SELECT id AS grp, id, name, kind, year FROM things WHERE {WHERE} ORDER BY id",
            group_column: "id",
            fields: vec![
                f("id", Kind::Stored, true),
                f("name", Kind::Text, true),
                f("kind", Kind::Keyword, true),
                f("year", Kind::Year, false),
            ],
        },
    }
}

fn source(s: &TestServer) -> Arc<dyn ChunkSource> {
    Arc::new(
        HttpSource::new(&s.addr)
            .unwrap()
            .with_object_prefix("objects/"),
    )
}

fn q(text: &str) -> SearchQuery {
    SearchQuery {
        text: Some(text.into()),
        mode: Mode::Exact,
        limit: 10,
        ..Default::default()
    }
}

fn bump(master: &Path, ids: &[&str], new_name: &str) {
    let c = Connection::open(master).unwrap();
    for id in ids {
        c.execute(
            "UPDATE things SET name = ?2 WHERE id = ?1",
            rusqlite::params![id, new_name],
        )
        .unwrap();
        c.execute("UPDATE items SET seq = 2 WHERE id = ?1", [id])
            .unwrap();
    }
    c.execute("UPDATE meta SET value = '2' WHERE key = 'seq'", [])
        .unwrap();
}

#[test]
fn publish_read_update_follow_pin_offline() {
    let dir = tempfile::tempdir().unwrap();
    let (master, work, tree, state) = (
        dir.path().join("master.sqlite"),
        dir.path().join("work"),
        dir.path().join("tree"),
        dir.path().join("state"),
    );
    fixture(&master);
    let p = profile();
    let r1 = publish(&p, &master, &work, &tree, false, false).unwrap();
    assert!(r1.search.full_build && r1.objects_written > 10);
    let server = TestServer::start(&tree);
    let src = source(&server);
    let cache = ChunkCache::open(&dir.path().join("cache"), 1 << 30, 16 << 20).unwrap();

    // Read: search, filters, lookups.
    let ix = V2Index::open(src.clone(), cache.clone(), &state, "things").unwrap();
    let alpha = ix.search(&q("alpha")).unwrap();
    assert_eq!(alpha.total, (0..N).filter(|i| i % 7 == 0).count());
    assert!(alpha.hits[0].fields["name"]
        .as_str()
        .unwrap()
        .contains("alpha"));
    let red_alpha = ix
        .search(&SearchQuery {
            terms: vec![("kind".into(), "red".into())],
            ..q("alpha")
        })
        .unwrap();
    assert_eq!(
        red_alpha.total,
        (0..N).filter(|i| i % 7 == 0 && i % 3 == 0).count()
    );
    let dated = ix
        .search(&SearchQuery {
            years: Some((1950, 1959, false)),
            ..q("alpha")
        })
        .unwrap();
    assert_eq!(
        dated.total,
        (0..N)
            .filter(|i| i % 7 == 0 && i % 10 != 0 && (50..60).contains(&(i % 100)))
            .count()
    );
    let row = ix
        .get("things", &[ValueRef::Text(b"t00042")])
        .unwrap()
        .unwrap();
    assert_eq!(row["kind"], "red");
    let greens = ix
        .range("things_kind", &[ValueRef::Text(b"green")], false, Some(5))
        .unwrap();
    assert_eq!(greens.len(), 5);
    assert!(greens.iter().all(|r| r["kind"] == "green"));
    // Typo fallback (exact finds nothing).
    let typo = ix
        .search(&SearchQuery {
            mode: Mode::Auto,
            ..q("alpah")
        })
        .unwrap();
    assert_eq!(typo.mode, "fuzzy");
    assert!(typo.total > 0);
    // Nothing has every word: any of them, the fuller matches first.
    let partial = ix.search(&q("alpha qqqq")).unwrap();
    assert_eq!(partial.mode, "any");
    assert_eq!(partial.total, (0..N).filter(|i| i % 7 == 0).count());
    let mixed = ix.search(&q("alpha number qqqq")).unwrap();
    assert_eq!(mixed.mode, "any");
    assert_eq!(mixed.total, N as usize);
    assert!(mixed.hits[0].fields["name"].as_str().unwrap().contains("alpha"));

    // Pin the whole version.
    let pin1 = ix.pin().unwrap();
    assert!(pin1.objects > 10);

    // An incremental update: 3 changed records.
    bump(&master, &["t00001", "t00002", "t00003"], "renamed gamma");
    let r2 = publish(&p, &master, &work, &tree, false, false).unwrap();
    assert!(!r2.search.full_build && r2.search.groups_changed == 3 && r2.search.segments == 2);
    // A small fixed set: the new segment's files and hot pack, a delete
    // list, meta.json, the changed tree nodes and the version object.
    assert!(
        r2.objects_written <= 20,
        "update wrote {} of {}",
        r2.objects_written,
        r1.objects_written
    );

    // Follow: the update is seen, the new version answers, and moving the
    // pin fetches only the new objects.
    let m = ix.check_update().unwrap().expect("update visible");
    let ix2 = ix.reopen(m, &state).unwrap();
    assert_eq!(ix2.search(&q("gamma")).unwrap().total, 3);
    assert_eq!(
        ix2.search(&q("alpha")).unwrap().total,
        alpha.total,
        "unchanged documents still found once"
    );
    let sync = ix.sync_pin(&ix2).unwrap();
    assert!(
        sync.fetched <= 20 && sync.fetched < pin1.objects,
        "sync fetched {} of {}",
        sync.fetched,
        pin1.objects
    );
    assert!(ix2.check_update().unwrap().is_none());

    // Offline: the last opened version, from the pinned cache.
    server.stop();
    std::thread::sleep(std::time::Duration::from_millis(100));
    let off = V2Index::open_cached(src.clone(), cache.clone(), &state, "things").unwrap();
    assert_eq!(off.manifest.seq, 2);
    assert_eq!(off.search(&q("gamma")).unwrap().total, 3);
    assert_eq!(
        off.get("things", &[ValueRef::Text(b"t03999")])
            .unwrap()
            .unwrap()["id"],
        "t03999"
    );
}

#[test]
fn offline_without_a_pin_fails_cleanly_for_uncached_reads() {
    let dir = tempfile::tempdir().unwrap();
    let (master, work, tree, state) = (
        dir.path().join("master.sqlite"),
        dir.path().join("work"),
        dir.path().join("tree"),
        dir.path().join("state"),
    );
    fixture(&master);
    publish(&profile(), &master, &work, &tree, false, false).unwrap();
    let server = TestServer::start(&tree);
    let cache = ChunkCache::open(&dir.path().join("cache"), 1 << 30, 16 << 20).unwrap();
    let ix = V2Index::open(source(&server), cache.clone(), &state, "things").unwrap();
    ix.get("things", &[ValueRef::Text(b"t00000")]).unwrap();
    server.stop();
    std::thread::sleep(std::time::Duration::from_millis(100));
    let off = V2Index::open_cached(source(&server), cache, &state, "things").unwrap();
    assert!(
        off.get("things", &[ValueRef::Text(b"t00000")])
            .unwrap()
            .is_some(),
        "cached row answers"
    );
    assert!(
        off.get("things", &[ValueRef::Text(b"t03999")]).is_err(),
        "an uncached leaf is an error, not a wrong answer"
    );
}

/// A record that disappears from the master (a withdrawal: no change-log
/// entry needed) loses its search documents on the next publish, and an
/// unchanged re-run touches nothing.
#[test]
fn vanished_groups_leave_the_search_index() {
    let dir = tempfile::tempdir().unwrap();
    let (master, work, tree, state) = (
        dir.path().join("master.sqlite"),
        dir.path().join("work"),
        dir.path().join("tree"),
        dir.path().join("state"),
    );
    fixture(&master);
    publish(&profile(), &master, &work, &tree, false, false).unwrap();
    {
        let c = Connection::open(&master).unwrap();
        c.execute("DELETE FROM things WHERE id IN ('t00007', 't00014')", [])
            .unwrap();
        c.execute("UPDATE meta SET value = '2' WHERE key = 'seq'", [])
            .unwrap();
    }
    let r = publish(&profile(), &master, &work, &tree, false, false).unwrap();
    assert_eq!(
        (
            r.search.groups_changed,
            r.search.groups_removed,
            r.search.docs_added
        ),
        (0, 2, 0)
    );
    let server = TestServer::start(&tree);
    let cache = ChunkCache::open(&dir.path().join("cache"), 1 << 30, 16 << 20).unwrap();
    let ix = V2Index::open(source(&server), cache, &state, "things").unwrap();
    let alpha = ix
        .search(&SearchQuery {
            limit: 1000,
            ..q("alpha")
        })
        .unwrap();
    assert_eq!(alpha.total, (N as usize).div_ceil(7) - 2);
    assert!(ix
        .get("things", &[ValueRef::Text(b"t00007")])
        .unwrap()
        .is_none());
    {
        let c = Connection::open(&master).unwrap();
        c.execute("UPDATE meta SET value = '3' WHERE key = 'seq'", [])
            .unwrap();
    }
    let r = publish(&profile(), &master, &work, &tree, false, false).unwrap();
    assert_eq!((r.search.groups_changed, r.search.groups_removed), (0, 0));
    assert_eq!(r.search.segments, 1);
}

/// Each segment object carries a record key -> doc ids map covering every
/// group once, and the version object no longer grows with the files.
#[test]
fn segment_objects_carry_the_key_map() {
    use strata::prolly::{parse_hash, Reader, Root};
    use strata::search::{decode_doc_ids, record_key, SegmentObj};
    use strata::version::{Manifest, Version};
    let dir = tempfile::tempdir().unwrap();
    let (master, work, tree) = (
        dir.path().join("master.sqlite"),
        dir.path().join("work"),
        dir.path().join("tree"),
    );
    fixture(&master);
    publish(&profile(), &master, &work, &tree, false, false).unwrap();
    let objs = tree.join("objects");
    let m: Manifest =
        serde_json::from_slice(&std::fs::read(tree.join("things/manifest.json")).unwrap()).unwrap();
    let read = |sha: &str| {
        strata_object::decode_verified(&std::fs::read(objs.join(sha)).unwrap(), sha, 0).unwrap()
    };
    // Every object decodes to bytes matching its name.
    for e in std::fs::read_dir(&objs).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().into_string().unwrap();
        strata_object::decode_verified(&std::fs::read(e.path()).unwrap(), &name, 0)
            .unwrap_or_else(|err| panic!("{name}: {err}"));
    }
    let v: Version = serde_json::from_slice(&read(&m.version.sha256)).unwrap();
    assert_eq!(v.search.segments.len(), 1);
    assert!(v
        .search
        .files
        .keys()
        .all(|n| !n.starts_with(&v.search.segments[0].id)));
    let seg = &v.search.segments[0];
    assert!(seg.del.is_none());
    let obj: SegmentObj = serde_json::from_slice(&read(&seg.object.sha256)).unwrap();
    assert!(!obj.files.is_empty() && !obj.hot_parts.is_empty());
    let root = Root {
        hash: parse_hash(&obj.keys.root).unwrap(),
        depth: obj.keys.depth,
        count: obj.keys.count,
        levels: obj.keys.levels.clone(),
    };
    assert_eq!(root.count, N as u64);
    let r = Reader::new(&objs);
    let all = r.range(&root, &[], &[0xff; 9], None).unwrap();
    assert_eq!(all.len(), N as usize);
    let mut docs: Vec<u32> = all.iter().flat_map(|(_, v)| decode_doc_ids(v)).collect();
    docs.sort();
    assert_eq!(docs, (0..N as u32).collect::<Vec<_>>(), "every doc once");
    // Documents follow the SQL's order (by id), so t00005 is doc 5.
    let k = record_key("t00005").to_be_bytes().to_vec();
    let got = r.get_many(&root, &[k.clone()]).unwrap();
    assert_eq!(decode_doc_ids(&got[&k]), vec![5]);
}

/// Facet counts over all matches, not only the returned hits.
#[test]
fn facets_count_every_match() {
    let dir = tempfile::tempdir().unwrap();
    let (master, work, tree, state) = (
        dir.path().join("master.sqlite"),
        dir.path().join("work"),
        dir.path().join("tree"),
        dir.path().join("state"),
    );
    fixture(&master);
    publish(&profile(), &master, &work, &tree, false, false).unwrap();
    let server = TestServer::start(&tree);
    let cache = ChunkCache::open(&dir.path().join("cache"), 1 << 30, 16 << 20).unwrap();
    let ix = V2Index::open(source(&server), cache, &state, "things").unwrap();
    let r = ix
        .search(&SearchQuery {
            limit: 3,
            facets: vec![("kind".into(), 10), ("year".into(), 200)],
            ..q("alpha")
        })
        .unwrap();
    let alpha: Vec<i64> = (0..N).filter(|i| i % 7 == 0).collect();
    assert_eq!(r.total, alpha.len());
    assert_eq!(r.hits.len(), 3);
    let kinds: std::collections::BTreeMap<String, u64> = r.facets["kind"].iter().cloned().collect();
    for (k, name) in ["red", "green", "blue"].iter().enumerate() {
        let want = alpha.iter().filter(|i| (*i % 3) as usize == k).count() as u64;
        assert_eq!(kinds[*name], want, "{name}");
    }
    let years: std::collections::BTreeMap<String, u64> = r.facets["year"].iter().cloned().collect();
    let undated = alpha.iter().filter(|i| *i % 10 == 0).count() as u64;
    assert_eq!(years["undated"], undated);
    assert_eq!(years.values().sum::<u64>(), alpha.len() as u64);
    assert_eq!(
        years["1907"],
        alpha
            .iter()
            .filter(|i| *i % 10 != 0 && 1900 + *i % 100 == 1907)
            .count() as u64
    );
}
