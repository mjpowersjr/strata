//! The client end to end over a small published index served by HTTP:
//! paging, plans, progress and cancel, typed errors, counts, sync of a kept
//! copy, two libraries on one folder, a fallback source and membership.

use rusqlite::Connection;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use strata::publish::{publish, Profile};
use strata::search::{Field, Kind, SearchSpec};
use strata_client::{
    error_kind, ChunkSource, Control, Cursor, ErrorKind, FallbackSource, Key, Library, SearchQuery,
};
use strata_fetch::testing::TestServer;

const N: i64 = 3000;

fn fixture(path: &Path) {
    let c = Connection::open(path).unwrap();
    c.execute_batch(
        "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
         CREATE TABLE items(id TEXT PRIMARY KEY, status TEXT, seq INTEGER);
         CREATE TABLE things(id TEXT PRIMARY KEY, name TEXT, kind TEXT);
         CREATE INDEX things_kind ON things(kind);
         INSERT INTO meta VALUES ('seq', '1');",
    )
    .unwrap();
    let tx = c.unchecked_transaction().unwrap();
    for i in 0..N {
        // Ids with characters a "next character" trick would trip on.
        let id = format!("t{i:05}{}", ["", "~", " z", "é"][(i % 4) as usize]);
        let kind = ["red", "green", "blue"][(i % 3) as usize];
        tx.execute(
            "INSERT INTO things VALUES (?1, ?2, ?3)",
            rusqlite::params![id, format!("thing {i} {}", "x".repeat((i % 30) as usize)), kind],
        )
        .unwrap();
        tx.execute("INSERT INTO items VALUES (?1, 'active', 1)", [&id]).unwrap();
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
        files_base: "https://files.example.invalid/download/",
        attribution: vec![],
        tables: vec!["things"],
        extra_indexes: vec![],
        views: vec![],
        search: SearchSpec {
            sql: "SELECT id AS grp, id, name, kind FROM things WHERE {WHERE} ORDER BY id",
            group_column: "id",
            fields: vec![
                f("id", Kind::Stored, true),
                f("name", Kind::Text, true),
                f("kind", Kind::Keyword, true),
            ],
        },
    }
}

struct World {
    dir: tempfile::TempDir,
    server: TestServer,
}

impl World {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        fixture(&dir.path().join("master.sqlite"));
        let w = Self {
            server: {
                std::fs::create_dir_all(dir.path().join("www/v3")).unwrap();
                TestServer::start(&dir.path().join("www"))
            },
            dir,
        };
        w.publish();
        w
    }
    fn publish(&self) {
        let d = self.dir.path();
        publish(&profile(), &d.join("master.sqlite"), &d.join("work"), &d.join("www/v3"), false, false)
            .unwrap();
    }
    fn bump(&self) {
        let c = Connection::open(self.dir.path().join("master.sqlite")).unwrap();
        c.execute_batch(
            "UPDATE things SET name = 'renamed' WHERE id IN ('t00001~', 't00002 z');
             UPDATE items SET seq = 2 WHERE id IN ('t00001~', 't00002 z');
             UPDATE meta SET value = '2' WHERE key = 'seq';",
        )
        .unwrap();
        self.publish();
    }
    fn lib(&self, name: &str) -> Library {
        Library::open(&self.server.addr, &self.dir.path().join(name), 1 << 30).unwrap()
    }
}

#[test]
fn pages_cover_a_whole_table_and_cursors_round_trip() {
    let w = World::new();
    let lib = w.lib("a");
    let ix = lib.index("things").unwrap();
    let mut ids = vec![];
    let mut after: Option<Cursor> = None;
    loop {
        let p = ix.range_page("things", &[], false, after.as_ref(), 333).unwrap();
        ids.extend(p.rows.iter().map(|r| r["id"].as_str().unwrap().to_string()));
        match p.next {
            // Through its text form, as an app would keep it.
            Some(c) => after = Some(c.to_string().parse().unwrap()),
            None => break,
        }
    }
    assert_eq!(ids.len() as i64, N);
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), ids.len());
    let all: Vec<_> = ix.range_iter("things", &[], false, 100).map(|r| r.unwrap()).collect();
    assert_eq!(all.len() as i64, N);
    // An index map, with a prefix.
    let red = ix
        .range_iter("things_kind", &[Key::from("red")], false, 77).collect::<anyhow::Result<Vec<_>>>().unwrap().len();
    assert_eq!(red as i64, (0..N).filter(|i| i % 3 == 0).count() as i64);
}

#[test]
fn counts_files_base_and_status() {
    let w = World::new();
    let lib = w.lib("a");
    let ix = lib.index("things").unwrap();
    assert_eq!(ix.files_base(), "https://files.example.invalid/download/");
    assert_eq!(ix.records(), N as u64);
    assert_eq!(ix.count("things"), Some(N as u64));
    assert_eq!(ix.count("nope"), None);
    assert!(ix.counts().iter().any(|(m, c)| m == "things_kind" && *c == N as u64));
    assert_eq!(lib.status(&ix).records, N as u64);
}

#[test]
fn plan_progress_cancel_and_resume() {
    let w = World::new();
    let lib = w.lib("a");
    let ix = lib.index("things").unwrap();
    let plan = lib.plan_keep(&ix).unwrap();
    assert!(plan.missing > 10 && plan.missing_bytes > 0 && plan.bytes >= plan.missing_bytes);

    // Cancel after a few objects.
    let ctl = Control::new();
    let seen = Arc::new(AtomicUsize::new(0));
    let c2 = ctl.clone();
    let s2 = seen.clone();
    let ctl = ctl.on_progress(move |p| {
        if s2.fetch_add(1, Ordering::SeqCst) == 3 {
            c2.cancel();
        }
        assert!(p.objects_done <= p.objects_total);
    });
    let e = lib.keep_offline_with(&ix, &ctl).unwrap_err();
    assert_eq!(error_kind(&e), ErrorKind::Cancelled);
    assert!(!lib.is_kept("things"));
    let part = lib.plan_keep(&ix).unwrap();
    assert!(part.missing < plan.missing, "fetched objects stay cached");

    // Resume: progress ends at the total, the plan is then empty.
    let last = Arc::new(Mutex::new(None));
    let l2 = last.clone();
    let r = lib
        .keep_offline_with(&ix, &Control::new().on_progress(move |p| *l2.lock().unwrap() = Some(p.clone())))
        .unwrap();
    let p = last.lock().unwrap().clone().unwrap();
    assert_eq!(p.objects_done, p.objects_total);
    assert_eq!(p.objects_total, part.missing);
    assert_eq!(p.bytes_done, p.bytes_total);
    assert_eq!(r.fetched, part.missing);
    assert!(r.bytes_fetched > 0 && r.bytes_fetched <= part.missing_bytes);
    assert_eq!(lib.plan_keep(&ix).unwrap().missing, 0);
    assert!(lib.is_kept("things"));
}

#[test]
fn kept_copy_syncs_and_opens_offline() {
    let w = World::new();
    let lib = w.lib("a");
    let ix = lib.index("things").unwrap();
    lib.keep_offline(&ix).unwrap();
    assert!(lib.plan_sync(&ix).unwrap().is_none());
    w.bump();
    let plan = lib.plan_sync(&ix).unwrap().unwrap();
    assert!(plan.seq > ix.seq() && plan.missing > 0);
    let (newer, r) = lib.sync_with(&ix, &Control::new()).unwrap().unwrap();
    // The plan is an upper bound: opening the version fetched its hot packs.
    assert!(r.updated && r.fetched > 0 && r.fetched <= plan.missing);
    assert!(lib.status(&newer).offline);
    assert_eq!(lib.plan_keep(&newer).unwrap().missing, 0);
    // A restart with no network: the kept (new) version, complete.
    let dead = Library::open("http://127.0.0.1:9", &w.dir.path().join("a"), 1 << 30).unwrap();
    let off = dead.index("things").unwrap();
    assert_eq!(off.seq(), newer.seq());
    let n = off.range_iter("things", &[], false, 500).collect::<anyhow::Result<Vec<_>>>().unwrap().len();
    assert_eq!(n as i64, N);
    let hit = off
        .search(&SearchQuery {
            text: Some("renamed".into()),
            limit: 5,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(hit.total, 2);
}

#[test]
fn a_small_cache_keeps_a_whole_kept_copy() {
    let w = World::new();
    let root = w.dir.path().join("small");
    let lib = Library::open(&w.server.addr, &root, 4096).unwrap();
    let ix = lib.index("things").unwrap();
    lib.keep_offline(&ix).unwrap();
    assert_eq!(lib.plan_keep(&ix).unwrap().missing, 0, "nothing evicted during the download");
    for (s, _) in ix.objects().unwrap() {
        assert!(root.join("cache").join(&s[..2]).join(&s).exists(), "{s} on disk");
    }
    drop(ix);
    let dead = Library::open("http://127.0.0.1:9", &root, 4096).unwrap();
    let off = dead.index_offline("things").unwrap();
    assert_eq!(off.range_iter("things", &[], false, 500).collect::<anyhow::Result<Vec<_>>>().unwrap().len() as i64, N);
}

#[test]
fn typed_errors() {
    let w = World::new();
    let lib = w.lib("a");
    let e = lib.index("missing").err().unwrap();
    assert_eq!(error_kind(&e), ErrorKind::Status(404));
    let dead = Library::open("http://127.0.0.1:9", &w.dir.path().join("b"), 1 << 20).unwrap();
    let e = dead.index("things").err().unwrap();
    assert_eq!(error_kind(&e), ErrorKind::Offline);
}

#[test]
fn two_libraries_on_one_folder_share_fetches() {
    let w = World::new();
    let a = w.lib("shared");
    let b = w.lib("shared");
    let ia = a.index("things").unwrap();
    let ib = b.index("things").unwrap();
    assert!(b.plan_keep(&ib).unwrap().missing > 0);
    a.keep_offline(&ia).unwrap();
    assert_eq!(b.plan_keep(&ib).unwrap().missing, 0);
    assert!(b.is_kept("things"));
    assert_eq!(b.keep_offline(&ib).unwrap().fetched, 0);
}

#[test]
fn fallback_source_and_membership() {
    let w = World::new();
    // The "computer at home": a library that keeps the index and serves it.
    let home = w.lib("home");
    let hix = home.index("things").unwrap();
    home.keep_offline(&hix).unwrap();
    let (sha, size) = hix.objects().unwrap()[0].clone();
    assert_eq!(hix.member(&sha).unwrap(), Some(size));
    assert!(home.stored_object(&hix, &sha).unwrap().is_some());
    let stranger = "00".repeat(32);
    assert_eq!(hix.member(&stranger).unwrap(), None);
    assert!(home.stored_object(&hix, &stranger).unwrap().is_none());
    assert!(String::from_utf8(hix.manifest_json()).unwrap().contains("\"things\""));

    // A phone: home first (down here), then the public tree.
    let src: Arc<dyn ChunkSource> = Arc::new(FallbackSource::new(vec![
        Library::http_source("http://127.0.0.1:9").unwrap(),
        Library::http_source(&w.server.addr).unwrap(),
    ]));
    let phone = Library::open_with(&w.dir.path().join("phone"), 1 << 30, src).unwrap();
    let pix = phone.index("things").unwrap();
    let r = phone.keep_offline(&pix).unwrap();
    assert!(r.fetched > 0);
    assert_eq!(phone.plan_keep(&pix).unwrap().missing, 0);
    assert_eq!(pix.range_iter("things", &[], false, 1000).collect::<anyhow::Result<Vec<_>>>().unwrap().len() as i64, N);
}

fn dir_bytes(p: &Path) -> u64 {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let m = e.metadata().unwrap();
            n += if m.is_dir() { dir_bytes(&e.path()) } else { m.len() };
        }
    }
    n
}

#[test]
fn bytes_fetched_match_what_was_written() {
    let w = World::new();
    let root = w.dir.path().join("count");
    let lib = w.lib("count");
    let ix = lib.index("things").unwrap();
    // Listing reads the internal tree nodes (as a plan does); not counted.
    lib.plan_keep(&ix).unwrap();
    let before = dir_bytes(&root.join("cache"));
    let last = Arc::new(Mutex::new(0u64));
    let l2 = last.clone();
    let r = lib
        .keep_offline_with(&ix, &Control::new().on_progress(move |p| *l2.lock().unwrap() = p.downloaded))
        .unwrap();
    let written = dir_bytes(&root.join("cache")) - before;
    assert_eq!(r.bytes_fetched, written);
    // With map bytes recorded, a whole-index plan is close to the truth (an
    // upper bound only for the few compressed search objects).
    let fresh = w.lib("fresh");
    let fx = fresh.index("things").unwrap();
    let plan = fresh.plan_keep(&fx).unwrap();
    let got = fresh.keep_offline(&fx).unwrap().bytes_fetched;
    assert!(!plan.estimated);
    assert!(plan.missing_bytes >= got && plan.missing_bytes < got + got / 10, "{plan:?} vs {got}");
    assert_eq!(*last.lock().unwrap(), written);
}
