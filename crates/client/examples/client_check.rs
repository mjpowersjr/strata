//! `client_check <base> <index> <root> <query>`: open, keep offline, search,
//! then reopen with a dead base (offline) and search again.
use strata_client::{Library, SearchQuery};
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let (base, index, root, text) = (&a[1], &a[2], Path::new(&a[3]), &a[4]);
    let lib = Library::open(base, root, 4 << 30)?;
    let ix = lib.index(index)?;
    println!("status {}", serde_json::to_string(&lib.status(&ix))?);
    let kept = lib.keep_offline(&ix)?;
    println!("kept {}", serde_json::to_string(&kept)?);
    let q = SearchQuery {
        text: Some(text.clone()),
        limit: 3,
        ..Default::default()
    };
    let r = ix.search(&q)?;
    println!("online hits {}", r.hits.len());
    let (ix, s) = lib.sync(ix)?;
    println!("sync {}", serde_json::to_string(&s)?);
    drop(ix);
    // A base nothing listens on: the network fails, the kept copy answers.
    let dead = Library::open("http://127.0.0.1:9", root, 4 << 30)?;
    let ix = dead.index(index)?;
    println!("status {}", serde_json::to_string(&dead.status(&ix))?);
    let r2 = ix.search(&q)?;
    println!(
        "offline hits {} same={}",
        r2.hits.len(),
        serde_json::to_string(&r.hits)? == serde_json::to_string(&r2.hits)?
    );
    for (t, key, _) in ix.tables().into_iter().take(3) {
        println!("table {t} key {key:?}");
    }
    Ok(())
}
