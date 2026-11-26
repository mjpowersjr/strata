//! `plan_check <base> <index> <root>`: what keeping an index offline would
//! download, its counts, and the first two pages of its items.
use std::path::Path;
use strata_client::Library;

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let lib = Library::open(&a[1], Path::new(&a[3]), 8 << 30)?;
    let ix = lib.index(&a[2])?;
    println!("records {} files_base {}", ix.records(), ix.files_base());
    for (m, c) in ix.counts() {
        println!("  {m}: {c}");
    }
    let t = std::time::Instant::now();
    let p = lib.plan_keep(&ix)?;
    println!("plan {} ({:.1}s)", serde_json::to_string(&p)?, t.elapsed().as_secs_f64());
    let p1 = ix.range_page("items", &[], false, None, 3)?;
    let p2 = ix.range_page("items", &[], false, p1.next.as_ref(), 3)?;
    for r in p1.rows.iter().chain(&p2.rows) {
        println!("  {}", r["id"]);
    }
    Ok(())
}
