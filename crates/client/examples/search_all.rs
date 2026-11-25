//! Search several published indexes at once:
//! `search_all BASE CACHE_DIR index1,index2,… QUERY…`
fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let lib = strata_client::Library::open(&a[0], std::path::Path::new(&a[1]), 1 << 30)?;
    let ixs: Vec<strata_client::Index> = a[2].split(',').map(|n| lib.index(n)).collect::<anyhow::Result<_>>()?;
    let refs: Vec<&strata_client::Index> = ixs.iter().collect();
    let q = strata_client::SearchQuery { text: Some(a[3..].join(" ")), limit: 8, ..Default::default() };
    let r = strata_client::search_all(&refs, &q)?;
    println!("totals {:?}", r.totals);
    for h in r.hits {
        let f = &h.hit.fields;
        let title = f.get("title").or(f.get("song")).and_then(|v| v.as_str()).unwrap_or("");
        let who = f.get("artist").or(f.get("series_name")).and_then(|v| v.as_str()).unwrap_or("");
        println!("  {:<9} #{} {:.4}  {} — {}", h.index, h.rank + 1, h.score, title, who);
    }
    Ok(())
}
