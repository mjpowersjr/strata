//! `cond_check <store-url>`: do the store's conditional writes behave
//! (create-if-absent, replace-if-match, refuse a stale ETag)?
use strata_state::{store, Cond, PutOpts};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::args().nth(1).expect("store url");
    let s = store::open(&url)?;
    let k = "probe/cond-check";
    let o = PutOpts::default();
    s.delete(k).await.ok();
    let a = s.put(k, b"one".to_vec(), Cond::Absent, &o).await?;
    println!("absent on new key: {a:?}");
    let (_, e1) = s.get(k).await?.unwrap();
    println!("get etag: {e1:?}");
    let b = s.put(k, b"two".to_vec(), Cond::Match(e1.clone()), &o).await?;
    println!("match current etag: {b:?}");
    let c = s.put(k, b"three".to_vec(), Cond::Match(e1), &o).await?;
    println!("match stale etag (want Conflict): {c:?}");
    let d = s.put(k, b"four".to_vec(), Cond::Absent, &o).await?;
    println!("absent on existing (want Conflict): {d:?}");
    s.delete(k).await?;
    Ok(())
}
