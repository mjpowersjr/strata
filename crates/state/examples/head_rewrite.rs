//! `head_rewrite <store-url> <collection>`: read HEAD and write the same bytes
//! back with the commit's conditional write (a no-op that tests If-Match).
use strata_state::{store, Cond, PutOpts};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let s = store::open(&a[1])?;
    let k = format!("{}/HEAD", a[2]);
    let (b, e) = s.get(&k).await?.expect("HEAD");
    println!("etag {e:?}, {} bytes", b.len());
    let o = PutOpts { content_type: Some("application/json".into()), ..Default::default() };
    println!("{:?}", s.put(&k, b, Cond::Match(e), &o).await?);
    Ok(())
}
