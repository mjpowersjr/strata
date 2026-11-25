//! The pass-based reader natively, over a local tree:
//! `pass_check <tree> <index> <query JSON>` prints the result and passes.
use strata_web::{Fetch, WebReader};

struct Dir(std::path::PathBuf);

#[async_trait::async_trait(?Send)]
impl Fetch for Dir {
    async fn get(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        Ok(std::fs::read(self.0.join(path))?)
    }
}

/// Drive a future whose I/O is always ready (local files) without an
/// executor: the query code runs `block_on` itself, as it may in a browser
/// (where the outer future is driven by JS, not a futures executor).
fn run_ready<F: std::future::Future>(f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    run_ready(async {
        let r = WebReader::open(Dir(a[0].clone().into()), &a[1]).await?;
        let opened = r.stats();
        let q: strata_core::query::SearchQuery = serde_json::from_str(&a[2])?;
        let res = r.search(&q).await?;
        println!("{}", serde_json::to_string(&res)?);
        eprintln!("open: {opened:?}; after search: {:?}", r.stats());
        Ok(())
    })
}
