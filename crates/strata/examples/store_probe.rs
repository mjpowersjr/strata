//! Build one table's map from a catalog into a scratch dir and report.
use strata::prolly::Writer;
use strata::store::build_store;
fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let c = rusqlite::Connection::open_with_flags(
        format!("file:{}?immutable=1", a[0]),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;
    let mut w = Writer::new(std::path::Path::new(&a[1]))?;
    let r = build_store(&c, &[a[2].as_str()], &[], &[], 16384, &mut w)?;
    println!("{}", serde_json::to_string_pretty(&r)?);
    println!("nodes {} bytes {}", w.written.len(), w.bytes);
    Ok(())
}
