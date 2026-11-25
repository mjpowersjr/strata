//! Cost of finding changed groups' doc ids through the segment key maps:
//! `keys_probe <tree> <index> <master> <group sql> <n>` (the SQL lists group
//! keys; every k-th of them is looked up).
use strata::prolly::{parse_hash, Reader, Root};
use strata::search::{decode_doc_ids, record_key, SegmentObj};
use strata::version::{Manifest, Version};

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let (tree, index, master, sql, n) = (&a[0], &a[1], &a[2], &a[3], a[4].parse::<usize>()?);
    let objs = std::path::Path::new(tree).join("objects");
    let m: Manifest =
        serde_json::from_slice(&std::fs::read(format!("{tree}/{index}/manifest.json"))?)?;
    let v: Version = serde_json::from_slice(&std::fs::read(objs.join(&m.version.sha256))?)?;
    let c = rusqlite::Connection::open_with_flags(
        format!("file:{master}?immutable=1"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;
    let mut st = c.prepare(sql)?;
    let all: Vec<String> = st.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
    let step = (all.len() / n).max(1);
    let keys: Vec<Vec<u8>> = all
        .iter()
        .step_by(step)
        .take(n)
        .map(|g| record_key(g).to_be_bytes().to_vec())
        .collect();
    for seg in &v.search.segments {
        let obj: SegmentObj =
            serde_json::from_slice(&std::fs::read(objs.join(&seg.object.sha256))?)?;
        let root = Root {
            hash: parse_hash(&obj.keys.root)?,
            depth: obj.keys.depth,
            count: obj.keys.count,
            levels: obj.keys.levels.clone(),
        };
        let r = Reader::new(&objs);
        let t = std::time::Instant::now();
        let got = r.get_many(&root, &keys)?;
        let docs: usize = got.values().map(|v| decode_doc_ids(v).len()).sum();
        let c = r.counts();
        println!(
            "{index} segment {} ({} docs, key map depth {}): {} groups -> {} found, {docs} docs; {} round trips, {} nodes, {:.2} MiB, {} ms",
            &seg.id[..8], seg.max_doc, root.depth, keys.len(), got.len(), c.rounds, c.fetches, c.bytes as f64 / 1048576.0, t.elapsed().as_millis()
        );
    }
    Ok(())
}
