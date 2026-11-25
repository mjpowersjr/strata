//! What an in-place edit of a published map costs:
//! `apply_probe <tree dir> <index> <map> <n edits> [--compare]`.
//! Edits `n` rows spread over the map (a third updated, a third deleted, a
//! third inserted next to an existing key), applies them in place, and
//! reports nodes read and new nodes written. `--compare` also rebuilds the
//! edited map in full and checks the root is the same.
use strata::prolly::{self, Reader, Root, Writer};
use std::collections::BTreeMap;

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let (tree, index, map, n) = (&a[0], &a[1], &a[2], a[3].parse::<usize>()?);
    let compare = a.iter().any(|x| x == "--compare");
    let objs = std::path::Path::new(tree).join("objects");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(format!("{tree}/{index}/manifest.json"))?)?;
    let vsha = manifest["version"]["sha256"].as_str().unwrap();
    let version: serde_json::Value = serde_json::from_slice(&std::fs::read(objs.join(vsha))?)?;
    let m = &version["store"]["maps"][map];
    let target = version["store"]["node_target"].as_u64().unwrap() as usize;
    let root = Root {
        hash: prolly::parse_hash(m["root"].as_str().unwrap())?,
        depth: m["depth"].as_u64().unwrap() as u8,
        count: m["count"].as_u64().unwrap(),
        levels: m["levels"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x.as_u64()).collect())
            .unwrap_or_default(),
    };
    let internal_bytes = Reader::new(&objs).preload_internal(&root)?;
    println!(
        "  internal nodes: {:.2} MiB",
        internal_bytes as f64 / 1048576.0
    );
    let r = Reader::new(&objs);
    let firsts = r.leaf_first_keys(&root)?;
    let step = (firsts.len() / n.max(1)).max(1);
    let picks: Vec<Vec<u8>> = firsts.iter().step_by(step).take(n).cloned().collect();
    let vals = r.get_many(&root, &picks)?;
    let mut edits: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    for (i, k) in picks.iter().enumerate() {
        match i % 3 {
            0 => {
                let mut v = vals[k].clone();
                v.extend_from_slice(b" ");
                edits.insert(k.clone(), Some(v));
            }
            1 => {
                edits.insert(k.clone(), None);
            }
            _ => {
                let mut k2 = k.clone();
                k2.push(0x01);
                edits.insert(k2, Some(vals[k].clone()));
            }
        }
    }
    let scratch = tempfile::tempdir()?;
    let mut w = Writer::new(scratch.path())?;
    // A fresh reader: count what the edit itself reads.
    let r = Reader::new(&objs);
    let t = std::time::Instant::now();
    let (new_root, st) = prolly::apply(&r, &root, &edits, target, &mut w)?;
    let ms = t.elapsed().as_millis();
    let c = r.counts();
    let (mut new_nodes, mut new_bytes) = (0u64, 0u64);
    for h in &w.written {
        let name = prolly::hash_hex(h);
        if !objs.join(&name).exists() {
            new_nodes += 1;
            new_bytes += std::fs::metadata(scratch.path().join(&name))?.len();
        }
    }
    println!(
        "{index}/{map}: {} rows, depth {}, {} leaves; {} edits in {ms} ms; read {} nodes ({:.2} MiB: {} internal, {} leaves, read-all {}, {} round trips); {new_nodes} new nodes, {:.1} KiB",
        root.count, root.depth, firsts.len(), edits.len(), c.fetches, c.bytes as f64 / 1048576.0,
        st.internal_nodes_read, st.leaves_read, st.read_all_internal, st.rounds, new_bytes as f64 / 1024.0
    );
    if compare {
        let all = r.range(&root, &[], &[0xff; 64], None)?;
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = all.into_iter().collect();
        for (k, v) in &edits {
            match v {
                Some(v) => {
                    m.insert(k.clone(), v.clone());
                }
                None => {
                    m.remove(k);
                }
            }
        }
        let mut w2 = Writer::new(scratch.path())?;
        let full = prolly::build(m.into_iter().collect(), target, &mut w2)?;
        println!(
            "  full rebuild root equal: {}",
            full.hash == new_root.hash
                && full.depth == new_root.depth
                && full.count == new_root.count
        );
    }
    Ok(())
}
