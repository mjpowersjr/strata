//! Publish one index version into a staging tree (docs/format.md section 7):
//! update the search index in the working directory, rebuild the store maps,
//! write new objects to `{tree}/objects/`, then the version object, then
//! `{tree}/{index}/manifest.json` last. Uploading the staging tree to R2 is
//! a separate step (only objects R2 doesn't have yet).

use crate::prolly::Writer;
use crate::search;
use crate::store;
use crate::version::{Manifest, ObjRef, Version, FORMAT, SPEC_VERSION};
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub use strata_core::profile::Profile;

/// Publisher state kept in the working directory.
#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    seq: u64,
    version: Option<ObjRef>,
    /// Fingerprint of the profile it was published with ([`profile_hash`]).
    #[serde(default)]
    profile: String,
    /// The format it was published in ([`FORMAT`]): another one starts over.
    #[serde(default)]
    format: String,
}

/// A fingerprint of what a profile publishes (tables, orderings, views,
/// search spec, metadata): a different profile republishes even when the
/// catalog didn't change.
pub fn profile_hash(p: &Profile) -> String {
    let desc = format!(
        "{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}",
        p.index,
        p.profile,
        p.audio_base,
        p.attribution,
        p.tables,
        p.extra_indexes,
        p.views,
        p.search
    );
    hex::encode(Sha256::digest(desc.as_bytes()))
}

#[derive(Debug, Default, Serialize)]
pub struct PublishReport {
    pub index: String,
    pub from_seq: u64,
    pub to_seq: u64,
    pub search: search::SearchUpdate,
    pub objects_written: u64,
    pub bytes_written: u64,
    pub version: Option<ObjRef>,
    pub unchanged: bool,
}

pub const SEARCH_CHUNK: u64 = 16384;
pub const NODE_TARGET: usize = 16384;

struct Objects {
    dir: PathBuf,
    written: u64,
    bytes: u64,
}

impl Objects {
    fn put(&mut self, sha: &str, data: &[u8]) -> Result<()> {
        let p = self.dir.join(sha);
        if !p.exists() {
            // Runaway guard: one publish never legitimately writes this much.
            anyhow::ensure!(
                self.written < 1_000_000 && self.bytes < 5 << 30,
                "publish wrote over 1M objects or 5 GB: aborting"
            );
            let tmp = p.with_extension("tmp");
            std::fs::write(&tmp, data)?;
            std::fs::rename(&tmp, &p)?;
            self.written += 1;
            self.bytes += data.len() as u64;
        }
        Ok(())
    }
}

pub fn publish(
    p: &Profile,
    master: &Path,
    work: &Path,
    tree: &Path,
    full: bool,
    force_merge: bool,
) -> Result<PublishReport> {
    publish_with(p, master, work, tree, full, force_merge, false)
}

/// [`publish`]; `rewrite_version` writes a new version object and manifest
/// even with no catalog changes (for example after the version format
/// gained a field), without touching the search index.
pub fn publish_with(
    p: &Profile,
    master: &Path,
    work: &Path,
    tree: &Path,
    full: bool,
    force_merge: bool,
    rewrite_version: bool,
) -> Result<PublishReport> {
    let c = Connection::open_with_flags(
        format!("file:{}?immutable=1", master.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| master.display().to_string())?;
    std::fs::create_dir_all(work)?;
    let state_path = work.join("state.json");
    let state: State = std::fs::read(&state_path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    let seq: u64 = c
        .query_row("SELECT value FROM meta WHERE key='seq'", [], |r| {
            r.get::<_, String>(0)
        })?
        .parse()?;
    // Published in another format (another tree): nothing carries over.
    let same_format = state.format == FORMAT;
    let full = full || !same_format;
    let mut rep = PublishReport {
        index: p.index.into(),
        from_seq: state.seq,
        to_seq: seq,
        ..Default::default()
    };
    // A catalog behind the last publish is a different catalog (an older
    // copy, or a rebuild that didn't continue the seq): refuse rather than
    // publish a seq that goes backwards. `--full` starts over on purpose.
    anyhow::ensure!(
        full || state.version.is_none() || seq >= state.seq,
        "catalog seq {seq} is behind the last publish ({}): restore the newer catalog, rebuild with --previous, or publish with --full",
        state.seq
    );
    let phash = profile_hash(p);
    if seq == state.seq
        && !full
        && !force_merge
        && !rewrite_version
        && state.version.is_some()
        && state.profile == phash
        && same_format
    {
        rep.unchanged = true;
        rep.version = state.version;
        return Ok(rep);
    }
    // 1. Search: full build, or the changed groups (by group hash) as one
    //    new segment.
    let sdir = work.join("search");
    rep.search = search::update(
        &sdir,
        &p.search,
        &c,
        full || state.version.is_none(),
        force_merge,
    )?;
    // 2. Objects.
    let odir = tree.join("objects");
    std::fs::create_dir_all(&odir)?;
    let mut objs = Objects {
        dir: odir.clone(),
        written: 0,
        bytes: 0,
    };
    // Search and store tree nodes go through one prolly writer (which writes
    // `{dir}/{sha}`); count what's new by listing.
    let before = std::fs::read_dir(&odir)?.count();
    let mut w = Writer::new(&odir)?;
    let mut search_ref = search::publish_files(
        &sdir,
        SEARCH_CHUNK,
        &mut |sha, data| objs.put(sha, data),
        &mut w,
    )?;
    search_ref.fields = search::field_infos(&p.search);
    // 3. Store.
    let before = before + objs.written as usize;
    let store_ref = store::build_store(
        &c,
        &p.tables,
        &p.extra_indexes,
        &p.views,
        NODE_TARGET,
        &mut w,
    )?;
    let after = std::fs::read_dir(&odir)?.count();
    objs.written += (after - before) as u64;
    objs.bytes += w.bytes;
    let records: u64 = c.query_row(
        "SELECT count(*) FROM items WHERE status='active'",
        [],
        |r| r.get::<_, i64>(0),
    )? as u64;
    // 4. Version object, then the manifest last.
    let version = Version {
        index: p.index.into(),
        seq,
        previous: if same_format { state.version.clone() } else { None },
        profile: p.profile.into(),
        audio_base: p.audio_base.into(),
        attribution: p.attribution.clone(),
        search: search_ref,
        store: store_ref,
        records,
    };
    let vbytes = serde_json::to_vec(&version)?;
    let (vsha, stored) = strata_object::encode_named(&vbytes, strata_object::Codec::Zstd(3));
    objs.put(&vsha, &stored)?;
    let vref = ObjRef {
        sha256: vsha,
        size: vbytes.len() as u64,
    };
    let manifest = Manifest {
        format: FORMAT.into(),
        spec_version: SPEC_VERSION.into(),
        index: p.index.into(),
        seq,
        built_at: chrono_now(),
        version: vref.clone(),
    };
    let mdir = tree.join(p.index);
    std::fs::create_dir_all(&mdir)?;
    std::fs::write(
        mdir.join("manifest.json.tmp"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    std::fs::rename(mdir.join("manifest.json.tmp"), mdir.join("manifest.json"))?;
    std::fs::write(
        &state_path,
        serde_json::to_vec(&State {
            seq,
            version: Some(vref.clone()),
            profile: phash,
            format: FORMAT.into(),
        })?,
    )?;
    rep.objects_written = objs.written;
    rep.bytes_written = objs.bytes;
    rep.version = Some(vref);
    Ok(rep)
}

fn chrono_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
