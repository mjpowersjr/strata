# strata format (spec 3.2)

The published format: content-addressed objects under one manifest per index
version, tantivy search segments, and prolly-tree record maps.

- **3.2 (2026-11-26):** a store map may record `bytes`, its nodes' total stored size, so a
  reader can size a whole download exactly before fetching (section 3). Optional; 3.0 and 3.1
  versions still read.
- **3.1 (2026-11-25):** the version's `audio_base` is `files_base`.
- **3.0 (2026-11-25):** objects are named by the sha256 of their decoded bytes and stored with
  an encoding byte (section 2, "Stored form"). Published trees use the prefix `v3/`.
- **2.x:** objects named by the hash of their stored bytes (nodes compressed first).

Examples below come from the indexes strata was built for (archive.org audio collections:
`great78`, `otr`, `concerts`); nothing in the format depends on them.

## 1. Goals

- **Search, filters and facet counts in place:** one parallel step per query, verified bytes.
- **Browse and look up records in place:** a few round trips per page.
- **Cheap daily updates:** an update adds objects and never rewrites old ones; clients fetch
  only what is new.
- **Offline:** pin a whole collection or part of one, sync exactly the difference.
- **Generic:** nothing in the format is specific to 78s, radio or concerts. A collection is
  defined by its tables and its search documents (section 6).

## 2. Objects and the manifest

**Every published file is immutable and named by its hash, except the manifest.**

| key (R2, under `v3/`) | what | cache |
|---|---|---|
| `objects/{sha256}` | any immutable object: search file chunks, hot packs, tree nodes, version descriptions | `public, max-age=31536000, immutable` |
| `{index}/manifest.json` | the current version of one index: a pointer to its version object | `public, max-age=60` |

- **One object namespace for all indexes.** Identical bytes are stored once, and a client's
  cache is shared across indexes and versions. Readers verify every object against its name.
- **`{index}`** is a short name: `great78`, `otr`, `concerts`. A future collection gets a new
  one.
- **Served** from the edge-cached custom domain (`docs/r2-public-assets.md`).

### Stored form (spec 3.0)

**An object's name is the sha256 of its decoded bytes; how it is stored is separate.** The
stored object is one encoding byte, then the payload (`crates/object`):

| byte | payload |
|---|---|
| `0` | the bytes as they are |
| `1` | a zstd frame of the bytes (with its content size) |

- **The compressor never enters a name.** The publisher may change the codec, its level or its
  version at any time; an unchanged object keeps its name, and a new publish writes only what
  changed. (Before 3.0, nodes and delete files were named after compression, so a zstd upgrade
  would have renamed every node: a full republish for nothing.)
- **Readers decode, then verify** the decoded bytes against the name. Decoding is bounded: by the
  `size` in the reference where there is one (`ObjRef.size` is the decoded size), else 256 MiB;
  a zstd frame's own content size is checked against the bound before anything is allocated.
- **What the publisher uses now:** tree nodes, segment objects and the version object zstd (3),
  delete files zstd (9); search file chunks and hot packs raw (tantivy compresses what
  compresses, and they are read often).
- **Caches** keep the stored form on disk (compact offline copies) and the decoded bytes in
  memory.

### `{index}/manifest.json`

```json
{
  "format": "v2",
  "spec_version": "2.0.0",
  "index": "otr",
  "seq": 330970,
  "built_at": "2026-11-23T08:00:00Z",
  "version": { "sha256": "…", "size": 1834 }
}
```

- `seq` is the master catalog's change sequence that this version reflects.
- `version` names the version object (section 3). The manifest is written **last**, so it is
  the atomic switch between versions.
- **Clients refuse a `spec_version` whose major part they don't know.**

## 3. The version object

A JSON object in `objects/`, describing one complete version:

```json
{
  "index": "otr",
  "seq": 330970,
  "previous": { "sha256": "…" },
  "profile": "otr",
  "files_base": "https://archive.org/download/",
  "attribution": [ … ],
  "search": {
    "engine": "tantivy-0.27-sstable",
    "chunk_size": 16384,
    "fields": [ { "name": "title", "kind": "text", "boost": 3.0, "stored": true }, … ],
    "files": { "meta.json": { "size": 1788, "chunks": ["…"] }, ".managed.json": { … } },
    "segments": [
      { "id": "9a5adbe0…", "max_doc": 3486496,
        "object": { "sha256": "…", "size": 828669 },
        "hot":    { "sha256": "…", "size": 3696755 },
        "del":    { "name": "9a5adbe0….1.del", "size": 435913, "object": { "sha256": "…", "size": 2296 } } }
    ]
  },
  "store": {
    "node_target": 16384,
    "maps": {
      "episodes":        { "root": "…", "depth": 4, "count": 218861, "bytes": 9123456, "levels": [1630, 12, 1], "key": ["id"] },
      "series_by_count": { "root": "…", "depth": 2, "count": 12870, "key": ["n_episodes desc", "slug"], "of": "series" }
    }
  },
  "counts": { "records": 218861 }
}
```

- **`previous`** chains versions, so a client or a GC pass can walk the history.
- **`search.files`** holds only the index-wide files (`meta.json`, `.managed.json`), which are
  small and new in every version. Each file is cut into `chunk_size` chunks, and each chunk is
  an object.
- **`search.segments`** has three references per segment, so the version object stays a few KB
  however large the index grows (spec 2.1; in 2.0 it listed every chunk, 836 KB for concerts):
  - **`object`**: the immutable segment object (JSON). It holds the segment's files and their
    chunks, `hot_parts` (how the hot pack splits into chunks), and `keys`, a prolly map from
    record key (8 bytes, big-endian) to the segment's doc ids (delta varints). The key map
    lets a publisher find a changed group's documents without reading the `record_key`
    column; readers never need it. An unchanged segment is the same object in every version.
  - **`hot`**: what opening the segment reads, plus its field norms, deduplicated and
    concatenated (spike section 12). Each part is verified against its own hash after the
    pack's hash is checked.
  - **`del`** (when the segment has deletes): the `.del` alive bitset as one object (stored
    zstd-compressed). It changes on every update that deletes from the segment, so it is kept out of
    the segment object and the hot pack. Mostly ones, it compresses to almost nothing.
- **Opening** fetches each segment's object, hot pack and delete object in parallel: still
  three round trips (manifest, version, segments).
- **`store.maps`** holds one prolly tree per table and per secondary index (section 5). A map's
  identity is its root hash. `levels` counts its nodes per level (leaves first). Publishers
  editing in place use it to see a change of height. `bytes` (3.2, optional) is the stored size
  of all its nodes, each once: node sizes are recorded nowhere else, so this is how a reader
  sizes a download before fetching it (what's missing = `bytes` less the nodes it has).

## 4. Search (tantivy segments)

**The one-index rule:** one tantivy index per collection holds the search text **and** every
filter field.

- **Text fields** are tokenized with Unicode word splitting, lower-casing and ASCII folding
  (like FTS5's `unicode61 remove_diacritics`), each with a boost.
- **Filter fields** are raw terms plus fast columns, **without field norms**.
- **Year:** `year` plus an explicit `dated` flag. "Include undated" is a query choice, never a
  guessed value.
- **Display fields are stored** (`STORED`): the few fields a result row shows, such as id,
  title and date. A result list then needs no store lookup.
- **Document order** follows the collection's main browse key where that helps: concerts by
  artist, then date. Columns compress, and "one artist by date" is doc-id order.
- **The dictionary is the sstable dictionary** (tantivy's `quickwit` feature): block-based,
  so it can be read in place.

**Queries** (reader behaviour, not format):
- every token must match some text field, with BM25 and field boosts;
- filters are required clauses;
- facet counts come from term aggregations in the same pass;
- **prefix and typo terms are rewritten** to their 32 most frequent dictionary expansions, so
  the warm-up fetches everything in parallel (spike section 12);
- typo matching (edit distance 1) is a fallback when exact matching finds fewer than 5 hits.

**Updates:**
- **Which records changed is found by hashing, not from the change log.** The publisher runs the
  search query over the whole catalog, hashes each group's rows (a group is the documents one
  record key covers: an item's tracks, a show's songs) and compares with the hashes it kept
  from the last build (`{work}/search-groups.tsv.zst`). Changed, new and vanished groups are
  replaced. A document derived from several records (a show, from its recordings) is replaced
  whenever anything it shows changes, and a withdrawal needs no change-log entry.
- Delete the changed groups' documents by their record key (a fast column, `record_key`,
  holding the first 8 bytes of the sha256 of the group key).
- Add their new documents as **one new segment**. The other segments are unchanged.
- **Merge policy:** merge everything into one segment when there are more than 8 segments, or
  more than 10% of documents are deleted, or at a weekly publish. A merge rewrites the search
  files once.

## 5. Store (prolly trees)

**Every table is a sorted map from its primary key to its row**, cut into content-defined nodes
(spike section 4).
- **Node boundaries:** an entry ends a node when the first 4 bytes of sha256(key), read as a
  u32, fall below `2^32 × entry_bytes / node_target`. There is a hard cap at 4 × the target.
- **Nodes:** prefix-compressed keys, one object each (stored zstd-compressed, named by the hash of
  the uncompressed node).
- **Internal entries** list each child's first key, hash and entry count, so a range with a
  limit knows how many leaves it needs.
- **The same data always gives the same tree,** so an update rewrites only the paths to changed
  keys, and diffing two versions walks only the subtrees that differ.

**Keys:**
- Byte strings: text parts separated by 0x00; integers as big-endian with the sign bit flipped
  (byte order equals numeric order); descending order by negating.
- **Keys are stable record ids, never SQLite rowids.** Rowids are reassigned on update and
  churn every map that uses them.

**Values:** a row as a JSON array, in the column order the version object's map entry lists.

**Secondary indexes** are maps of their own, from `(index columns…, primary key)` to an empty
value. A lookup through one is two batched steps: index range, then primary keys. Each is
listed under `store.maps` with `"of": "<table>"`.

**Reading:** each tree level of a range scan or a batch of lookups is one round trip, with all
that level's nodes fetched in parallel. Internal nodes are small: caching them makes most
pages 1 or 2 round trips.

## 6. What a collection defines (the profile's v2 part)

- **Tables:** name, primary key columns, value columns, secondary indexes. These come from the
  master catalog, via a SQL query per table.
- **Search documents:** a SQL query with one row per document, a mapping of columns to text,
  filter and stored fields, boosts, and the document order.
- **Attribution and licence notes**, copied into the version object.

Nothing else is collection-specific: the publisher, the reader and the sync are shared code.

## 7. Publishing a version

1. **Changes:** nothing to do when the master's `seq` equals the last published one. Otherwise
   the search finds its changed groups by hash (section 4), and the store rebuilds every map
   (unchanged nodes hash the same and are not rewritten).
2. **Search:** apply deletes and adds as a new segment (or merge, by policy), then chunk the
   new and changed files and build the new segments' hot packs.
3. **Store:** for each map, apply upserts and deletes and write the new nodes.
4. **Upload** objects R2 doesn't have yet (list first, or keep a local record of what's been
   uploaded), then the version object, then **`manifest.json` last**.
5. **GC:**
   - Keep every object referenced by the versions published in the last 30 days, and always
     by the last 3.
   - Delete the rest in batches.
   - GC never deletes an object a retained version of *any* index references.

**Cost, measured** (Great 78, spike sections 2 and 4):
- One change: search 31 KB, store 63 KiB.
- 125 changes: search 166 KB, store 1.6 to 2.8 MiB.
- A full merge: about 16 MiB, once.

## 8. Reading, following and syncing

- **Open:** fetch `manifest.json`, then the version object, then every segment's hot pack (one
  request each), and open the search index. Store maps open by their root hashes.
- **Follow:** poll `manifest.json` (it is cached for 60 s, so polling is cheap). On a new
  version:
  - **search:** new segment files and delete files only; old segments' cached chunks stay
    valid;
  - **store:** new roots; nodes are fetched lazily, and unchanged nodes are already cached.
  - A reader binds to one version at a time and switches between queries, so it never mixes
    versions.
- **Pin (offline):** fetch every object a version references, for a whole index or one map's
  key range (for example one artist). Syncing a pin to a new version fetches the new objects
  only:
  - a tree diff walks only differing subtrees;
  - the search index adds its new segment files.
- **Verify** every object against its hash, whatever the source (CDN, Worker, relay, peer).
- **Retention:** a client can skip any number of versions. It compares its version's object
  set with the current one, so no delta chain is needed.
