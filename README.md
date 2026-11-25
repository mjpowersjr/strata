# strata

Static, content-addressed indexes that apps read in place over plain HTTP:
search with filters and facets, rows by key, key ranges, and offline copies
that sync only what changed. Publish to any static host (an S3/R2 bucket, a
CDN); read natively or in the browser (WebAssembly).

- **Objects:** every published file is immutable and named by the SHA-256 of
  its decoded bytes; stored with one encoding byte (raw or zstd). Readers
  verify everything they fetch.
- **Store:** each table is a prolly tree (content-defined, history-independent
  sorted map): an update rewrites only the paths to changed keys, and a reader
  needs `depth` round trips for a range or a batch of lookups.
- **Search:** tantivy segments, chunked into objects; opening reads one small
  "hot" pack per segment.
- **Manifest:** `{index}/manifest.json`, the only mutable file, written last.

Format: [docs/format.md](docs/format.md) (spec 3.0).

| Crate | What |
|---|---|
| `strata-object` | Stored form of an object |
| `strata-core` | Prolly trees, keys, search, queries (wasm-ready) |
| `strata` | Native reader; publisher from SQLite (`sqlite` feature) |
| `strata-fetch` | Verified, cached objects over HTTP |
| `strata-state` | Durable job state: blobs and a conditional HEAD on S3/R2 or a folder |
| `strata-web` | The reader for browsers (wasm32) |
| `strata-client` | Embedding API for apps |

Used by archive-index (indexes of archive.org audio collections).

Build and test:
`cargo test --release`.
