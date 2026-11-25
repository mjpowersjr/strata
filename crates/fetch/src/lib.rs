//! Content-addressed chunks over HTTP, verified and cached: what the v2
//! reader (`strata`) and the client fetch through.
//!
//! - [`cache::ChunkCache`]: verified, content-addressed chunks on disk,
//!   shared by every version and index; single-flight; pinning.
//! - [`source::ChunkSource`]: where chunks come from ([`source::HttpSource`]
//!   by default; pluggable).
//! - [`testing::TestServer`]: a local HTTP server over a directory (tests).

pub mod cache;
pub mod source;
pub mod testing;

pub use cache::{CacheStats, ChunkCache};
pub use source::{ChunkSource, HttpSource};

/// Why a remote read failed (section 5.7).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteError {
    #[error("not available offline: {what}")]
    Offline { what: String },
    #[error("fetch {url} failed{}: {detail}", status.map(|s| format!(" ({s})")).unwrap_or_default())]
    Fetch {
        url: String,
        status: Option<u16>,
        detail: String,
    },
    #[error("integrity check failed for {chunk}; the bytes were discarded")]
    Integrity { chunk: String },
    #[error("{0}")]
    SpecUnsupported(String),
    #[error("{0}")]
    ProfileMismatch(String),
    #[error(
        "catalog schema v{have} is newer than this software understands (v{want}); update the app"
    )]
    SchemaTooNew { have: u32, want: u32 },
    #[error("sqlite: {0}")]
    Sql(String),
    #[error("io: {0}")]
    Io(String),
}
