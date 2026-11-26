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
pub use source::{ChunkSource, FallbackSource, HttpSource};

/// Why a remote read failed (section 5.7).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteError {
    /// No connection, or it closed or reset before any answer.
    #[error("not available offline: {what}")]
    Offline { what: String },
    /// The server answered with a status other than success (after retries
    /// for 5xx and 429).
    #[error("fetch {url}: HTTP {status}")]
    Status { url: String, status: u16 },
    /// The response body ended early or its read failed part way.
    #[error("fetch {url} cut short: {detail}")]
    Truncated { url: String, detail: String },
    /// No complete answer within the request timeout.
    #[error("fetch {url} timed out")]
    Timeout { url: String },
    /// The answer wasn't HTTP (something else listens there, or a broken
    /// proxy).
    #[error("fetch {url}: not an HTTP answer: {detail}")]
    Protocol { url: String, detail: String },
    /// Any other failure (TLS, a client error).
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
