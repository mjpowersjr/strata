//! Where chunks come from: the [`ChunkSource`] trait (pluggable, so an app
//! can route fetches through its own client or another device) and the
//! default [`HttpSource`]. Verification never depends on the source: the
//! cache checks every chunk against its sha256 (section 5.3, 9).

use crate::RemoteError;
use std::time::Duration;

/// Fetches immutable chunks by sha256, and the few small JSON objects that
/// name them (`manifest.json`, `catalog/versions/{sha}.json`).
///
/// Implementations block the calling thread. Never call them, or anything
/// that reads a remote catalog, directly from async code: use
/// `tokio::task::spawn_blocking` (or a plain thread).
pub trait ChunkSource: Send + Sync {
    /// The chunk's bytes. The caller verifies them.
    fn fetch(&self, sha256: &str, size: u64) -> Result<Vec<u8>, RemoteError>;
    /// A root-relative object (`manifest.json`, a versions JSON).
    fn fetch_path(&self, path: &str) -> Result<Vec<u8>, RemoteError>;
}

/// HTTP over the published `v1/` tree of one index.
pub struct HttpSource {
    client: reqwest::blocking::Client,
    /// The tree's root URL ending in `/`, e.g. `https://host/v3/`.
    base: String,
    /// `assets_base` (spec 1.1) for immutable objects, if any.
    assets_base: Option<String>,
    retries: u32,
    /// Path of an object relative to the base: v1 `catalog/chunks/` (default)
    /// or v2 `objects/` (docs/format.md).
    object_prefix: String,
}

impl HttpSource {
    /// `base` is the tree's root (`…/v3/`).
    pub fn new(base: &str) -> Result<Self, RemoteError> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(concat!("strata-fetch/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| RemoteError::Fetch {
                url: base.into(),
                status: None,
                detail: e.to_string(),
            })?;
        let mut base = base.to_string();
        if !base.ends_with('/') {
            base.push('/');
        }
        Ok(Self {
            client,
            base,
            assets_base: None,
            retries: 2,
            object_prefix: "catalog/chunks/".into(),
        })
    }

    /// Per-request timeout (default 60 s) and retry count (default 2).
    pub fn with_timeout(mut self, timeout: Duration, retries: u32) -> Result<Self, RemoteError> {
        self.client = reqwest::blocking::Client::builder()
            .user_agent(concat!("strata-fetch/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            .timeout(timeout)
            .build()
            .map_err(|e| RemoteError::Fetch {
                url: self.base.clone(),
                status: None,
                detail: e.to_string(),
            })?;
        self.retries = retries;
        Ok(self)
    }

    pub fn with_assets_base(mut self, assets_base: Option<String>) -> Self {
        self.assets_base = assets_base.map(|b| if b.ends_with('/') { b } else { format!("{b}/") });
        self
    }

    /// Fetch objects from `{base}{prefix}{sha}` (v2: `"objects/"`).
    pub fn with_object_prefix(mut self, prefix: &str) -> Self {
        self.object_prefix = prefix.to_string();
        self
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    fn get(&self, url: &str) -> Result<Vec<u8>, RemoteError> {
        let mut last = None;
        for attempt in 0..=self.retries {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(200 * (1 << attempt)));
            }
            match self.client.get(url).send() {
                Ok(r) if r.status().is_success() => match r.bytes() {
                    Ok(b) => return Ok(b.to_vec()),
                    Err(e) => {
                        last = Some(RemoteError::Fetch {
                            url: url.into(),
                            status: None,
                            detail: e.to_string(),
                        })
                    }
                },
                Ok(r) => {
                    let s = r.status().as_u16();
                    last = Some(RemoteError::Fetch {
                        url: url.into(),
                        status: Some(s),
                        detail: String::new(),
                    });
                    if (400..500).contains(&s) && s != 429 {
                        break;
                    }
                }
                Err(e) if e.is_connect() => {
                    last = Some(RemoteError::Offline { what: url.into() });
                }
                Err(e) => {
                    last = Some(RemoteError::Fetch {
                        url: url.into(),
                        status: None,
                        detail: e.to_string(),
                    })
                }
            }
        }
        Err(last.unwrap())
    }
}

impl ChunkSource for HttpSource {
    fn fetch(&self, sha256: &str, _size: u64) -> Result<Vec<u8>, RemoteError> {
        let path = format!("{}{sha256}", self.object_prefix);
        if let Some(a) = &self.assets_base {
            if let Ok(b) = self.get(&format!("{a}{path}")) {
                return Ok(b);
            }
        }
        self.get(&format!("{}{path}", self.base))
    }
    fn fetch_path(&self, path: &str) -> Result<Vec<u8>, RemoteError> {
        self.get(&format!("{}{path}", self.base))
    }
}
