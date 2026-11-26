//! Progress, cancel and size plans for downloads (keep offline, sync).

use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// What a download would fetch ([`Library::plan_keep`](crate::Library::plan_keep),
/// [`Library::plan_sync`](crate::Library::plan_sync)).
///
/// Sizes are stored sizes: exact for search objects (decoded size plus the
/// encoding byte; a few small compressed ones count as an upper bound) and
/// for the tree nodes when the version records each map's bytes (spec 3.2:
/// missing = recorded total less what's on disk). A version without them
/// counts nodes at its target node size, an upper bound (`estimated`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// The version planned for.
    pub seq: u64,
    /// Objects of that version, and their size.
    pub objects: usize,
    pub bytes: u64,
    /// Those not in the cache yet: what would be downloaded.
    pub missing: usize,
    pub missing_bytes: u64,
    pub estimated: bool,
}

/// A download's progress over the objects it has to fetch (not those
/// already cached). `bytes_*` are in [`Plan`] units (decoded, estimated for
/// nodes), so `bytes_done / bytes_total` is a fraction; `downloaded` is the
/// bytes actually received for those objects (each counted once; not the
/// internal tree nodes read to list them, which a [`Plan`] reads too).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Progress {
    pub objects_done: usize,
    pub objects_total: usize,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub downloaded: u64,
}

type Callback = Arc<dyn Fn(&Progress) + Send + Sync>;

/// Progress reporting and cancel for one download. Clone it to keep a
/// handle for [`cancel`](Self::cancel) on another thread.
#[derive(Clone, Default)]
pub struct Control {
    cancel: Arc<AtomicBool>,
    progress: Option<Callback>,
}

impl Control {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called after every fetched object (and once at the start), from the
    /// download's worker threads, one call at a time.
    pub fn on_progress(mut self, f: impl Fn(&Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(f));
        self
    }

    /// Stop the download: objects in flight finish, no new one starts, and
    /// the call returns [`Cancelled`].
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    pub(crate) fn report(&self, p: &Progress) {
        if let Some(f) = &self.progress {
            f(p)
        }
    }
}

/// A download stopped by [`Control::cancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cancelled")
    }
}

impl std::error::Error for Cancelled {}
