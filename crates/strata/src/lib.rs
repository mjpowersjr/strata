//! Published format v2 (docs/format.md): content-addressed objects under
//! one manifest per index version; tantivy search segments; prolly-tree
//! record maps; the publisher.

//!
//! Reading needs no SQLite. Publishing from a SQLite catalog is the `sqlite`
//! feature (`publish`, `store`, `search`, `keys`).

pub use strata_core::profile;
pub use strata_core::prolly;
pub mod reader;
pub mod version;
#[cfg(feature = "sqlite")]
pub mod keys;
#[cfg(feature = "sqlite")]
pub mod publish;
#[cfg(feature = "sqlite")]
pub mod search;
#[cfg(feature = "sqlite")]
pub mod store;

pub use strata_core::profile::Profile;
#[cfg(feature = "sqlite")]
pub use publish::{publish, publish_with, PublishReport};
pub use version::{Manifest, Version};
