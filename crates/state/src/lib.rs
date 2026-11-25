//! Durable state for the pipeline job (docs/container-plan.md): a storage
//! trait with a local-folder and an S3/R2 backend, and snapshots of a
//! working directory as content-addressed blobs listed by a `HEAD` that is
//! written conditionally (the commit and the lock in one).

pub mod snapshot;
pub mod store;

pub use snapshot::{Blobs, Component, Head, Kind, Snap};
pub use store::{Cond, Put, PutOpts, Store};
