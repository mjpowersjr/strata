//! Format v2 (docs/format.md) parts that build for wasm32 as well as
//! natively, so a Worker publisher writes exactly what the native one does:
//! prolly trees ([`prolly`]) and search segment publishing ([`search`]).
pub mod keys;
pub mod profile;
pub mod prolly;
pub mod query;
pub mod search;
pub mod version;

/// A content-addressed object: the sha256 (hex) of its decoded bytes and
/// their size (the stored form, `strata_object`, is usually smaller).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ObjRef {
    pub sha256: String,
    pub size: u64,
}
