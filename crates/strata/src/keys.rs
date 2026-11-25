//! Key encoding (docs/format.md section 5): [`strata_core::keys`],
//! taking rusqlite values.

use strata_core::keys::{self as core, ValueRef as V};
pub use strata_core::keys::{decode, next_prefix, row_json, Part, Val, INT, NULL, REAL, TEXT};
use rusqlite::types::ValueRef;

pub fn conv(v: ValueRef<'_>) -> V<'_> {
    match v {
        ValueRef::Null => V::Null,
        ValueRef::Integer(i) => V::Integer(i),
        ValueRef::Real(f) => V::Real(f),
        ValueRef::Text(t) => V::Text(t),
        ValueRef::Blob(b) => V::Blob(b),
    }
}

/// Append one component.
pub fn push(out: &mut Vec<u8>, v: ValueRef<'_>, desc: bool) {
    core::push(out, conv(v), desc)
}

/// Encode a tuple of values (all ascending).
pub fn tuple(values: &[ValueRef<'_>]) -> Vec<u8> {
    core::tuple(&values.iter().map(|v| conv(*v)).collect::<Vec<_>>())
}

/// The byte range of every key whose components start with `values`.
pub fn prefix_range(values: &[ValueRef<'_>], partial_text: bool) -> (Vec<u8>, Vec<u8>) {
    core::prefix_range(
        &values.iter().map(|v| conv(*v)).collect::<Vec<_>>(),
        partial_text,
    )
}
