//! Telling failures apart without reading their text.

use crate::{Cancelled, RemoteError};
use serde::Serialize;

/// What kind of failure an error from this crate is ([`error_kind`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// No connection (or, for an offline open, nothing saved to open).
    Offline,
    /// The server answered with this HTTP status (404/410: gone; 5xx: try
    /// later).
    Status(u16),
    /// The response ended early.
    Truncated,
    /// No complete answer in time.
    Timeout,
    /// An object didn't match its name (discarded, not cached).
    Integrity,
    /// A format or spec version this software doesn't read.
    Unsupported,
    /// Stopped by [`Control::cancel`](crate::Control::cancel).
    Cancelled,
    /// Anything else (a local disk error, a malformed object, a bug).
    Other,
}

/// The kind of an error returned by [`Library`](crate::Library) or
/// [`Index`](crate::Index), from the typed error inside it (also when it
/// was carried through the search engine's I/O errors).
pub fn error_kind(e: &anyhow::Error) -> ErrorKind {
    for c in e.chain() {
        if c.downcast_ref::<Cancelled>().is_some() {
            return ErrorKind::Cancelled;
        }
        if let Some(r) = remote(c) {
            return kind_of(r);
        }
    }
    ErrorKind::Other
}

fn remote<'a>(c: &'a (dyn std::error::Error + 'static)) -> Option<&'a RemoteError> {
    if let Some(r) = c.downcast_ref::<RemoteError>() {
        return Some(r);
    }
    // Wrapped in an io::Error (the search engine's directory reads).
    let io = c.downcast_ref::<std::io::Error>()?;
    io.get_ref()?.downcast_ref::<RemoteError>()
}

fn kind_of(r: &RemoteError) -> ErrorKind {
    match r {
        RemoteError::Offline { .. } => ErrorKind::Offline,
        RemoteError::Status { status, .. } => ErrorKind::Status(*status),
        RemoteError::Fetch {
            status: Some(s), ..
        } => ErrorKind::Status(*s),
        RemoteError::Truncated { .. } => ErrorKind::Truncated,
        RemoteError::Timeout { .. } => ErrorKind::Timeout,
        RemoteError::Integrity { .. } => ErrorKind::Integrity,
        RemoteError::SpecUnsupported(_) | RemoteError::SchemaTooNew { .. } => ErrorKind::Unsupported,
        _ => ErrorKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_survive_context_and_io_wrapping() {
        let e = anyhow::Error::from(RemoteError::Status {
            url: "u".into(),
            status: 410,
        })
        .context("open things");
        assert_eq!(error_kind(&e), ErrorKind::Status(410));
        let io = std::io::Error::other(RemoteError::Timeout { url: "u".into() });
        assert_eq!(error_kind(&anyhow::Error::from(io)), ErrorKind::Timeout);
        assert_eq!(error_kind(&anyhow::Error::from(Cancelled)), ErrorKind::Cancelled);
        assert_eq!(error_kind(&anyhow::anyhow!("x")), ErrorKind::Other);
    }
}
