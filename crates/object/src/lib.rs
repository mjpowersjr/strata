//! The stored form of a published object (docs/format.md, "Objects").
//!
//! An object is stored as one encoding byte followed by the payload:
//!
//! | byte | payload |
//! |---|---|
//! | `0` | the bytes as they are |
//! | `1` | a zstd frame of the bytes |
//!
//! Its name is the SHA-256 of the **decoded** bytes, never of the stored
//! ones. How an object is compressed is then the publisher's choice, free to
//! change (a new zstd, another level, raw instead) without renaming
//! anything: an unchanged object keeps its name whatever bytes store it.
//! Readers decode (bounded by the size they expect) and verify the decoded
//! bytes against the name.

use sha2::{Digest, Sha256};

/// How the payload is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Raw,
    /// zstd at this level.
    Zstd(i32),
}

const RAW: u8 = 0;
const ZSTD: u8 = 1;

/// The largest object decoded when its size isn't known in advance (tree
/// nodes: a reference is just a hash). A sanity bound only: decoding
/// allocates what the object needs, never this.
pub const MAX_UNSIZED: usize = 256 << 20;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("empty object")]
    Empty,
    #[error("unknown object encoding {0}")]
    Encoding(u8),
    #[error("zstd: {0}")]
    Zstd(String),
    #[error("object is {got} bytes decoded, expected {want}")]
    Size { got: u64, want: u64 },
    #[error("object does not match its name {0}")]
    Hash(String),
}

/// The name of an object with these (decoded) bytes.
pub fn name(raw: &[u8]) -> String {
    hex::encode(Sha256::digest(raw))
}

/// The stored form of `raw`.
pub fn encode(raw: &[u8], codec: Codec) -> Vec<u8> {
    match codec {
        Codec::Raw => {
            let mut v = Vec::with_capacity(raw.len() + 1);
            v.push(RAW);
            v.extend_from_slice(raw);
            v
        }
        Codec::Zstd(level) => {
            let z = zstd::bulk::compress(raw, level).expect("zstd compress");
            let mut v = Vec::with_capacity(z.len() + 1);
            v.push(ZSTD);
            v.extend_from_slice(&z);
            v
        }
    }
}

/// `(name, stored form)` of `raw`.
pub fn encode_named(raw: &[u8], codec: Codec) -> (String, Vec<u8>) {
    (name(raw), encode(raw, codec))
}

/// The decoded bytes of a stored object, at most `max` of them.
pub fn decode(stored: &[u8], max: usize) -> Result<Vec<u8>, Error> {
    let (&tag, payload) = stored.split_first().ok_or(Error::Empty)?;
    match tag {
        RAW => {
            if payload.len() > max {
                return Err(Error::Size { got: payload.len() as u64, want: max as u64 });
            }
            Ok(payload.to_vec())
        }
        ZSTD => {
            // Frames written by `encode` record their size: allocate exactly
            // that (bounded by `max`), not `max`.
            let n = match zstd::zstd_safe::get_frame_content_size(payload) {
                Ok(Some(n)) => n,
                Ok(None) => return Err(Error::Zstd("frame without a content size".into())),
                Err(_) => return Err(Error::Zstd("not a zstd frame".into())),
            };
            if n > max as u64 {
                return Err(Error::Size { got: n, want: max as u64 });
            }
            let raw = zstd::bulk::decompress(payload, n as usize)
                .map_err(|e| Error::Zstd(e.to_string()))?;
            if raw.len() as u64 != n {
                return Err(Error::Size { got: raw.len() as u64, want: n });
            }
            Ok(raw)
        }
        t => Err(Error::Encoding(t)),
    }
}

/// Decode and verify against `name`; `size` is the decoded size when known
/// (0: unknown, bounded by [`MAX_UNSIZED`]).
pub fn decode_verified(stored: &[u8], name_hex: &str, size: u64) -> Result<Vec<u8>, Error> {
    let max = if size == 0 { MAX_UNSIZED } else { size as usize };
    let raw = decode(stored, max)?;
    if size != 0 && raw.len() as u64 != size {
        return Err(Error::Size { got: raw.len() as u64, want: size });
    }
    if name(&raw) != name_hex {
        return Err(Error::Hash(name_hex.to_string()));
    }
    Ok(raw)
}

/// Check a stored object against its name without keeping its decoded
/// bytes (for objects only written to disk, such as a whole index kept
/// offline). Decodes into a buffer and decoder reused by the calling
/// thread, so verifying many objects doesn't allocate each one's decoded
/// size anew. Accepts exactly what [`decode_verified`] accepts.
pub fn verify(stored: &[u8], name_hex: &str, size: u64) -> Result<(), Error> {
    use std::cell::RefCell;
    /// A thread's buffer is let go after an object larger than this.
    const KEEP: usize = 4 << 20;
    thread_local! {
        static SCRATCH: RefCell<(Option<zstd::bulk::Decompressor<'static>>, Vec<u8>)> =
            const { RefCell::new((None, Vec::new())) };
    }
    let max = if size == 0 {
        MAX_UNSIZED
    } else {
        size as usize
    };
    let check = |raw: &[u8]| {
        if size != 0 && raw.len() as u64 != size {
            return Err(Error::Size {
                got: raw.len() as u64,
                want: size,
            });
        }
        if hex::encode(Sha256::digest(raw)) != name_hex {
            return Err(Error::Hash(name_hex.to_string()));
        }
        Ok(())
    };
    let (&tag, payload) = stored.split_first().ok_or(Error::Empty)?;
    match tag {
        RAW => {
            if payload.len() > max {
                return Err(Error::Size {
                    got: payload.len() as u64,
                    want: max as u64,
                });
            }
            check(payload)
        }
        ZSTD => {
            let n = match zstd::zstd_safe::get_frame_content_size(payload) {
                Ok(Some(n)) => n,
                Ok(None) => return Err(Error::Zstd("frame without a content size".into())),
                Err(_) => return Err(Error::Zstd("not a zstd frame".into())),
            };
            if n > max as u64 {
                return Err(Error::Size {
                    got: n,
                    want: max as u64,
                });
            }
            SCRATCH.with(|s| {
                let mut s = s.borrow_mut();
                let (dec, buf) = &mut *s;
                if dec.is_none() {
                    *dec = Some(
                        zstd::bulk::Decompressor::new().map_err(|e| Error::Zstd(e.to_string()))?,
                    );
                }
                buf.clear();
                buf.reserve(n as usize);
                let r = dec
                    .as_mut()
                    .unwrap()
                    .decompress_to_buffer(payload, buf)
                    .map_err(|e| Error::Zstd(e.to_string()))
                    .and_then(|got| {
                        if got as u64 != n {
                            return Err(Error::Size {
                                got: got as u64,
                                want: n,
                            });
                        }
                        check(buf)
                    });
                if buf.capacity() > KEEP {
                    *buf = Vec::new();
                }
                r
            })
        }
        t => Err(Error::Encoding(t)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_name_is_of_the_decoded_bytes() {
        let raw = b"hello hello hello hello hello".repeat(10);
        let (n1, z) = encode_named(&raw, Codec::Zstd(3));
        let (n2, r) = encode_named(&raw, Codec::Raw);
        let (n3, z19) = encode_named(&raw, Codec::Zstd(19));
        assert_eq!(n1, n2);
        assert_eq!(n1, n3);
        assert_ne!(z, r);
        for s in [&z, &r, &z19] {
            assert_eq!(decode_verified(s, &n1, raw.len() as u64).unwrap(), raw);
            assert_eq!(decode_verified(s, &n1, 0).unwrap(), raw);
        }
    }

    #[test]
    fn rejects_wrong_name_size_and_encoding() {
        let raw = vec![7u8; 1000];
        let (n, z) = encode_named(&raw, Codec::Zstd(3));
        assert!(matches!(decode_verified(&z, &name(b"x"), 0), Err(Error::Hash(_))));
        assert!(matches!(decode_verified(&z, &n, 999), Err(Error::Size { .. })));
        let (_, r) = encode_named(&raw, Codec::Raw);
        assert!(matches!(decode_verified(&r, &n, 999), Err(Error::Size { .. })));
        assert!(matches!(decode(&[9, 1, 2], 10), Err(Error::Encoding(9))));
        assert!(matches!(decode(&[], 10), Err(Error::Empty)));
        // A header-less (old format) object never verifies.
        assert!(decode_verified(&raw, &n, 0).is_err());
    }

    #[test]
    fn verify_accepts_and_rejects_what_decode_verified_does() {
        let raw = b"hello hello hello hello hello".repeat(10);
        let (n, z) = encode_named(&raw, Codec::Zstd(3));
        let (_, r) = encode_named(&raw, Codec::Raw);
        for s in [&z, &r] {
            verify(s, &n, raw.len() as u64).unwrap();
            verify(s, &n, 0).unwrap();
            assert!(matches!(verify(s, &name(b"x"), 0), Err(Error::Hash(_))));
            assert!(matches!(verify(s, &n, 9), Err(Error::Size { .. })));
        }
        // Reused buffer: a larger object, then a smaller one, then one too
        // big to keep the buffer for.
        let big = vec![3u8; 5 << 20];
        let (nb, zb) = encode_named(&big, Codec::Zstd(1));
        verify(&zb, &nb, big.len() as u64).unwrap();
        verify(&z, &n, raw.len() as u64).unwrap();
        assert!(matches!(verify(&[9, 1, 2], &n, 0), Err(Error::Encoding(9))));
        assert!(matches!(verify(&[], &n, 0), Err(Error::Empty)));
        assert!(verify(&raw, &n, 0).is_err());
    }
}
