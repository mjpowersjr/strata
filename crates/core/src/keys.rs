//! Order-preserving key encoding for store maps (docs/format.md section 5),
//! in the style of FoundationDB's tuple layer:
//!
//! - each component is a type tag followed by its payload, so byte order of
//!   encoded keys equals the order of the values, component by component;
//! - NULL `0x00` (sorts first), INTEGER `0x15` + 8 bytes big-endian with the
//!   sign bit flipped, REAL `0x21` + 8 bytes (IEEE order transform), TEXT
//!   `0x02` + bytes with every `0x00` escaped as `0x00 0xFF`, then `0x00`;
//! - a descending component is the same encoding with every byte inverted.
//!
//! A prefix of a TEXT component (without its terminator) is a byte prefix of
//! every key whose component starts with it, so "ids starting with
//! `lum-and-abner/`" is a prefix range.

/// A value as keys and rows see it (SQLite's types; a blob encodes like text).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a [u8]),
    Blob(&'a [u8]),
}

/// An owned [`ValueRef`] (query results from D1 or SQLite).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Val {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl Val {
    pub fn as_ref(&self) -> ValueRef<'_> {
        match self {
            Val::Null => ValueRef::Null,
            Val::Int(i) => ValueRef::Integer(*i),
            Val::Real(f) => ValueRef::Real(*f),
            Val::Text(t) => ValueRef::Text(t.as_bytes()),
        }
    }
    /// The row encoding's JSON value (store maps hold rows as JSON arrays).
    pub fn to_json(&self) -> serde_json::Value {
        row_json(self.as_ref())
    }
}

/// One column of a stored row: what the store map value's JSON array holds.
pub fn row_json(v: ValueRef<'_>) -> serde_json::Value {
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => i.into(),
        ValueRef::Real(f) => f.into(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned().into(),
        ValueRef::Blob(_) => serde_json::Value::Null,
    }
}

/// A row's store value: its columns as a JSON array.
pub fn row_value(vals: &[ValueRef<'_>]) -> Vec<u8> {
    let v: Vec<serde_json::Value> = vals.iter().map(|x| row_json(*x)).collect();
    serde_json::to_vec(&v).expect("json")
}

/// A stored row back as values (JSON numbers: integers stay integers).
pub fn row_from_json(b: &[u8]) -> Option<Vec<Val>> {
    let v: Vec<serde_json::Value> = serde_json::from_slice(b).ok()?;
    Some(
        v.into_iter()
            .map(|x| match x {
                serde_json::Value::Null => Val::Null,
                serde_json::Value::Number(n) => match n.as_i64() {
                    Some(i) => Val::Int(i),
                    None => Val::Real(n.as_f64().unwrap_or(0.0)),
                },
                serde_json::Value::String(s) => Val::Text(s),
                other => Val::Text(other.to_string()),
            })
            .collect(),
    )
}

pub const NULL: u8 = 0x00;
pub const TEXT: u8 = 0x02;
pub const INT: u8 = 0x15;
pub const REAL: u8 = 0x21;

fn push_text(out: &mut Vec<u8>, s: &[u8], terminate: bool) {
    out.push(TEXT);
    for &b in s {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    if terminate {
        out.push(0x00);
    }
}

/// Append one component.
pub fn push(out: &mut Vec<u8>, v: ValueRef<'_>, desc: bool) {
    let start = out.len();
    match v {
        ValueRef::Null => out.push(NULL),
        ValueRef::Integer(i) => {
            out.push(INT);
            out.extend_from_slice(&((i as u64) ^ (1 << 63)).to_be_bytes());
        }
        ValueRef::Real(f) => {
            out.push(REAL);
            let b = f.to_bits();
            let t = if b >> 63 == 1 { !b } else { b ^ (1 << 63) };
            out.extend_from_slice(&t.to_be_bytes());
        }
        ValueRef::Text(t) => push_text(out, t, true),
        ValueRef::Blob(t) => push_text(out, t, true),
    }
    if desc {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

/// Encode a tuple of values (all ascending).
pub fn tuple(values: &[ValueRef<'_>]) -> Vec<u8> {
    let mut k = vec![];
    for v in values {
        push(&mut k, *v, false);
    }
    k
}

/// The byte range of every key whose components start with `values`, the
/// last of which may be a text prefix (`partial_text = true`).
pub fn prefix_range(values: &[ValueRef<'_>], partial_text: bool) -> (Vec<u8>, Vec<u8>) {
    let mut lo = vec![];
    for (i, v) in values.iter().enumerate() {
        let last = i + 1 == values.len();
        match (v, last && partial_text) {
            (ValueRef::Text(t), true) => push_text(&mut lo, t, false),
            _ => push(&mut lo, *v, false),
        }
    }
    let hi = next_prefix(&lo);
    (lo, hi)
}

/// Smallest byte string greater than every string starting with `p`.
pub fn next_prefix(p: &[u8]) -> Vec<u8> {
    let mut hi = p.to_vec();
    while let Some(&last) = hi.last() {
        if last == 0xFF {
            hi.pop();
        } else {
            *hi.last_mut().unwrap() += 1;
            return hi;
        }
    }
    vec![0xFF; p.len() + 1]
}

/// A decoded key component.
#[derive(Debug, Clone, PartialEq)]
pub enum Part {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
}

impl Part {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Part::Null => serde_json::Value::Null,
            Part::Int(i) => (*i).into(),
            Part::Real(f) => (*f).into(),
            Part::Text(t) => t.clone().into(),
        }
    }
}

/// Decode a key of ascending components (as written by [`tuple`]; index
/// keys may start with descending components: pass how many to skip with
/// `desc` flags per position, `true` = descending).
pub fn decode(mut k: &[u8], desc: &[bool]) -> Option<Vec<Part>> {
    let mut out = vec![];
    let mut i = 0;
    while !k.is_empty() {
        let d = desc.get(i).copied().unwrap_or(false);
        let byte = |b: u8| if d { !b } else { b };
        let tag = byte(k[0]);
        k = &k[1..];
        match tag {
            NULL => out.push(Part::Null),
            INT => {
                let mut b = [0u8; 8];
                for (j, x) in k.get(..8)?.iter().enumerate() {
                    b[j] = byte(*x);
                }
                out.push(Part::Int((u64::from_be_bytes(b) ^ (1 << 63)) as i64));
                k = &k[8..];
            }
            REAL => {
                let mut b = [0u8; 8];
                for (j, x) in k.get(..8)?.iter().enumerate() {
                    b[j] = byte(*x);
                }
                let t = u64::from_be_bytes(b);
                let bits = if t >> 63 == 1 { t ^ (1 << 63) } else { !t };
                out.push(Part::Real(f64::from_bits(bits)));
                k = &k[8..];
            }
            TEXT => {
                let mut s = vec![];
                loop {
                    let b = byte(*k.first()?);
                    k = &k[1..];
                    if b == 0 {
                        if k.first().map(|x| byte(*x)) == Some(0xFF) {
                            s.push(0);
                            k = &k[1..];
                            continue;
                        }
                        break;
                    }
                    s.push(b);
                }
                out.push(Part::Text(String::from_utf8_lossy(&s).into_owned()));
            }
            _ => return None,
        }
        i += 1;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::ValueRef::*;
    use super::*;

    #[test]
    fn order_matches_values() {
        let vals: Vec<Vec<ValueRef>> = vec![
            vec![Null],
            vec![Text(b"")],
            vec![Text(b"a")],
            vec![Text(b"a\0b")],
            vec![Text(b"ab")],
            vec![Text(b"b"), Integer(-5)],
            vec![Text(b"b"), Integer(3)],
            vec![Integer(i64::MIN)],
            vec![Integer(-1)],
            vec![Integer(0)],
            vec![Integer(7)],
            vec![Real(-2.5)],
            vec![Real(0.0)],
            vec![Real(1e9)],
        ];
        let keys: Vec<Vec<u8>> = vals.iter().map(|v| tuple(v)).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn descending_component_reverses_order() {
        let enc = |n: i64| {
            let mut k = vec![];
            push(&mut k, Integer(n), true);
            push(&mut k, Text(b"x"), false);
            k
        };
        assert!(enc(10) < enc(5));
        assert!(enc(5) < enc(-3));
    }

    #[test]
    fn decode_round_trips() {
        let k = tuple(&[Text(b"a\0b"), Integer(-7), Real(2.5), Null]);
        assert_eq!(
            decode(&k, &[]).unwrap(),
            vec![
                Part::Text("a\0b".into()),
                Part::Int(-7),
                Part::Real(2.5),
                Part::Null
            ]
        );
        let mut d = vec![];
        push(&mut d, Integer(42), true);
        push(&mut d, Text(b"slug"), false);
        assert_eq!(
            decode(&d, &[true]).unwrap(),
            vec![Part::Int(42), Part::Text("slug".into())]
        );
    }

    #[test]
    fn text_prefix_range() {
        let (lo, hi) = prefix_range(&[Text(b"lum-and-abner/")], true);
        for s in [&b"lum-and-abner/1942-01-01"[..], b"lum-and-abner/undated/x"] {
            let k = tuple(&[Text(s)]);
            assert!(k >= lo && k < hi);
        }
        let other = tuple(&[Text(b"lum-and-abnerz")]);
        assert!(!(other >= lo && other < hi));
    }
}
