//! Key/value object storage: a local folder, or an S3-compatible bucket
//! (Cloudflare R2). Keys are `/`-separated relative paths. Writes can be
//! conditional on the current ETag ([`Cond`]), which is what makes `HEAD` a
//! commit point and a lock at once.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Condition for a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cond {
    /// Unconditional.
    None,
    /// Only if the key doesn't exist yet.
    Absent,
    /// Only if the key's current ETag is this.
    Match(String),
}

#[derive(Debug, Clone, Default)]
pub struct PutOpts {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
}

/// Result of a conditional write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Put {
    /// Written; the new ETag.
    Done(String),
    /// The condition didn't hold; nothing was written.
    Conflict,
}

#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// The object and its ETag, or `None` when absent.
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>>;
    async fn put(&self, key: &str, body: Vec<u8>, cond: Cond, opts: &PutOpts) -> Result<Put>;
    /// Every key under `prefix` (the prefix included in each key).
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// For messages.
    fn describe(&self) -> String;
}

/// Open a store from a URL: `file:///abs/path`, a plain path, or
/// `s3://bucket/prefix` with the endpoint and keys from the environment
/// (`S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, optional
/// `S3_REGION`, default `auto` as R2 expects).
pub fn open(url: &str) -> Result<Box<dyn Store>> {
    if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let env = |k: &str| std::env::var(k).with_context(|| format!("{k} is not set"));
        return Ok(Box::new(S3Store::new(
            &env("S3_ENDPOINT")?,
            bucket,
            prefix,
            &env("S3_ACCESS_KEY_ID")?,
            &env("S3_SECRET_ACCESS_KEY")?,
            &std::env::var("S3_REGION").unwrap_or_else(|_| "auto".into()),
        )?));
    }
    let path = url.strip_prefix("file://").unwrap_or(url);
    Ok(Box::new(LocalStore::new(path)?))
}

pub fn sha256_hex(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

fn check_key(key: &str) -> Result<()> {
    anyhow::ensure!(
        !key.is_empty()
            && !key.starts_with('/')
            && !key.contains('\\')
            && key
                .split('/')
                .all(|s| !s.is_empty() && s != "." && s != ".."),
        "bad key {key:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------- local

/// A folder. ETags are the sha256 of the content. Conditional writes take an
/// exclusive lock file per key, so concurrent writers on one machine are
/// serialized.
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        check_key(key)?;
        Ok(self.root.join(key))
    }
}

struct LockFile(PathBuf);

impl LockFile {
    fn take(p: PathBuf) -> Result<Self> {
        for _ in 0..600 {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&p)
            {
                Ok(_) => return Ok(Self(p)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("lock {} held for 30 s", p.display())
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[async_trait::async_trait]
impl Store for LocalStore {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        match std::fs::read(self.path(key)?) {
            Ok(b) => {
                let e = sha256_hex(&b);
                Ok(Some((b, e)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: &str, body: Vec<u8>, cond: Cond, _opts: &PutOpts) -> Result<Put> {
        let p = self.path(key)?;
        if let Some(d) = p.parent() {
            std::fs::create_dir_all(d)?;
        }
        let _lock = match cond {
            Cond::None => None,
            _ => Some(LockFile::take(p.with_extension("lock"))?),
        };
        let current = match std::fs::read(&p) {
            Ok(b) => Some(sha256_hex(&b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let ok = match &cond {
            Cond::None => true,
            Cond::Absent => current.is_none(),
            Cond::Match(e) => current.as_deref() == Some(e.as_str()),
        };
        if !ok {
            return Ok(Put::Conflict);
        }
        let etag = sha256_hex(&body);
        let tmp = p.with_extension(format!("tmp{}", std::process::id()));
        std::fs::write(&tmp, &body)?;
        std::fs::rename(&tmp, &p)?;
        Ok(Put::Done(etag))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = vec![];
        let mut stack = vec![self.root.clone()];
        while let Some(d) = stack.pop() {
            let rd = match std::fs::read_dir(&d) {
                Ok(r) => r,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            for e in rd {
                let e = e?;
                let p = e.path();
                if e.file_type()?.is_dir() {
                    stack.push(p);
                    continue;
                }
                let rel = p
                    .strip_prefix(&self.root)?
                    .to_string_lossy()
                    .replace('\\', "/");
                if rel.ends_with(".lock") || rel.contains(".tmp") {
                    continue;
                }
                if rel.starts_with(prefix) {
                    out.push(rel);
                }
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match std::fs::remove_file(self.path(key)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn describe(&self) -> String {
        self.root.display().to_string()
    }
}

// ------------------------------------------------------------------- S3

/// An S3-compatible bucket, path-style requests signed with AWS Signature
/// Version 4. R2: endpoint `https://{account}.r2.cloudflarestorage.com`,
/// region `auto`; it honours `If-Match` / `If-None-Match` on PUT.
pub struct S3Store {
    endpoint: String,
    host: String,
    bucket: String,
    prefix: String,
    access: String,
    secret: String,
    region: String,
    http: reqwest::Client,
}

impl S3Store {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        prefix: &str,
        access: &str,
        secret: &str,
        region: &str,
    ) -> Result<Self> {
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let host = endpoint
            .split("://")
            .nth(1)
            .context("S3 endpoint needs a scheme")?
            .split('/')
            .next()
            .unwrap_or_default()
            .to_string();
        let prefix = prefix.trim_matches('/');
        Ok(Self {
            endpoint,
            host,
            bucket: bucket.into(),
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            },
            access: access.into(),
            secret: secret.into(),
            region: region.into(),
            http: reqwest::Client::builder()
                .user_agent("strata-state/0.1")
                .timeout(Duration::from_secs(300))
                .build()?,
        })
    }

    fn object_path(&self, key: &str) -> String {
        format!(
            "/{}/{}",
            self.bucket,
            uri_encode(&format!("{}{key}", self.prefix), false)
        )
    }

    /// Send a signed request; retries transport errors and 5xx.
    async fn send(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<reqwest::Response> {
        let mut last = None;
        for attempt in 0..5u64 {
            let now = chrono::Utc::now();
            let signed = sign_v4(&SignInput {
                method,
                host: &self.host,
                path,
                query,
                headers,
                payload_sha256: &sha256_hex(&body),
                access: &self.access,
                secret: &self.secret,
                region: &self.region,
                amz_date: &now.format("%Y%m%dT%H%M%SZ").to_string(),
            });
            let qs = canonical_query(query);
            let url = if qs.is_empty() {
                format!("{}{path}", self.endpoint)
            } else {
                format!("{}{path}?{qs}", self.endpoint)
            };
            let mut req = self
                .http
                .request(reqwest::Method::from_bytes(method.as_bytes())?, &url);
            for (k, v) in &signed {
                if k != "host" {
                    req = req.header(k, v);
                }
            }
            match req.body(body.clone()).send().await {
                Ok(r) if r.status().is_server_error() => {
                    last = Some(anyhow::anyhow!("{method} {path}: HTTP {}", r.status()))
                }
                Ok(r) => return Ok(r),
                Err(e) => last = Some(e.into()),
            }
            tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) * (attempt + 1))).await;
        }
        Err(last.unwrap())
    }
}

fn etag_of(r: &reqwest::Response) -> String {
    r.headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        // R2 marks the ETag weak (`W/"…"`) when it compresses a response
        // (JSON, for one); the tag inside is the object's, which If-Match takes.
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_string()
}

#[async_trait::async_trait]
impl Store for S3Store {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        check_key(key)?;
        // `send` retries the request; a connection dropped while reading the
        // body is retried here, from the start.
        let mut attempt = 0u64;
        loop {
            let r = self
                .send("GET", &self.object_path(key), &[], &[], vec![])
                .await?;
            match r.status().as_u16() {
                404 => return Ok(None),
                200 => {
                    let e = etag_of(&r);
                    match r.bytes().await {
                        Ok(b) => return Ok(Some((b.to_vec(), e))),
                        Err(err) if attempt < 4 => {
                            attempt += 1;
                            tracing::warn!(key, attempt, error = %err, "GET body interrupted; retrying");
                            tokio::time::sleep(Duration::from_millis(500 * attempt * attempt)).await;
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
                s => anyhow::bail!(
                    "GET {key}: HTTP {s}: {}",
                    r.text().await.unwrap_or_default()
                ),
            }
        }
    }

    async fn put(&self, key: &str, body: Vec<u8>, cond: Cond, opts: &PutOpts) -> Result<Put> {
        check_key(key)?;
        let mut h: Vec<(String, String)> = vec![];
        if let Some(c) = &opts.content_type {
            h.push(("content-type".into(), c.clone()));
        }
        if let Some(c) = &opts.cache_control {
            h.push(("cache-control".into(), c.clone()));
        }
        match &cond {
            Cond::None => {}
            Cond::Absent => h.push(("if-none-match".into(), "*".into())),
            Cond::Match(e) => h.push(("if-match".into(), format!("\"{e}\""))),
        }
        let r = self
            .send("PUT", &self.object_path(key), &[], &h, body)
            .await?;
        match r.status().as_u16() {
            200 | 201 => Ok(Put::Done(etag_of(&r))),
            // 412: the condition failed. 409: a concurrent conditional write.
            412 | 409 => Ok(Put::Conflict),
            s => anyhow::bail!(
                "PUT {key}: HTTP {s}: {}",
                r.text().await.unwrap_or_default()
            ),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = vec![];
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), format!("{}{prefix}", self.prefix)),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token".into(), t.clone()));
            }
            let r = self
                .send("GET", &format!("/{}", self.bucket), &q, &[], vec![])
                .await?;
            let s = r.status();
            let text = r.text().await?;
            anyhow::ensure!(s.is_success(), "LIST {prefix}: HTTP {s}: {text}");
            for k in xml_values(&text, "Key") {
                if let Some(k) = k.strip_prefix(&self.prefix) {
                    out.push(k.to_string());
                }
            }
            token = xml_values(&text, "NextContinuationToken")
                .into_iter()
                .next();
            if token.is_none() {
                break;
            }
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        check_key(key)?;
        let r = self
            .send("DELETE", &self.object_path(key), &[], &[], vec![])
            .await?;
        anyhow::ensure!(
            r.status().is_success() || r.status().as_u16() == 404,
            "DELETE {key}: HTTP {}",
            r.status()
        );
        Ok(())
    }

    fn describe(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }
}

/// The text of every `<tag>…</tag>` (S3 XML has no attributes on these).
fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = vec![];
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(
            after[..j]
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'"),
        );
        rest = &after[j + close.len()..];
    }
    out
}

// --------------------------------------------------------------- SigV4

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    inner.update(msg);
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// RFC 3986 encoding as SigV4 wants it (`/` kept in paths).
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn canonical_query(q: &[(String, String)]) -> String {
    let mut v: Vec<(String, String)> = q
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    v.sort();
    v.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub(crate) struct SignInput<'a> {
    pub method: &'a str,
    pub host: &'a str,
    /// Already URI-encoded.
    pub path: &'a str,
    pub query: &'a [(String, String)],
    pub headers: &'a [(String, String)],
    pub payload_sha256: &'a str,
    pub access: &'a str,
    pub secret: &'a str,
    pub region: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date: &'a str,
}

/// The request headers to send (lowercase names), `authorization` included.
pub(crate) fn sign_v4(i: &SignInput) -> Vec<(String, String)> {
    let mut h: Vec<(String, String)> = i
        .headers
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.trim().to_string()))
        .collect();
    h.push(("host".into(), i.host.into()));
    h.push(("x-amz-content-sha256".into(), i.payload_sha256.into()));
    h.push(("x-amz-date".into(), i.amz_date.into()));
    h.sort();
    let signed: Vec<&str> = h.iter().map(|(k, _)| k.as_str()).collect();
    let signed = signed.join(";");
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        i.method,
        i.path,
        canonical_query(i.query),
        h.iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>(),
        signed,
        i.payload_sha256
    );
    let date = &i.amz_date[..8];
    let scope = format!("{date}/{}/s3/aws4_request", i.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        i.amz_date,
        sha256_hex(canonical.as_bytes())
    );
    let k = hmac_sha256(format!("AWS4{}", i.secret).as_bytes(), date.as_bytes());
    let k = hmac_sha256(&k, i.region.as_bytes());
    let k = hmac_sha256(&k, b"s3");
    let k = hmac_sha256(&k, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&k, to_sign.as_bytes()));
    h.push((
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={sig}",
            i.access
        ),
    ));
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's published Signature V4 example for S3 ("GET Object").
    #[test]
    fn sigv4_matches_the_aws_example() {
        let h = sign_v4(&SignInput {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            query: &[],
            headers: &[("Range".into(), "bytes=0-9".into())],
            payload_sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            access: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
            amz_date: "20130524T000000Z",
        });
        let auth = &h.iter().find(|(k, _)| k == "authorization").unwrap().1;
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn hmac_matches_rfc4231() {
        // RFC 4231 test case 2.
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn list_xml_parses() {
        let x = "<ListBucketResult><Contents><Key>a/b&amp;c</Key></Contents><Contents><Key>a/d</Key></Contents><NextContinuationToken>t1</NextContinuationToken></ListBucketResult>";
        assert_eq!(xml_values(x, "Key"), vec!["a/b&c", "a/d"]);
        assert_eq!(xml_values(x, "NextContinuationToken"), vec!["t1"]);
    }

    #[tokio::test]
    async fn local_conditional_writes() {
        let dir = std::env::temp_dir().join(format!("state-store-{}", std::process::id()));
        let s = LocalStore::new(&dir).unwrap();
        let o = PutOpts::default();
        let Put::Done(e1) = s
            .put("c/HEAD", b"one".to_vec(), Cond::Absent, &o)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            s.put("c/HEAD", b"x".to_vec(), Cond::Absent, &o)
                .await
                .unwrap(),
            Put::Conflict
        );
        assert_eq!(
            s.put("c/HEAD", b"x".to_vec(), Cond::Match("nope".into()), &o)
                .await
                .unwrap(),
            Put::Conflict
        );
        assert!(matches!(
            s.put("c/HEAD", b"two".to_vec(), Cond::Match(e1.clone()), &o)
                .await
                .unwrap(),
            Put::Done(_)
        ));
        assert_eq!(s.get("c/HEAD").await.unwrap().unwrap().0, b"two");
        assert_eq!(s.list("c/").await.unwrap(), vec!["c/HEAD"]);
        assert!(s.put("../x", vec![], Cond::None, &o).await.is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
