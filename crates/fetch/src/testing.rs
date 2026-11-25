//! A small HTTP/1.1 file server for tests and benchmarks: serves a local
//! `v1/` tree with an added delay per request (a round-trip time) and a
//! bandwidth limit, and can inject faults. Real sockets, real HTTP, so the
//! reader's timings over it are measured, with the network shape simulated
//! by sleeping (the design allows this in place of `tc netem`).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Answer 500 this many times, then serve normally.
    Fail500(u32),
    /// Send a body cut to half its length.
    Truncate,
    /// Send a body of the right length with one byte flipped.
    Corrupt,
    /// Sleep this long before answering.
    Stall(Duration),
}

#[derive(Default)]
pub struct Shape {
    pub rtt: Duration,
    /// Bytes per second; 0 = unlimited.
    pub bandwidth: u64,
}

pub struct TestServer {
    pub addr: String,
    root: PathBuf,
    shape: Arc<Mutex<Shape>>,
    faults: Arc<Mutex<HashMap<String, Fault>>>,
    pub requests: Arc<AtomicU64>,
    pub bytes: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    /// Replacement bodies by path (e.g. a swapped manifest).
    overrides: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl TestServer {
    pub fn start(root: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("http://{}/", listener.local_addr().unwrap());
        let s = Self {
            addr,
            root: root.to_path_buf(),
            shape: Arc::default(),
            faults: Arc::default(),
            requests: Arc::default(),
            bytes: Arc::default(),
            stop: Arc::default(),
            overrides: Arc::default(),
        };
        let (root, shape, faults, req, bytes, stop, ov) = (
            s.root.clone(),
            s.shape.clone(),
            s.faults.clone(),
            s.requests.clone(),
            s.bytes.clone(),
            s.stop.clone(),
            s.overrides.clone(),
        );
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(conn) = conn else { continue };
                let (root, shape, faults, req, bytes, stop, ov) = (
                    root.clone(),
                    shape.clone(),
                    faults.clone(),
                    req.clone(),
                    bytes.clone(),
                    stop.clone(),
                    ov.clone(),
                );
                std::thread::spawn(move || {
                    let _ = serve(conn, &root, &shape, &faults, &req, &bytes, &stop, &ov);
                });
            }
        });
        s
    }

    pub fn set_shape(&self, rtt: Duration, bandwidth: u64) {
        *self.shape.lock().unwrap() = Shape { rtt, bandwidth };
    }

    pub fn fault(&self, path: &str, f: Fault) {
        self.faults.lock().unwrap().insert(path.to_string(), f);
    }

    pub fn clear_faults(&self) {
        self.faults.lock().unwrap().clear();
    }

    pub fn override_body(&self, path: &str, body: Vec<u8>) {
        self.overrides
            .lock()
            .unwrap()
            .insert(path.to_string(), body);
    }

    /// Refuse all further connections and requests (offline).
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop.
        let _ = TcpStream::connect(
            self.addr
                .trim_start_matches("http://")
                .trim_end_matches('/'),
        );
    }

    pub fn reset_counts(&self) {
        self.requests.store(0, Ordering::SeqCst);
        self.bytes.store(0, Ordering::SeqCst);
    }
}

#[allow(clippy::too_many_arguments)]
fn serve(
    conn: TcpStream,
    root: &Path,
    shape: &Mutex<Shape>,
    faults: &Mutex<HashMap<String, Fault>>,
    req: &AtomicU64,
    bytes: &AtomicU64,
    stop: &AtomicBool,
    ov: &Mutex<HashMap<String, Vec<u8>>>,
) -> std::io::Result<()> {
    // No Nagle: a response is one write, but small requests must not wait
    // for delayed ACKs.
    conn.set_nodelay(true)?;
    let mut r = BufReader::new(conn.try_clone()?);
    let mut w = conn;
    loop {
        let mut line = String::new();
        if r.read_line(&mut line)? == 0 {
            return Ok(());
        }
        // Headers (ignored apart from the end).
        loop {
            let mut h = String::new();
            if r.read_line(&mut h)? == 0 || h == "\r\n" || h == "\n" {
                break;
            }
        }
        if stop.load(Ordering::SeqCst) {
            return Ok(());
        }
        req.fetch_add(1, Ordering::SeqCst);
        let path = line
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .split('?')
            .next()
            .unwrap_or("")
            .trim_start_matches('/')
            .to_string();
        let (rtt, bw) = {
            let s = shape.lock().unwrap();
            (s.rtt, s.bandwidth)
        };
        let fault = {
            let mut f = faults.lock().unwrap();
            match f.get(&path).copied() {
                Some(Fault::Fail500(n)) if n > 1 => {
                    f.insert(path.clone(), Fault::Fail500(n - 1));
                    Some(Fault::Fail500(n))
                }
                Some(Fault::Fail500(n)) => {
                    f.remove(&path);
                    Some(Fault::Fail500(n))
                }
                other => other,
            }
        };
        std::thread::sleep(rtt);
        if let Some(Fault::Stall(d)) = fault {
            std::thread::sleep(d);
        }
        if let Some(Fault::Fail500(_)) = fault {
            w.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")?;
            continue;
        }
        let body = match ov.lock().unwrap().get(&path).cloned() {
            Some(b) => Some(b),
            None => {
                let p = root.join(&path);
                if p.starts_with(root) && p.is_file() {
                    let mut b = vec![];
                    std::fs::File::open(&p)?.read_to_end(&mut b)?;
                    Some(b)
                } else {
                    None
                }
            }
        };
        let Some(mut body) = body else {
            w.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")?;
            continue;
        };
        let declared = body.len();
        match fault {
            Some(Fault::Truncate) => body.truncate(declared / 2),
            Some(Fault::Corrupt) if !body.is_empty() => body[declared / 2] ^= 0xff,
            _ => {}
        }
        if bw > 0 {
            std::thread::sleep(Duration::from_secs_f64(body.len() as f64 / bw as f64));
        }
        let ctype = match path.rsplit('.').next().unwrap_or("") {
            "html" => "text/html; charset=utf-8",
            "js" | "mjs" => "text/javascript",
            "wasm" => "application/wasm",
            "json" => "application/json",
            _ => "application/octet-stream",
        };
        let mut out = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {declared}\r\n\r\n"
        )
        .into_bytes();
        out.extend_from_slice(&body);
        w.write_all(&out)?;
        bytes.fetch_add(body.len() as u64, Ordering::SeqCst);
        if body.len() < declared {
            // Close mid-body.
            return Ok(());
        }
    }
}
