//! `keep_probe <base> <index> <root> [mem_mb]`: keep an index offline and
//! measure this process's memory while it runs and after the library and
//! index are dropped: peak RSS (VmHWM), RSS, and on glibc the heap in use
//! and held by malloc (`mallinfo2`), and after `malloc_trim(0)` (what of
//! the freed memory malloc gives back). Delete `<root>` first for a cold
//! keep; `mem_mb` is the library's in-memory cap (default 64).
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strata_client::{Control, Library, Options};

fn status(field: &str) -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|n| n.parse::<u64>().ok())
        .map_or(0, |kb| kb >> 10)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn heap() -> String {
    #[repr(C)]
    #[derive(Default)]
    struct Mallinfo2 {
        arena: usize,
        ordblks: usize,
        smblks: usize,
        hblks: usize,
        hblkhd: usize,
        usmblks: usize,
        fsmblks: usize,
        uordblks: usize,
        fordblks: usize,
        keepcost: usize,
    }
    extern "C" {
        fn mallinfo2() -> Mallinfo2;
    }
    // Main arena only for `arena`; `uordblks` + `hblkhd` is all in use.
    let m = unsafe { mallinfo2() };
    format!(
        "heap in use {} MB, free in malloc {} MB, mmapped {} MB",
        (m.uordblks + m.hblkhd) >> 20,
        m.fordblks >> 20,
        m.hblkhd >> 20
    )
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn heap() -> String {
    String::new()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim() {
    extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    unsafe { malloc_trim(0) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim() {}

fn mem(what: &str) {
    println!(
        "{what:<28} rss {:>5} MB  peak {:>5} MB  {}",
        status("VmRSS:"),
        status("VmHWM:"),
        heap()
    );
}

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let mem_mb: Option<u64> = a.get(4).and_then(|m| m.parse().ok());
    mem("start");
    let mut opts = Options::default();
    if let Some(m) = mem_mb {
        opts = opts.memory_cap(m << 20);
    }
    let lib = Library::open_with_options(
        Path::new(&a[3]),
        8 << 30,
        Library::http_source(&a[1])?,
        opts,
    )?;
    let ix = lib.index(&a[2])?;
    mem("index open");
    let plan = lib.plan_keep(&ix)?;
    println!("plan {}", serde_json::to_string(&plan)?);
    mem("planned");
    let done = Arc::new(AtomicBool::new(false));
    let watch = {
        let done = done.clone();
        std::thread::spawn(move || {
            let mut n = 0;
            while !done.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(200));
                n += 1;
                if n % 25 == 0 {
                    mem("  keeping");
                }
            }
        })
    };
    let t = std::time::Instant::now();
    let r = lib.keep_offline_with(&ix, &Control::default())?;
    done.store(true, Ordering::SeqCst);
    watch.join().unwrap();
    println!(
        "kept: {} objects, {} bytes, {:.0} s",
        r.fetched,
        r.bytes_fetched,
        t.elapsed().as_secs_f64()
    );
    mem("kept");
    lib.clear_memory();
    ix.clear_memory();
    mem("clear_memory");
    drop(ix);
    drop(lib);
    mem("dropped");
    trim();
    mem("malloc_trim(0)");
    Ok(())
}
