//! Serve a directory over HTTP with Range support (the test server), for
//! trying `query-v2 --base` against a local staging tree:
//! `serve_tree <root>` where `<root>/v2` is the tree. Prints the address.
use strata_fetch::testing::TestServer;

fn main() {
    let root = std::env::args().nth(1).expect("usage: serve_tree <root>");
    let s = TestServer::start(std::path::Path::new(&root));
    println!("{}", s.addr);
    loop {
        std::thread::park();
    }
}
