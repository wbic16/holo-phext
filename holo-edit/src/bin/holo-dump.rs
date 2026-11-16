//! holo-dump — parity listing for a HoloPhext store: one line per
//! scroll, `<coordinate>\t<sha256 of content>\t<byte length>`,
//! sorted by coordinate. Compared against an independent listing
//! built from the same base file with libphext (Python port) to
//! prove a migration lossless over the full corpus.

use holo_phext::core::Core;
use sha2::{Digest, Sha256};
use std::path::Path;

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = Path::new(args.get(1).expect("usage: holo-dump <base.phext>"));
    let core = Core::open(path).expect("open holo-phext core");
    let snap = core.shared.snapshot();
    let coords = snap.coordinates();
    for c in &coords {
        let v = snap.get(c).expect("coordinate listed but unreadable");
        let digest = Sha256::digest(v.as_bytes());
        println!("{}\t{}\t{}", c, hex(&digest), v.len());
    }
    eprintln!("seq={} count={}", snap.seq(), coords.len());
}
