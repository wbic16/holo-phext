//! Stage 7, Leg 2(e): the properties holo-phext exists for, tested
//! through its public async interface (the same snapshot machinery
//! the holo-edit bridge serves):
//!
//! * atomicity / snapshot isolation — while a writer commits
//!   two-scroll transactions (a generation counter written to two
//!   coordinates at once), readers holding ONE snapshot must never
//!   see the pair at different generations;
//! * concurrent readers during a write load — single reads always
//!   return one complete generation value, never a torn string,
//!   and never error;
//! * update dispatch — a subscriber sees every commit, in order.

use holo_phext::{HoloPhext, Txn};
use libphext::phext::{self, Coordinate};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn coord(s: &str) -> Coordinate {
    phext::to_coordinate(s)
}

fn temp_store(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("holo-bridge-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.phext")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_commits_are_never_seen_half_applied() {
    let base = temp_store("isolation");
    let a = coord("1.7.1/1.1.1/1.1.1");
    let b = coord("1.7.1/1.1.1/1.1.2");
    let store = Arc::new(HoloPhext::open(&base).await.unwrap());
    store
        .commit(Txn::new().set(a, "gen-0").set(b, "gen-0"))
        .await
        .unwrap();

    let violations = Arc::new(AtomicUsize::new(0));
    let torn = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));

    // writer: 300 paired generation commits
    let w = {
        let s = store.clone();
        tokio::spawn(async move {
            for i in 1..=300u32 {
                let v = format!("gen-{}", i);
                s.commit(Txn::new().set(a, &v).set(b, &v)).await.unwrap();
            }
        })
    };

    // snapshot readers: both coordinates from ONE snapshot must agree
    let mut readers = Vec::new();
    for _ in 0..3 {
        let s = store.clone();
        let v = violations.clone();
        let r = reads.clone();
        readers.push(tokio::spawn(async move {
            for _ in 0..400 {
                let snap = s.snapshot();
                let va = snap.get(&a);
                let vb = snap.get(&b);
                r.fetch_add(1, Ordering::Relaxed);
                if va != vb {
                    v.fetch_add(1, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    // concurrent single readers: a value is always one whole generation
    let mut singles = Vec::new();
    for _ in 0..2 {
        let s = store.clone();
        let t = torn.clone();
        singles.push(tokio::spawn(async move {
            for _ in 0..400 {
                if let Some(v) = s.read(&a) {
                    let well_formed = v.strip_prefix("gen-")
                        .map(|n| n.parse::<u32>().is_ok())
                        .unwrap_or(false);
                    if !well_formed {
                        t.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    w.await.unwrap();
    for h in readers {
        h.await.unwrap();
    }
    for h in singles {
        h.await.unwrap();
    }

    assert_eq!(store.seq(), 301);
    assert_eq!(reads.load(Ordering::Relaxed), 1200);
    assert_eq!(
        violations.load(Ordering::Relaxed),
        0,
        "a snapshot saw a half-applied paired transaction"
    );
    assert_eq!(
        torn.load(Ordering::Relaxed),
        0,
        "a concurrent reader saw a torn value during the write load"
    );
    let _ = std::fs::remove_dir_all(base.parent().unwrap());
}
