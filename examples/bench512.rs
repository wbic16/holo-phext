//! bench512 — Boardy's bar, measured.
//!
//! Modes:
//!   gen <path> <scrolls> <scroll_bytes>
//!       Deterministically generate a base phext.
//!   run <src_base> <work_dir> <commits> <checkpoint_every> <producers>
//!       <value_bytes> <csv_prefix> <max_secs>
//!       Copy src_base into work_dir, open the store, drive a
//!       write-heavy stream of single-set commits at fresh
//!       coordinates from <producers> concurrent producers, drive
//!       checkpoint() every <checkpoint_every> acked commits, record
//!       per-commit latencies + checkpoint windows as CSV.
//!   crashwriter <src_base> <work_dir> <ack_file> <value_bytes>
//!       Commit in a loop, appending each acked seq to ack_file.
//!       Meant to be kill -9'd mid-run.
//!   crashverify <work_dir> <ack_file> <value_bytes>
//!       Reopen (recovery replays), compare store seq to the last
//!       acked seq, spot-check values, reopen a second time and
//!       confirm the state is identical (replay idempotence).
//!   verify <base_path> <expected_scrolls> <scroll_bytes>
//!       libphext-only: explode the base file, count scrolls,
//!       spot-check generator content. No engine code involved.

use holo_phext::{HoloPhext, Txn};
use libphext::phext::{self, Coordinate};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn base_coord(i: u64) -> Coordinate {
    phext::to_coordinate(&format!("1.1.1/1.{}.1/1.1.{}", 1 + i / 128, 1 + i % 128))
}

fn commit_coord(k: u64) -> Coordinate {
    phext::to_coordinate(&format!("3.1.1/1.{}.1/1.1.{}", 1 + k / 1000, 1 + k % 1000))
}

fn scroll_content(i: u64, bytes: usize) -> String {
    let mut s = format!("scroll {:08} deterministic bench content. ", i);
    while s.len() < bytes {
        s.push_str("the quick brown fox jumps over the lazy dog. ");
    }
    s.truncate(bytes);
    s
}

fn value_content(k: u64, bytes: usize) -> String {
    let mut s = format!("val-{:08}-", k);
    while s.len() < bytes {
        s.push('x');
    }
    s.truncate(bytes);
    s
}

fn gen(path: &Path, scrolls: u64, scroll_bytes: usize) {
    let t0 = Instant::now();
    let mut map: HashMap<Coordinate, String> = HashMap::with_capacity(scrolls as usize);
    for i in 0..scrolls {
        map.insert(base_coord(i), scroll_content(i, scroll_bytes));
    }
    let text = phext::implode(map);
    std::fs::write(path, &text).unwrap();
    println!(
        "gen: {} scrolls x {} B -> {} bytes in {:?}",
        scrolls,
        scroll_bytes,
        text.len(),
        t0.elapsed()
    );
}

struct RunCfg {
    commits: u64,
    checkpoint_every: u64,
    producers: usize,
    value_bytes: usize,
    max_secs: u64,
}

async fn run(src: &Path, work: &Path, cfg: RunCfg, prefix: &str) {
    std::fs::create_dir_all(work).unwrap();
    let base = work.join("bench.phext");
    std::fs::copy(src, &base).unwrap();
    let wal = work.join("bench.wal.phext");

    let store = Arc::new(HoloPhext::open(&base).await.unwrap());
    let t0 = Instant::now();
    let next_idx = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // ---- checkpoint driver ----
    let cp_csv = Arc::new(std::sync::Mutex::new(String::from(
        "start_ms,end_ms,dur_ms,wal_bytes_before,acked_before\n",
    )));
    let cp_task = {
        let store = store.clone();
        let acked = acked.clone();
        let stop = stop.clone();
        let cp_csv = cp_csv.clone();
        let wal = wal.clone();
        let every = cfg.checkpoint_every;
        let total = cfg.commits;
        tokio::spawn(async move {
            if every == 0 {
                return;
            }
            let mut next_cp = every;
            loop {
                while acked.load(Ordering::SeqCst) < next_cp && !stop.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                if stop.load(Ordering::SeqCst) || acked.load(Ordering::SeqCst) >= total {
                    break;
                }
                let wal_bytes = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
                let acked_before = acked.load(Ordering::SeqCst);
                let start = t0.elapsed().as_millis();
                let ct = Instant::now();
                store.checkpoint().await.unwrap();
                let dur = ct.elapsed().as_millis();
                let end = t0.elapsed().as_millis();
                cp_csv.lock().unwrap().push_str(&format!(
                    "{},{},{},{},{}\n",
                    start, end, dur, wal_bytes, acked_before
                ));
                next_cp += every;
            }
        })
    };

    // ---- producers ----
    let mut handles = Vec::new();
    for pid in 0..cfg.producers {
        let store = store.clone();
        let next_idx = next_idx.clone();
        let acked = acked.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let mut rows = String::new();
            loop {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let k = next_idx.fetch_add(1, Ordering::SeqCst);
                if k >= cfg.commits {
                    break;
                }
                let txn = Txn::new().set(commit_coord(k), &value_content(k, cfg.value_bytes));
                let ct = Instant::now();
                let seq = store.commit(txn).await.unwrap();
                let lat = ct.elapsed().as_micros();
                let at = t0.elapsed().as_millis();
                rows.push_str(&format!("{},{},{},{},{}\n", k, seq, pid, lat, at));
                acked.fetch_add(1, Ordering::SeqCst);
            }
            rows
        }));
    }

    // wall-clock cap watchdog
    let watchdog = {
        let stop = stop.clone();
        let acked = acked.clone();
        let total = cfg.commits;
        tokio::spawn(async move {
            let cap = Duration::from_secs(cfg.max_secs);
            while t0.elapsed() < cap {
                if acked.load(Ordering::SeqCst) >= total {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            eprintln!("bench512: wall-clock cap hit; stopping producers");
            stop.store(true, Ordering::SeqCst);
        })
    };

    let mut lat_csv = String::from("idx,seq,producer,latency_us,acked_at_ms\n");
    for h in handles {
        lat_csv.push_str(&h.await.unwrap());
    }
    stop.store(true, Ordering::SeqCst);
    let _ = cp_task.await;
    watchdog.abort();

    // final checkpoint (timed, recorded separately)
    let wal_bytes = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let ct = Instant::now();
    store.checkpoint().await.unwrap();
    let final_cp_ms = ct.elapsed().as_millis();

    let done = acked.load(Ordering::SeqCst);
    std::fs::write(format!("{}-latency.csv", prefix), &lat_csv).unwrap();
    std::fs::write(format!("{}-checkpoints.csv", prefix), cp_csv.lock().unwrap().as_str()).unwrap();
    println!(
        "run: {} commits acked of {} in {:?} ({:.1} commits/s); final checkpoint {} ms (wal {} B); base {} B",
        done,
        cfg.commits,
        t0.elapsed(),
        done as f64 / t0.elapsed().as_secs_f64(),
        final_cp_ms,
        wal_bytes,
        std::fs::metadata(&base).map(|m| m.len()).unwrap_or(0)
    );
}

async fn crashwriter(src: &Path, work: &Path, ack_file: &Path, value_bytes: usize) {
    std::fs::create_dir_all(work).unwrap();
    let base = work.join("bench.phext");
    std::fs::copy(src, &base).unwrap();
    let store = HoloPhext::open(&base).await.unwrap();
    let mut ack = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(ack_file)
        .unwrap();
    let mut k: u64 = 0;
    loop {
        let txn = Txn::new().set(commit_coord(k), &value_content(k, value_bytes));
        let seq = store.commit(txn).await.unwrap();
        writeln!(ack, "{}", seq).unwrap();
        ack.sync_data().unwrap();
        k += 1;
    }
}

async fn crashverify(work: &Path, ack_file: &Path, value_bytes: usize) {
    let base = work.join("bench.phext");
    let acked_max: u64 = std::fs::read_to_string(ack_file)
        .unwrap()
        .lines()
        .filter_map(|l| l.parse().ok())
        .max()
        .unwrap_or(0);
    // single producer: seq n corresponds to commit index n-1
    let check = |snap: &holo_phext::Snapshot| -> (u64, usize, usize) {
        let mut ok = 0usize;
        let mut bad = 0usize;
        let mut probes: Vec<u64> = vec![0, 1, 2];
        if acked_max > 10 {
            probes.push(acked_max / 2);
            probes.push(acked_max - 2);
            probes.push(acked_max - 1);
        }
        for k in probes {
            if k >= acked_max {
                continue;
            }
            match snap.get(&commit_coord(k)) {
                Some(v) if v == value_content(k, value_bytes) => ok += 1,
                _ => bad += 1,
            }
        }
        (snap.seq(), ok, bad)
    };
    let store = HoloPhext::open(&base).await.unwrap();
    let (seq1, ok1, bad1) = check(&store.snapshot());
    drop(store);
    // reopen: replaying the same WAL onto the recovered state must
    // change nothing (idempotence)
    let store2 = HoloPhext::open(&base).await.unwrap();
    let (seq2, ok2, bad2) = check(&store2.snapshot());
    println!(
        "crashverify: last acked seq={} recovered seq={} probes ok={} bad={} | reopen seq={} probes ok={} bad={}",
        acked_max, seq1, ok1, bad1, seq2, ok2, bad2
    );
    if seq1 < acked_max || bad1 > 0 || seq2 != seq1 || bad2 > 0 {
        std::process::exit(1);
    }
}

fn verify(path: &Path, base_scrolls: u64, expected_total: u64, scroll_bytes: usize) {
    let text = std::fs::read_to_string(path).unwrap();
    let map = phext::explode(&text);
    let mut bad = 0usize;
    for i in [0u64, 1, base_scrolls / 2, base_scrolls - 1] {
        match map.get(&base_coord(i)) {
            Some(v) if *v == scroll_content(i, scroll_bytes) => {}
            _ => bad += 1,
        }
    }
    println!(
        "verify: {} scrolls (expected total {}), {} bytes, base probe failures {}",
        map.len(),
        expected_total,
        text.len(),
        bad
    );
    if map.len() as u64 != expected_total || bad > 0 {
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "gen" => gen(Path::new(&args[2]), args[3].parse().unwrap(), args[4].parse().unwrap()),
        "run" => {
            let cfg = RunCfg {
                commits: args[4].parse().unwrap(),
                checkpoint_every: args[5].parse().unwrap(),
                producers: args[6].parse().unwrap(),
                value_bytes: args[7].parse().unwrap(),
                max_secs: args[9].parse().unwrap(),
            };
            run(Path::new(&args[2]), Path::new(&args[3]), cfg, &args[8]).await;
        }
        "crashwriter" => {
            crashwriter(Path::new(&args[2]), Path::new(&args[3]), Path::new(&args[4]), args[5].parse().unwrap()).await
        }
        "crashverify" => crashverify(Path::new(&args[2]), Path::new(&args[3]), args[4].parse().unwrap()).await,
        "verify" => verify(
            Path::new(&args[2]),
            args[3].parse().unwrap(),
            args[4].parse().unwrap(),
            args[5].parse().unwrap(),
        ),
        other => {
            eprintln!("unknown mode {}", other);
            std::process::exit(2);
        }
    }
}
