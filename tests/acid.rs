//! ACID acceptance tests for the public async interface, plus the
//! crash-recovery cases that define the engine's contract.

use holo_phext::{HoloPhext, Op, Txn};
use libphext::phext::{self, Coordinate};
use std::path::PathBuf;

fn coord(s: &str) -> Coordinate {
    phext::to_coordinate(s)
}

fn temp_store(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("holo-phext-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.phext")
}

fn cleanup(base: &PathBuf) {
    let dir = base.parent().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn commit_read_back_and_reopen() {
    let base = temp_store("basic");
    let a = coord("1.1.1/1.1.1/1.1.1");
    let b = coord("1.1.1/1.1.1/1.1.2");
    {
        let store = HoloPhext::open(&base).await.unwrap();
        let seq = store.commit(Txn::new().set(a, "alpha").set(b, "beta")).await.unwrap();
        assert_eq!(seq, 1);
        assert_eq!(store.read(&a).as_deref(), Some("alpha"));
        assert_eq!(store.read(&b).as_deref(), Some("beta"));
    }
    {
        let store = HoloPhext::open(&base).await.unwrap();
        assert_eq!(store.seq(), 1);
        assert_eq!(store.read(&a).as_deref(), Some("alpha"));
        assert_eq!(store.read(&b).as_deref(), Some("beta"));
    }
    cleanup(&base);
}

#[tokio::test]
async fn snapshot_isolation_holds_across_commits() {
    let base = temp_store("snapshot");
    let a = coord("1.1.1/1.1.1/1.1.1");
    let store = HoloPhext::open(&base).await.unwrap();
    store.commit(Txn::new().set(a, "v1")).await.unwrap();
    let frozen = store.snapshot();
    store.commit(Txn::new().set(a, "v2")).await.unwrap();
    assert_eq!(frozen.seq(), 1);
    assert_eq!(frozen.get(&a).as_deref(), Some("v1"), "a snapshot never sees a later commit");
    assert_eq!(store.read(&a).as_deref(), Some("v2"));
    cleanup(&base);
}

#[tokio::test]
async fn subscribers_receive_every_commit_in_order() {
    let base = temp_store("dispatch");
    let store = HoloPhext::open(&base).await.unwrap();
    let mut rx = store.subscribe();
    for i in 1..=5u64 {
        let c = coord(&format!("1.1.1/1.1.1/1.1.{}", i));
        store.commit(Txn::new().set(c, "x")).await.unwrap();
    }
    for expected in 1..=5u64 {
        let update = rx.recv().await.unwrap();
        assert_eq!(update.seq, expected);
        assert_eq!(update.changes.len(), 1);
        assert!(matches!(update.changes[0].1, Op::Set(_)));
    }
    cleanup(&base);
}

#[tokio::test]
async fn concurrent_commits_serialize_with_unique_sequences() {
    let base = temp_store("concurrent");
    let store = std::sync::Arc::new(HoloPhext::open(&base).await.unwrap());
    let mut handles = Vec::new();
    for i in 0..40usize {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            let c = coord(&format!("1.1.1/1.1.1/1.{}.{}", i / 9 + 1, i % 9 + 1));
            s.commit(Txn::new().set(c, "v")).await.unwrap()
        }));
    }
    let mut seqs: Vec<u64> = Vec::new();
    for h in handles {
        seqs.push(h.await.unwrap());
    }
    seqs.sort();
    assert_eq!(seqs, (1..=40).collect::<Vec<u64>>(), "every commit got a unique sequence");
    assert_eq!(store.snapshot().coordinates().len(), 40);
    cleanup(&base);
}

#[tokio::test]
async fn deletes_and_checkpoint_survive_reopen() {
    let base = temp_store("checkpoint");
    let a = coord("1.1.1/1.1.1/1.1.1");
    let b = coord("1.1.1/1.1.1/1.1.2");
    {
        let store = HoloPhext::open(&base).await.unwrap();
        store.commit(Txn::new().set(a, "keep").set(b, "drop")).await.unwrap();
        store.commit(Txn::new().delete(b)).await.unwrap();
        store.checkpoint().await.unwrap();
        assert_eq!(store.read(&b), None);
    }
    // after a checkpoint the base file IS the document: readable by
    // plain libphext, with the delete already folded in
    let text = std::fs::read_to_string(&base).unwrap();
    assert_eq!(phext::fetch(&text, a), "keep");
    assert_eq!(phext::fetch(&text, b), "");
    let wal = std::fs::read_to_string(base.with_file_name("store.wal.phext")).unwrap();
    assert!(wal.contains("checkpoint_seq=2"), "WAL restarted at the checkpoint: {}", wal);
    let store = HoloPhext::open(&base).await.unwrap();
    assert_eq!(store.read(&a).as_deref(), Some("keep"));
    assert_eq!(store.seq(), 2, "sequence survives the checkpoint");
    cleanup(&base);
}

#[tokio::test]
async fn a_transaction_without_a_commit_marker_is_discarded() {
    let base = temp_store("atomicity");
    let a = coord("1.1.1/1.1.1/1.1.1");
    let ghost = coord("1.1.1/1.1.1/1.1.9");
    {
        let store = HoloPhext::open(&base).await.unwrap();
        store.commit(Txn::new().set(a, "real")).await.unwrap();
    }
    // forge a torn transaction directly in the WAL: one checked
    // entry at the next sequence, no commit marker after it
    let wal_path = base.with_file_name("store.wal.phext");
    let wal_text = std::fs::read_to_string(&wal_path).unwrap();
    let body = format!("coord={}\nop=set\n\nghost", ghost);
    let entry_at = phext::to_coordinate("1.1.1/1.2.1/2.1.1");
    let forged = phext::append(wal_text, entry_at, &phext::checked_scroll(&body)).unwrap();
    std::fs::write(&wal_path, &forged).unwrap();

    let store = HoloPhext::open(&base).await.unwrap();
    assert_eq!(store.read(&a).as_deref(), Some("real"));
    assert_eq!(store.read(&ghost), None, "an uncommitted transaction must not exist after recovery");
    assert_eq!(store.seq(), 1);
    // and the WAL was truncated back to the valid prefix
    let wal_after = std::fs::read_to_string(&wal_path).unwrap();
    assert!(!wal_after.contains("ghost"), "torn tail truncated at recovery");
    cleanup(&base);
}

#[tokio::test]
async fn a_torn_commit_marker_is_discarded() {
    let base = temp_store("torn-commit");
    let a = coord("1.1.1/1.1.1/1.1.1");
    let b = coord("1.1.1/1.1.1/1.1.2");
    {
        let store = HoloPhext::open(&base).await.unwrap();
        store.commit(Txn::new().set(a, "one")).await.unwrap();
        store.commit(Txn::new().set(b, "two")).await.unwrap();
    }
    // tear the WAL inside the second transaction's commit marker
    let wal_path = base.with_file_name("store.wal.phext");
    let wal_text = std::fs::read_to_string(&wal_path).unwrap();
    let marker_at = phext::to_coordinate("1.1.1/1.2.1/2.1.2");
    let tear = phext::offset(&wal_text, marker_at) + 10;
    std::fs::write(&wal_path, &wal_text[..tear]).unwrap();

    let store = HoloPhext::open(&base).await.unwrap();
    assert_eq!(store.read(&a).as_deref(), Some("one"), "the intact transaction survives");
    assert_eq!(store.read(&b), None, "the torn transaction is rejected whole — atomicity");
    assert_eq!(store.seq(), 1);
    cleanup(&base);
}

#[tokio::test]
async fn read_only_open_creates_no_wal_file() {
    let base = temp_store("lazy-wal");
    let a = coord("1.1.1/1.1.1/1.1.1");
    std::fs::write(&base, "seed content").unwrap();
    let wal = base.with_file_name("store.wal.phext");
    {
        let store = HoloPhext::open(&base).await.unwrap();
        assert_eq!(store.read(&a).as_deref(), Some("seed content"));
        assert!(!wal.exists(), "a read-only open must not create the WAL");
        store.commit(Txn::new().set(a, "updated")).await.unwrap();
        assert!(wal.exists(), "the first commit creates the WAL");
    }
    cleanup(&base);
}

#[tokio::test]
async fn recovery_over_many_transactions_is_linear() {
    // Regression: recovery once verified each transaction by
    // fetching from the raw WAL text — a whole-document parse per
    // transaction, quadratic in the log. 3,000 transactions must
    // reopen with every value intact, inside ordinary test time.
    let dir = tempdir();
    let path = dir.join("many.phext");
    let store = HoloPhext::open(&path).await.unwrap();
    for i in 0..3_000u32 {
        store
            .commit(Txn::new().set(c(9, 9, 9), &format!("v{i}")))
            .await
            .unwrap();
    }
    drop(store);
    let store = HoloPhext::open(&path).await.unwrap();
    assert_eq!(store.seq(), 3_000);
    assert_eq!(store.read(&c(9, 9, 9)).as_deref(), Some("v2999"));
}
