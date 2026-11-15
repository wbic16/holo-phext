//! The phext-lattice fit: `LatticeStore` must behave like
//! `MappedLattice` — staged overlay reads, revert, and a save that
//! leaves the complete merged document in the file at `path()`.

use holo_phext::compat::LatticeStore;
use libphext::phext::{self, Coordinate};
use std::path::PathBuf;

fn coord(s: &str) -> Coordinate {
    phext::to_coordinate(s)
}

fn temp_store(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("holo-phext-compat-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("doc.phext")
}

#[test]
fn lattice_store_mirrors_mapped_lattice() {
    let base = temp_store("facade");
    let a = coord("1.1.1/1.1.1/1.1.1");
    let b = coord("1.1.1/1.1.1/1.1.2");
    let mut store = LatticeStore::open(&base).unwrap();
    assert!(!store.is_dirty());
    assert_eq!(store.read_scroll(&a), None);

    // staged writes are readable before save, and revert drops them
    store.write_scroll(a, "draft".to_string());
    assert!(store.is_dirty());
    assert_eq!(store.pending_changes(), 1);
    assert_eq!(store.read_scroll(&a).as_deref(), Some("draft"));
    store.revert();
    assert!(!store.is_dirty());
    assert_eq!(store.read_scroll(&a), None);

    // save commits + checkpoints: the file at path() is the document
    store.write_scroll(a, "alpha".to_string());
    store.write_scroll(b, "beta".to_string());
    store.save().unwrap();
    assert!(!store.is_dirty());
    let text = std::fs::read_to_string(&base).unwrap();
    assert_eq!(phext::fetch(&text, a), "alpha");
    assert_eq!(phext::fetch(&text, b), "beta");
    assert_eq!(store.scroll_count(), 2);
    assert_eq!(store.populated_coordinates(), vec![a, b]);

    // a delete staged over a saved scroll reads as absent, and the
    // merged byte view agrees
    store.delete_scroll(a);
    assert!(!store.has_scroll(&a));
    let bytes = store.to_phext_bytes();
    let merged = String::from_utf8(bytes).unwrap();
    assert_eq!(phext::fetch(&merged, a), "");
    assert_eq!(phext::fetch(&merged, b), "beta");
    store.save().unwrap();

    // reopening sees exactly the saved state
    let store = LatticeStore::open(&base).unwrap();
    assert_eq!(store.read_scroll(&a), None);
    assert_eq!(store.read_scroll(&b).as_deref(), Some("beta"));
    let _ = std::fs::remove_dir_all(base.parent().unwrap());
}
