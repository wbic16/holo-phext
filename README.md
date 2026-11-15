# holo-phext

The layer2 protocol over [phext](https://github.com/wbic16/libphext-rs):
a memory-mapped lattice with **async I/O**, **update dispatch**, and
**ACID multi-scroll transactions**.

Phext gives every scroll an address and a text body. It does not —
cannot, as a format — make a multi-scroll write atomic, durable, or
visible to concurrent readers. HoloPhext owns that layer:

* base document in a memory-mapped file behind a one-scan
  coordinate→span index (the phext-lattice lattice design);
* transactions serialized into an append-only **phext WAL** —
  checked entry scrolls plus a commit marker, one write and one
  sync per group-committed batch, applied only after the sync;
* snapshot-isolated reads (a commit swaps one overlay Arc; readers
  see all of a transaction or none of it);
* every commit broadcast to subscribers as an `Update`, in order;
* checkpoints that fold the log into the base file (temp, sync,
  rename, directory sync) — after which the base is a plain phext
  any tool can read.

```rust
let store = HoloPhext::open(Path::new("store.phext")).await?;
let seq = store.commit(Txn::new()
    .set(coord_a, "alpha")
    .set(coord_b, "beta")).await?;   // atomic: both, or neither
let snap = store.snapshot();          // frozen at seq
let mut rx = store.subscribe();       // Update { seq, changes }
```

Design, ACID argument, and the fit for its two consumers (HoloStore's
storage core; phext-lattice, whose `MappedLattice` API is mirrored in
`src/compat.rs` ahead of its re-implementation on this engine):
[DESIGN.md](DESIGN.md).
