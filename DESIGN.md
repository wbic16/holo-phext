# HoloPhext — design

**The rule (Will, 2026-11-15): when phext cannot solve an integration,
build a layer2 protocol.** libphext v0.6.2 (PR #14) marked the exact
boundary: a string library can build commit markers and checked
scrolls, but it cannot make a write atomic, durable, or visible to
concurrent readers — those are storage semantics. HoloPhext is the
layer that owns them, so no phext host re-derives them again.

## Pieces

1. **The lattice** (`src/index.rs`) — one scan over a memory-mapped
   phext buffer builds a coordinate → byte-span index; reads are O(1)
   span slices. This is phext-lattice's `LatticeIndex` design; it
   lives here now, and phext-lattice will consume it from here.
2. **The core** (`src/core.rs`) — a memory-mapped base document plus
   an append-only phext WAL plus in-memory snapshot overlays.
3. **The async interface** (`src/lib.rs`) — a single writer task with
   group commit; `commit()` resolves with a sequence number;
   `snapshot()` is two Arc clones; `subscribe()` receives every
   committed transaction as an `Update`, in order (**update
   dispatch**).
4. **The compat facade** (`src/compat.rs`) — the `MappedLattice`
   surface on top of the core (see the fit table below).

## ACID, letter by letter

* **Atomicity** — a transaction is WAL entries (libphext checked
  scrolls) plus a commit marker (a libphext commit scroll). It is
  applied iff every entry verifies and the marker verifies, both at
  commit time and at recovery. A torn tail is truncated at open.
  Tested: forged entry without a marker; WAL torn inside a marker.
* **Consistency** — commits enforce the phext invariants: non-empty
  values for sets (an empty scroll does not exist; deletion is its
  own op), strictly ordered WAL addressing, verified integrity
  trailers. No committed state is an invalid phext — the base file
  after any checkpoint is readable by plain libphext.
* **Isolation** — snapshot reads. A commit builds a new overlay
  generation and swaps one Arc; a reader holding a snapshot sees all
  of a transaction or none of it, forever. Writers serialize through
  the single writer task (no lock ordering, no write-write
  conflicts by construction).
* **Durability** — the WAL suffix is written and `sync_data`'d
  before any op is applied or acknowledged (group commit: one sync
  per batch). Checkpoints follow the libphext durability profile:
  temp file, sync, rename, directory sync; the WAL then restarts
  with only its meta scroll. The crash window between the base
  rename and the WAL restart is safe because ops are absolute
  (replay is idempotent).

## File layout

* `<name>.phext` — the base document. Pure user content, no engine
  metadata squatting in it.
* `<name>.wal.phext` — the log since the last checkpoint. Meta
  scroll at 1.1.1/1.1.1/1.1.1 (`format=`, `checkpoint_seq=`).
  Transaction `seq` occupies volume = seq, chapter = 2: entries at
  scrolls 1..=n, the commit marker at n+1. Entry bodies are
  `coord=` / `op=set|del` / blank / raw value, wrapped in a checked
  scroll.

Both files are phexts: inspectable with less/grep, readable by any
phext tool, verifiable with libphext alone.

## Fit: HoloStore

The round-2 re-implementation (wbic16/holostore, PHEXT.md) hand-built
exactly this shape — manifest + snapshot volume + WAL volume, group
commit, checkpoint fold — in 717 lines inside a KV server. HoloPhext
is that shape factored out and completed (snapshot isolation,
dispatch, torn-tail recovery):

| holo_phext (round 2, hand-rolled) | HoloPhext |
|---|---|
| manifest scroll (format/node/seq) | WAL meta scroll |
| snapshot volume, 1,000 keys/chapter | base lattice + checkpoint |
| WAL volume, one scroll per op | per-transaction volumes, checked entries + commit marker |
| group commit, 300 µs window | writer-task batching, one sync per batch |
| RESP SET/GET/DEL surface | unchanged — it would sit on `HoloPhext::commit`/`read` |

Adoption path for HoloStore: replace the hand-rolled store file
with a `HoloPhext` handle; the RESP layer and the benchmark harness
carry over untouched. Consensus remains HoloStore's own layer —
representation and durability are HoloPhext's; agreement is not.

## Fit: phext-lattice

phext-lattice's editor stack is built on `MappedLattice`
(src/mmap.rs there, 1,033 lines): mmap + copy-on-write overlay +
`save()` merge. The `compat` module mirrors that surface method for
method (table in `src/compat.rs`); `save()` = commit the staged
transaction + checkpoint, which is strictly stronger than the
current merge (the staged batch becomes crash-safe and, through the
async interface, broadcastable).

Re-implementation plan for phext-lattice, in order:
1. Depend on holo-phext; delete lattice's own `src/index.rs` in
   favor of `holo_phext::LatticeIndex` (same design, one home).
2. Re-implement `MappedLattice` as a thin wrapper over
   `holo_phext::compat::LatticeStore`, keeping its public API so
   `phext_edit` and the editor modules compile unchanged.
3. Move the web layer's save path onto the async interface, so
   edits become transactions and other sessions receive `Update`
   dispatches (the seed-cache and claims flows already want this).
Each step is gated on phext-lattice's existing test suite.

## Limits, stated

* The overlay is cloned per commit (snapshot generations are
  whole-map Arcs). Right for editor- and store-scale documents;
  a persistent (structural-sharing) map is the known next step if
  profiled workloads demand it.
* One writer per store file. Multi-writer consensus is a consumer
  concern (HoloStore's Accord), not this layer's.
* The WAL is held in memory as text (bounded by checkpoint policy)
  so appends are pure suffix writes via libphext.
