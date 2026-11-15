//! The HoloPhext core: a memory-mapped base lattice, an append-only
//! phext WAL, and snapshot overlays — the ACID machinery the format
//! layer (libphext) deliberately does not own.
//!
//! Files: `<name>.phext` is the base document (pure user content,
//! memory-mapped read-only, replaced wholesale at checkpoint) and
//! `<name>.wal.phext` is the transaction log since the last checkpoint.
//!
//! WAL layout (all addressing inside the WAL phext):
//! * meta scroll at 1.1.1/1.1.1/1.1.1 — `format=` + `checkpoint_seq=`
//! * transaction `seq`: entries at volume = seq, chapter = 2,
//!   scroll = 1..=n; the commit marker at scroll n+1
//! * each entry is a libphext *checked scroll* whose body is
//!   `coord=<address>` / `op=set|del` / blank / raw value
//! * the marker is a libphext *commit scroll* over the entries
//!
//! Atomicity: a transaction applies iff every entry verifies and the
//! commit marker verifies — a torn tail is truncated at recovery.
//! Isolation: readers hold a `Snapshot` (an Arc of the base lattice
//! plus an Arc of the overlay as of one sequence); a commit swaps the
//! overlay Arc once per transaction, so a reader sees all of a
//! transaction or none of it. Durability: the WAL suffix is written
//! and `sync_data`'d before any op is applied or acknowledged;
//! checkpoints follow the libphext durability profile (temp file,
//! sync, rename, directory sync).

use crate::index::LatticeIndex;
use libphext::phext::{self, Coordinate};
use memmap2::Mmap;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// One write inside a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Set(String),
    Delete,
}

/// A multi-scroll transaction: a set of ops applied atomically or
/// not at all.
#[derive(Debug, Clone, Default)]
pub struct Txn {
    pub ops: Vec<(Coordinate, Op)>,
}

impl Txn {
    pub fn new() -> Txn {
        Txn { ops: Vec::new() }
    }
    pub fn set(mut self, coord: Coordinate, content: &str) -> Txn {
        self.ops.push((coord, Op::Set(content.to_string())));
        self
    }
    pub fn delete(mut self, coord: Coordinate) -> Txn {
        self.ops.push((coord, Op::Delete));
        self
    }
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// The read-only memory-mapped base lattice.
pub struct BaseLattice {
    map: Option<Mmap>,
    index: LatticeIndex,
}

impl BaseLattice {
    pub fn open(path: &Path) -> io::Result<BaseLattice> {
        if !path.exists() {
            File::create(path)?;
        }
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(BaseLattice { map: None, index: LatticeIndex::default() });
        }
        let map = unsafe { Mmap::map(&file)? };
        let index = LatticeIndex::build(&map);
        Ok(BaseLattice { map: Some(map), index })
    }

    pub fn read_scroll(&self, coord: &Coordinate) -> Option<String> {
        let span = self.index.get(coord)?;
        let map = self.map.as_ref()?;
        std::str::from_utf8(&map[span.start..span.end]).ok().map(|s| s.to_string())
    }

    pub fn bytes(&self) -> &[u8] {
        self.map.as_deref().unwrap_or(&[])
    }

    pub fn coordinates(&self) -> &[Coordinate] {
        self.index.coordinates()
    }
}

/// Shared, snapshot-able read state. Writers swap Arcs; readers clone
/// them into a `Snapshot` and are thereafter immune to new commits.
pub struct Shared {
    base: RwLock<Arc<BaseLattice>>,
    overlay: RwLock<Arc<BTreeMap<Coordinate, Option<String>>>>,
    seq: RwLock<u64>,
}

impl Shared {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            base: self.base.read().unwrap().clone(),
            overlay: self.overlay.read().unwrap().clone(),
            seq: *self.seq.read().unwrap(),
        }
    }

    pub fn seq(&self) -> u64 {
        *self.seq.read().unwrap()
    }
}

/// A frozen read view at one sequence number.
pub struct Snapshot {
    base: Arc<BaseLattice>,
    overlay: Arc<BTreeMap<Coordinate, Option<String>>>,
    seq: u64,
}

impl Snapshot {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// The raw bytes of the base document under this snapshot.
    pub fn base_bytes(&self) -> &[u8] {
        self.base.bytes()
    }

    pub fn get(&self, coord: &Coordinate) -> Option<String> {
        if let Some(entry) = self.overlay.get(coord) {
            return entry.clone();
        }
        self.base.read_scroll(coord)
    }

    pub fn contains(&self, coord: &Coordinate) -> bool {
        self.get(coord).is_some()
    }

    /// Every populated coordinate in this snapshot, ascending.
    pub fn coordinates(&self) -> Vec<Coordinate> {
        let mut out: Vec<Coordinate> = self
            .base
            .coordinates()
            .iter()
            .copied()
            .filter(|c| !matches!(self.overlay.get(c), Some(None)))
            .collect();
        for (coord, value) in self.overlay.iter() {
            if value.is_some() && !self.base.coordinates().contains(coord) {
                out.push(*coord);
            }
        }
        out.sort();
        out
    }
}

fn meta_coord() -> Coordinate {
    phext::to_coordinate("1.1.1/1.1.1/1.1.1")
}

fn entry_coord(seq: u64, index: usize) -> Coordinate {
    phext::to_coordinate(&format!("1.1.1/1.{}.1/2.1.{}", seq, index))
}

fn meta_content(checkpoint_seq: u64) -> String {
    format!("format=holo-phext-wal/1\ncheckpoint_seq={}\n", checkpoint_seq)
}

fn wal_path_for(base: &Path) -> PathBuf {
    let name = base.file_name().unwrap_or_default().to_string_lossy().to_string();
    let stem = name.strip_suffix(".phext").unwrap_or(&name);
    base.with_file_name(format!("{}.wal.phext", stem))
}

fn is_dimension_break(b: u8) -> bool {
    matches!(b, 0x01 | 0x17 | 0x18 | 0x19 | 0x1A | 0x1C | 0x1D | 0x1E | 0x1F)
}

/// Byte offset where the delimiter run introducing `coord` begins.
fn scroll_start(text: &str, coord: Coordinate) -> usize {
    let mut at = phext::offset(text, coord);
    let bytes = text.as_bytes();
    while at > 0 && is_dimension_break(bytes[at - 1]) {
        at -= 1;
    }
    at
}

fn sync_dir(path: &Path) -> io::Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    File::open(dir)?.sync_all()
}

/// The synchronous engine. The async interface drives one `Core`
/// from a single writer task; the compat facade drives it directly.
pub struct Core {
    base_path: PathBuf,
    wal_path: PathBuf,
    /// Opened lazily on the first write: opening a store read-only
    /// must not create the WAL file beside the document.
    wal_file: Option<File>,
    wal_text: String,
    next_seq: u64,
    checkpoint_seq: u64,
    pub shared: Arc<Shared>,
}

impl Core {
    pub fn open(base_path: &Path) -> io::Result<Core> {
        let wal_path = wal_path_for(base_path);
        let base = BaseLattice::open(base_path)?;

        // ---- load or create the WAL ----
        let wal_existed = wal_path.exists();
        let mut wal_text = if wal_existed {
            let mut s = String::new();
            File::open(&wal_path)?.read_to_string(&mut s)?;
            s
        } else {
            let mut m = HashMap::new();
            m.insert(meta_coord(), meta_content(0));
            phext::implode(m)
        };
        let checkpoint_seq = phext::fetch(&wal_text, meta_coord())
            .lines()
            .find_map(|l| l.strip_prefix("checkpoint_seq="))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        // ---- replay: validate transaction by transaction ----
        let exploded = phext::explode(&wal_text);
        let mut by_seq: BTreeMap<u64, Vec<Coordinate>> = BTreeMap::new();
        for coord in exploded.keys() {
            if coord.y.collection == 1 && coord.x.chapter == 2 {
                by_seq.entry(coord.y.volume as u64).or_default().push(*coord);
            }
        }
        let mut overlay: BTreeMap<Coordinate, Option<String>> = BTreeMap::new();
        let mut applied_seq = checkpoint_seq;
        let mut truncate_at: Option<usize> = None;
        for (seq, mut coords) in by_seq {
            coords.sort();
            if seq != applied_seq + 1 {
                truncate_at = Some(scroll_start(&wal_text, coords[0]));
                break;
            }
            let (commit_at, entry_coords) = coords.split_last().unwrap();
            let mut ops: Vec<(Coordinate, Op)> = Vec::new();
            let mut valid = phext::verify_commit(&wal_text, entry_coords, *commit_at);
            if valid {
                for entry in entry_coords {
                    match phext::fetch_checked(&wal_text, *entry)
                        .and_then(|body| parse_entry(&body))
                    {
                        Some(op) => ops.push(op),
                        None => {
                            valid = false;
                            break;
                        }
                    }
                }
            }
            if !valid {
                truncate_at = Some(scroll_start(&wal_text, coords[0]));
                break;
            }
            for (coord, op) in ops {
                overlay.insert(coord, match op { Op::Set(v) => Some(v), Op::Delete => None });
            }
            applied_seq = seq;
        }
        if let Some(at) = truncate_at {
            wal_text.truncate(at);
            let f = OpenOptions::new().write(true).open(&wal_path)?;
            f.set_len(at as u64)?;
            f.sync_all()?;
        }

        let _ = wal_existed;

        let shared = Arc::new(Shared {
            base: RwLock::new(Arc::new(base)),
            overlay: RwLock::new(Arc::new(overlay)),
            seq: RwLock::new(applied_seq),
        });
        Ok(Core {
            base_path: base_path.to_path_buf(),
            wal_path,
            wal_file: None,
            wal_text,
            next_seq: applied_seq + 1,
            checkpoint_seq,
            shared,
        })
    }
}

fn parse_entry(body: &str) -> Option<(Coordinate, Op)> {
    let (head, value) = match body.split_once("\n\n") {
        Some((h, v)) => (h, Some(v)),
        None => (body, None),
    };
    let mut coord = None;
    let mut op = None;
    for line in head.lines() {
        if let Some(v) = line.strip_prefix("coord=") {
            coord = Some(phext::to_coordinate(v));
        }
        if let Some(v) = line.strip_prefix("op=") {
            op = Some(v.to_string());
        }
    }
    match (coord, op.as_deref()) {
        (Some(c), Some("set")) => value.map(|v| (c, Op::Set(v.to_string()))),
        (Some(c), Some("del")) => Some((c, Op::Delete)),
        _ => None,
    }
}

fn entry_body(coord: &Coordinate, op: &Op) -> String {
    match op {
        Op::Set(value) => format!("coord={}\nop=set\n\n{}", coord, value),
        Op::Delete => format!("coord={}\nop=del", coord),
    }
}

impl Core {
    fn wal_file(&mut self) -> io::Result<&mut File> {
        if self.wal_file.is_none() {
            self.wal_file = Some(OpenOptions::new().append(true).create(true).open(&self.wal_path)?);
        }
        Ok(self.wal_file.as_mut().unwrap())
    }

    pub fn seq(&self) -> u64 {
        self.shared.seq()
    }

    pub fn checkpoint_seq(&self) -> u64 {
        self.checkpoint_seq
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Commits a batch of transactions: all of them are serialized
    /// into the WAL, written as one suffix, and synced ONCE; only
    /// then is each transaction applied, in order, one overlay swap
    /// per transaction. Returns the sequence of each transaction.
    /// This is the group-commit point the async writer drives.
    pub fn commit_batch(&mut self, txns: Vec<Txn>) -> io::Result<Vec<u64>> {
        let mut seqs = Vec::with_capacity(txns.len());
        let start_seq = self.next_seq;
        let old_len = self.wal_text.len();
        let mut text = std::mem::take(&mut self.wal_text);
        for txn in &txns {
            if txn.is_empty() {
                self.wal_text = text;
                self.next_seq = start_seq;
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty transaction"));
            }
            let seq = self.next_seq;
            let mut entries: Vec<(Coordinate, String)> = Vec::with_capacity(txn.ops.len() + 1);
            for (i, (coord, op)) in txn.ops.iter().enumerate() {
                if let Op::Set(v) = op {
                    if v.is_empty() {
                        self.wal_text = text;
                        self.next_seq = start_seq;
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "a set with an empty value would delete the scroll; use Op::Delete",
                        ));
                    }
                }
                entries.push((entry_coord(seq, i + 1), phext::checked_scroll(&entry_body(coord, op))));
            }
            let pairs: Vec<(Coordinate, &str)> =
                entries.iter().map(|(c, s)| (*c, s.as_str())).collect();
            let marker = phext::commit_scroll(&pairs);
            entries.push((entry_coord(seq, txn.ops.len() + 1), marker));
            let refs: Vec<(Coordinate, &str)> =
                entries.iter().map(|(c, s)| (*c, s.as_str())).collect();
            text = match phext::append_batch(text, &refs) {
                Some(t) => t,
                None => {
                    // Unreachable by construction (sequences strictly
                    // increase); if it ever fires, restore the WAL
                    // text from disk rather than trust memory.
                    self.wal_text = std::fs::read_to_string(&self.wal_path)?;
                    self.next_seq = start_seq;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WAL append rejected: coordinate order",
                    ));
                }
            };
            seqs.push(seq);
            self.next_seq += 1;
        }
        // one write, one sync — then, and only then, apply
        let wal = self.wal_file()?;
        wal.write_all(&text.as_bytes()[old_len..])?;
        wal.sync_data()?;
        self.wal_text = text;
        for (txn, &seq) in txns.iter().zip(&seqs) {
            let mut next = (**self.shared.overlay.read().unwrap()).clone();
            for (coord, op) in &txn.ops {
                next.insert(*coord, match op {
                    Op::Set(v) => Some(v.clone()),
                    Op::Delete => None,
                });
            }
            *self.shared.overlay.write().unwrap() = Arc::new(next);
            *self.shared.seq.write().unwrap() = seq;
        }
        Ok(seqs)
    }

    /// Folds the overlay into the base file (temp + sync + rename +
    /// directory sync), remaps the base, and restarts the WAL with
    /// only its meta scroll. Crash between the rename and the WAL
    /// rewrite is safe: replaying already-folded transactions onto
    /// the new base is idempotent, because ops are absolute.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        let snapshot = self.shared.snapshot();
        let base_text = std::str::from_utf8(snapshot.base.bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut map: HashMap<Coordinate, String> = phext::explode(base_text);
        for (coord, value) in snapshot.overlay.iter() {
            match value {
                Some(v) => {
                    map.insert(*coord, v.clone());
                }
                None => {
                    map.remove(coord);
                }
            }
        }
        let text = phext::implode(map);
        let tmp = self.base_path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(text.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.base_path)?;
        sync_dir(&self.base_path)?;

        self.checkpoint_seq = snapshot.seq;
        let mut m = HashMap::new();
        m.insert(meta_coord(), meta_content(self.checkpoint_seq));
        self.wal_text = phext::implode(m);
        let wal = self.wal_file()?;
        wal.set_len(0)?;
        wal.seek(SeekFrom::Start(0))?;
        wal.write_all(self.wal_text.as_bytes())?;
        wal.sync_all()?;

        *self.shared.base.write().unwrap() = Arc::new(BaseLattice::open(&self.base_path)?);
        *self.shared.overlay.write().unwrap() = Arc::new(BTreeMap::new());
        Ok(())
    }
}
