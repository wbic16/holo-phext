//! The lattice index: one scan over a phext buffer builds a
//! coordinate → byte-span map, after which every lookup is O(1).
//! This is the same memory-mapped lattice design as phext-lattice's
//! `src/index.rs` (LatticeIndex), kept small: HoloPhext is where the
//! design lives now, and phext-lattice will consume it from here.

use libphext::phext::Coordinate;
use std::collections::HashMap;

/// A byte range inside the mapped buffer holding one scroll's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrollSpan {
    pub start: usize,
    pub end: usize,
}

impl ScrollSpan {
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

/// Coordinate → span index over one immutable phext buffer, plus the
/// coordinates in ascending order for range walks.
#[derive(Debug, Default)]
pub struct LatticeIndex {
    spans: HashMap<Coordinate, ScrollSpan>,
    ordered: Vec<Coordinate>,
}

fn is_dimension_break(b: u8) -> bool {
    matches!(b, 0x01 | 0x17 | 0x18 | 0x19 | 0x1A | 0x1C | 0x1D | 0x1E | 0x1F)
}

fn apply_break(coord: &mut Coordinate, byte: u8) {
    match byte {
        0x01 => coord.library_break(),
        0x1F => coord.shelf_break(),
        0x1E => coord.series_break(),
        0x1D => coord.collection_break(),
        0x1C => coord.volume_break(),
        0x1A => coord.book_break(),
        0x19 => coord.chapter_break(),
        0x18 => coord.section_break(),
        0x17 => coord.scroll_break(),
        _ => {}
    }
}

impl LatticeIndex {
    /// One pass over `buffer`. Empty runs between delimiter bytes are
    /// addressing, not scrolls, and get no span (a scroll exists iff it
    /// holds at least one byte — the phext definition).
    pub fn build(buffer: &[u8]) -> LatticeIndex {
        let mut spans = HashMap::new();
        let mut ordered = Vec::new();
        let mut coord = libphext::phext::default_coordinate();
        let mut run_start: Option<usize> = None;
        let mut run_coord = coord;
        for (i, &b) in buffer.iter().enumerate() {
            if is_dimension_break(b) {
                if let Some(start) = run_start.take() {
                    spans.insert(run_coord, ScrollSpan { start, end: i });
                    ordered.push(run_coord);
                }
                apply_break(&mut coord, b);
            } else if run_start.is_none() {
                run_start = Some(i);
                run_coord = coord;
            }
        }
        if let Some(start) = run_start {
            spans.insert(run_coord, ScrollSpan { start, end: buffer.len() });
            ordered.push(run_coord);
        }
        LatticeIndex { spans, ordered }
    }

    pub fn get(&self, coord: &Coordinate) -> Option<&ScrollSpan> {
        self.spans.get(coord)
    }

    pub fn contains(&self, coord: &Coordinate) -> bool {
        self.spans.contains_key(coord)
    }

    pub fn scroll_count(&self) -> usize {
        self.ordered.len()
    }

    pub fn coordinates(&self) -> &[Coordinate] {
        &self.ordered
    }

    /// The first populated coordinate strictly after `coord`.
    pub fn next_populated(&self, coord: &Coordinate) -> Option<Coordinate> {
        match self.ordered.binary_search(coord) {
            Ok(i) => self.ordered.get(i + 1).copied(),
            Err(i) => self.ordered.get(i).copied(),
        }
    }
}
