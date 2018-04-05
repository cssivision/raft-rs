use raftpb::{Snapshot, Entry};

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
#[derive(Debug, PartialEq, Default)]
pub struct Unstable {
    // the incoming unstable snapshot, if any.
    pub snapshot: Option<Snapshot>,
    // all entries that have not yet been written to storage.
    pub entries: Vec<Entry>,
    pub offset: u64,
    tag: String,
}

impl Unstable {
    pub fn new(offset: u64, tag: String) -> Unstable {
        Unstable{
            snapshot: None,
            entries: vec![],
            offset: offset,
            tag: tag,
        }
    }

    /// maybe_term returns the term of the entry at index i, if there
    /// is any.
    pub fn maybe_term(&self, i: u64) -> Option<u64> {
        if i < self.offset {
            if let Some(sn) = self.snapshot.as_ref() {
                return Some(sn.get_metadata().get_term());
            }
            return None;
        }

        if let Some(last_index) = self.maybe_last_index() {
            if i > last_index {
                return None;
            }
            return Some(self.entries[(i-self.offset) as usize].get_term());
        }
        None
    }

    /// maybe_first_index returns the index of the first possible entry in entries
    /// if it has a snapshot.
    pub fn maybe_first_index(&self) -> Option<u64> {
         self.snapshot
            .as_ref()
            .map(|snap| snap.get_metadata().get_index() + 1)
    }

    // maybe_last_index returns the last index if it has at least one
    // unstable entry or snapshot.
    pub fn maybe_last_index(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self.snapshot
                .as_ref()
                .map(|snap| snap.get_metadata().get_index()),
            len => Some(self.offset + len as u64 - 1),
        }
    } 

    pub fn truncate_and_append(&mut self, ents: &[Entry]) {
        if ents.is_empty() {
            return
        }
        let after = ents[0].get_index();
        if after == self.offset + self.entries.len() as u64 {
            self.entries.extend_from_slice(ents);
        } else if after <= self.offset {
            self.offset = after;
            self.entries.clear();
            self.entries.extend_from_slice(ents);
        } else {
            info!("truncate the unstable entries before index {}", after);
            let off = self.offset;
            self.must_check_out_of_bounds(off, after);
            self.entries.truncate((after-off) as usize);
            self.entries.extend_from_slice(ents);
        }
    }

    pub fn slice(&self, lo: u64, hi: u64) -> &[Entry] {
        self.must_check_out_of_bounds(lo, hi);
        let l = lo as usize;
        let h = hi as usize;
        let off = self.offset as usize;
        &self.entries[l - off..h - off]
    }

    pub fn must_check_out_of_bounds(&self, low: u64, hight: u64) {
        if low > hight {
            panic!("invlid unstable slice {} > {}", low, hight);
        }

        let upper = self.offset + self.entries.len() as u64;
        if low < self.offset || hight > upper {
            panic!(
                "unstable.slice[{},{}) out of bound [{},{}]",
                low, hight,
                self.offset, upper,
            );   
        }
    }
}