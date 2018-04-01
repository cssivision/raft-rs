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
}