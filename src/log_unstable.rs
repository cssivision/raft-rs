use raftpb::{Snapshot, Entry};

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
#[derive(Debug, PartialEq, Default)]
pub struct Unstable {
    pub snapshot: Option<Snapshot>,
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
}