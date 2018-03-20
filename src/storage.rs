use raftpb::{ConfState, Entry, HardState, Snapshot};
use errors::Result;

#[derive(Debug, Clone)]
pub struct RaftState {
    pub hard_state: HardState,
    pub conf_state: ConfState,
}

pub trait Storage {
    /// initial_state returns the RaftState information
    fn initial_state(&self) -> Result<RaftState>;
    /// entries returns a slice of log entries in the range [lo,hi).
    /// max_size limits the total size of the log entries returned, but
    /// entries returns at least one entry if any.
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>>;
    /// term returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    fn term(&self, idx: u64) -> Result<u64>;
    /// first_index returns the index of the first log entry that is
    /// possible available via entries (older entries have been incorporated
    /// into the latest snapshot; if storage only contains the dummy entry the
    /// first log entry is not available).
    fn first_index(&self) -> Result<u64>;
    /// last_index returns the index of the last entry in the log.
    fn last_index(&self) -> Result<u64>;
    /// snapshot returns the most recent snapshot.
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    fn snapshot(&self) -> Result<Snapshot>;
}