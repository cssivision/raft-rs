use std::u64;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use protobuf::Message;
use raftpb::{ConfState, Entry, HardState, Snapshot};
use errors::{Result, StorageError};

pub const NO_LIMIT: u64 = u64::MAX;

pub trait Storage {
    /// initial_state returns the saved HardState and ConfState information.
    fn initial_state(&self) -> Result<(HardState, ConfState)>;

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

pub struct MemStorageCore {
    hard_state: HardState,
    snapshot: Snapshot,
    entries: Vec<Entry>,
}

impl Default for MemStorageCore {
    fn default() -> MemStorageCore {
        MemStorageCore {
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            hard_state: HardState::new(),
            snapshot: Snapshot::new(),
        }
    }
}

impl MemStorageCore {
    fn inner_last_index(&self) -> u64 {
        self.entries[0].get_index() + self.entries.len() as u64 - 1
    }

    /// set_hardstate saves the current HardState.
    pub fn set_hard_state(&mut self, hs: HardState) {
        self.hard_state = hs;
    }

    /// apply_snapshot overwrites the contents of this Storage object with
    /// those of the given snapshot.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        // handle check for old snapshot being applied
        let index = self.snapshot.get_metadata().get_index();
        let snapshot_index = snapshot.get_metadata().get_index();
        if index >= snapshot_index {
            return Err(StorageError::SnapshotOutOfDate.into());
        }

        let mut e = Entry::new();
        e.set_term(snapshot.get_metadata().get_term());
        e.set_index(snapshot.get_metadata().get_index());
        self.entries = vec![e];
        self.snapshot = snapshot;
        Ok(())
    }

    /// Compact discards all log entries prior to compactIndex.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than raftLog.applied.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.entries[0].get_index();
        if compact_index <= offset {
            return Err(StorageError::ErrCompacted.into());
        }

        if compact_index > self.inner_last_index() {
            panic!("compact {} is out of bound lastindex({})", compact_index, self.inner_last_index());
        }

        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
        Ok(())
    } 

    pub fn append(&mut self, ents: &[Entry]) -> Result<()> { 
        if ents.is_empty() {
            return Ok(());
        }

        let first = self.entries[0].get_index() + 1;
        let last = ents[0].get_index() + ents.len() as u64 - 1;

        // shortcut if there is no new entry.
        if last < first {
            return Ok(());
        }

        // truncate compacted entries
        let te: &[Entry] = if first > ents[0].get_index() {
            let t = (first-ents[0].get_index()) as usize;
            &ents[t..]
        } else {
            ents
        };

        let offset = ents[0].get_index() - self.entries[0].get_index();

        if self.entries.len() as u64 > offset {
            let mut new_entries: Vec<Entry> = vec![];
            new_entries.extend_from_slice(&self.entries[..offset as usize]);
            new_entries.extend_from_slice(te);
            self.entries = new_entries;
        } else if self.entries.len() as u64 == offset {
            self.entries.extend_from_slice(te);
        } else {
            panic!(
                "missing log entry [last: {}, append at: {}]",
                self.inner_last_index(),
                te[0].get_index(),
            );
        }

        Ok(())
    }

    pub fn create_snapshot(
        &mut self, 
        index: u64, 
        cs: Option<ConfState>, 
        data: Vec<u8>,
    ) -> Result<&Snapshot> {
        if index <= self.snapshot.get_metadata().get_index() {
            return Err(StorageError::SnapshotOutOfDate.into());
        }

        let offset = self.entries[0].get_index();
        if index > self.inner_last_index() {
            panic!(
                "snapshot {} is out of bound lastindex({})",
                index,
                self.inner_last_index(),
            )
        }

        self.snapshot.mut_metadata().set_index(index);
        self.snapshot.mut_metadata().set_term(self.entries[(index - offset) as usize].get_term());

        if let Some(cs) = cs {
            self.snapshot.mut_metadata().set_conf_state(cs);
        }

        self.snapshot.set_data(data);
        Ok(&self.snapshot)
    }
}

/// `MemStorage` is a thread-safe implementation of Storage trait.
/// It is mainly used for test purpose.
#[derive(Clone, Default)]
pub struct MemStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage{
            ..Default::default()
        }
    }

    fn read_lock(&self) -> RwLockReadGuard<MemStorageCore> {
        self.core.read().unwrap()
    }

    fn write_lock(&self) -> RwLockWriteGuard<MemStorageCore> {
        self.core.write().unwrap()
    }

     /// set_hardstate saves the current HardState.
    pub fn set_hard_state(&mut self, hs: HardState) {
        self.write_lock().set_hard_state(hs);
    }

    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        self.write_lock().append(ents)
    }

    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        self.write_lock().apply_snapshot(snapshot)
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> Result<(HardState, ConfState)> {
        let core = self.read_lock();
        Ok((core.hard_state.clone(), core.snapshot.get_metadata().get_conf_state().clone()))
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>> {
        let core = self.read_lock();
        let offset = core.entries[0].get_index();
        if low <= offset {
            return Err(StorageError::ErrCompacted.into());
        }

        if high > core.inner_last_index() + 1 {
            panic!("entries' hight({}) is out of bound lastindex({})", high, core.inner_last_index() + 1);
        }

        if core.entries.len() == 1 {
            return Err(StorageError::ErrUnavailable.into());
        }

        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();

        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn first_index(&self) -> Result<u64> {
        let core = self.read_lock();
        Ok(core.entries[0].get_index() + 1)
    }

    fn last_index(&self) -> Result<u64> {
        let core = self.read_lock();
        Ok(core.inner_last_index())
    }

    fn term(&self, index: u64) -> Result<u64> {
        let core = self.read_lock();
        let offset = core.entries[0].get_index();

        if index <= offset {
            return Err(StorageError::ErrCompacted.into());
        }
        if index-offset >= core.entries.len() as u64 {
            return Err(StorageError::ErrUnavailable.into());
        }

        Ok(core.entries[(index-offset) as usize].get_index())
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let core = self.read_lock();
        Ok(core.snapshot.clone())
    }
}

pub fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: u64) {
    if max == NO_LIMIT || entries.len() <= 1 {
        return;
    }

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += u64::from(Message::compute_size(e));
                true
            } else {
                size += u64::from(Message::compute_size(e));
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}