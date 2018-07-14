use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::u64;

use errors::{Error, Result, StorageError};
use raftpb::{ConfState, Entry, HardState, Snapshot};
use util::limit_size;

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
            return Err(Error::Storage(StorageError::SnapshotOutOfDate));
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
            return Err(Error::Storage(StorageError::Compacted));
        }

        if compact_index > self.inner_last_index() {
            panic!(
                "compact {} is out of bound lastindex({})",
                compact_index,
                self.inner_last_index()
            );
        }

        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
        Ok(())
    }

    // Append the new entries to storage.
    // ensure the entries are continuous and
    // entries[0].index > ms.entries[0].index
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
            let t = (first - ents[0].get_index()) as usize;
            &ents[t..]
        } else {
            ents
        };

        let offset = te[0].get_index() - self.entries[0].get_index();

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
            return Err(Error::Storage(StorageError::SnapshotOutOfDate));
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
        self.snapshot
            .mut_metadata()
            .set_term(self.entries[(index - offset) as usize].get_term());

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
        MemStorage {
            ..Default::default()
        }
    }

    fn read_lock(&self) -> RwLockReadGuard<MemStorageCore> {
        self.core.read().unwrap()
    }

    pub fn write_lock(&self) -> RwLockWriteGuard<MemStorageCore> {
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

    fn get_entries(&self) -> Vec<Entry> {
        self.read_lock().entries.to_vec()
    }

    pub fn compact(&mut self, index: u64) -> Result<()> {
        self.write_lock().compact(index)
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> Result<(HardState, ConfState)> {
        let core = self.read_lock();
        Ok((
            core.hard_state.clone(),
            core.snapshot.get_metadata().get_conf_state().clone(),
        ))
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>> {
        let core = self.read_lock();
        let offset = core.entries[0].get_index();
        if low <= offset {
            return Err(Error::Storage(StorageError::Compacted));
        }

        if high > core.inner_last_index() + 1 {
            panic!(
                "entries' hight({}) is out of bound lastindex({})",
                high,
                core.inner_last_index() + 1
            );
        }

        if core.entries.len() == 1 {
            return Err(Error::Storage(StorageError::Unavailable));
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

        if index < offset {
            return Err(Error::Storage(StorageError::Compacted));
        }
        if index - offset >= core.entries.len() as u64 {
            return Err(Error::Storage(StorageError::Unavailable));
        }

        Ok(core.entries[(index - offset) as usize].get_term())
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let core = self.read_lock();
        Ok(core.snapshot.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use protobuf::Message;
    use raftpb::SnapshotMetadata;
    use util::NO_LIMIT;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        e
    }

    fn new_memory_storage(ents: Vec<Entry>) -> MemStorage {
        let mut s = MemStorage::new();
        let mut core = MemStorageCore::default();
        core.entries = ents;
        s.core = Arc::new(RwLock::new(core));
        s
    }

    fn new_snapshot(index: u64, term: u64, cs: ConfState, data: Vec<u8>) -> Snapshot {
        let mut snapshot = Snapshot::new();
        snapshot.set_data(data);
        let mut meta = SnapshotMetadata::new();
        meta.set_conf_state(cs.clone());
        meta.set_index(index);
        meta.set_term(term);
        snapshot.set_metadata(meta);
        snapshot
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let tests = vec![
            (2, Some(Error::Storage(StorageError::Compacted)), 0),
            (3, None, 3),
            (4, None, 4),
            (5, None, 5),
            (6, Some(Error::Storage(StorageError::Unavailable)), 0),
        ];

        let s = new_memory_storage(ents);
        for (i, werr, wterm) in tests {
            match s.term(i) {
                Err(e) => {
                    assert_eq!(e, werr.unwrap());
                }
                Ok(t) => {
                    assert_eq!(t, wterm);
                }
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let tests = vec![
            (
                2,
                6,
                NO_LIMIT,
                Some(Error::Storage(StorageError::Compacted)),
                None,
            ),
            (
                3,
                4,
                NO_LIMIT,
                Some(Error::Storage(StorageError::Compacted)),
                None,
            ),
            (4, 5, NO_LIMIT, None, Some(vec![new_entry(4, 4)])),
            (
                4,
                6,
                NO_LIMIT,
                None,
                Some(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                NO_LIMIT,
                None,
                Some(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            (4, 7, 0, None, Some(vec![new_entry(4, 4)])),
            (
                4,
                7,
                u64::from(Message::compute_size(&ents[1]) + Message::compute_size(&ents[2])),
                None,
                Some(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(
                    Message::compute_size(&ents[1])
                        + Message::compute_size(&ents[2])
                        + Message::compute_size(&ents[3]) / 2,
                ),
                None,
                Some(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(
                    Message::compute_size(&ents[1])
                        + Message::compute_size(&ents[2])
                        + Message::compute_size(&ents[3]) - 1,
                ),
                None,
                Some(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(
                    Message::compute_size(&ents[1])
                        + Message::compute_size(&ents[2])
                        + Message::compute_size(&ents[3]),
                ),
                None,
                Some(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];

        let s = new_memory_storage(ents);
        for (lo, hi, max_size, werr, wents) in tests {
            match s.entries(lo, hi, max_size) {
                Err(e) => {
                    assert_eq!(e, werr.unwrap());
                }
                Ok(ents) => {
                    assert_eq!(ents, wents.unwrap());
                }
            }
        }
    }

    #[test]
    fn test_storage_append() {
        let tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
            ),
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                vec![new_entry(3, 3), new_entry(4, 5)],
            ),
            (
                vec![new_entry(6, 5)],
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
            ),
        ];

        for (ents, wents) in tests {
            let mut s = new_memory_storage(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]);
            match s.append(&ents) {
                Err(e) => panic!(e),
                Ok(_) => assert_eq!(s.get_entries(), wents),
            }
        }
    }

    #[test]
    fn test_storage_last_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut s = new_memory_storage(ents);

        match s.last_index() {
            Err(e) => panic!(e),
            Ok(last) => assert_eq!(last, 5),
        }

        s.append(&vec![new_entry(6, 5)]).unwrap();
        match s.last_index() {
            Err(e) => panic!(e),
            Ok(last) => assert_eq!(last, 6),
        }
    }

    #[test]
    fn test_storage_first_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut s = new_memory_storage(ents);

        match s.first_index() {
            Err(e) => panic!(e),
            Ok(first) => assert_eq!(first, 4),
        }

        s.compact(4).unwrap();
        match s.first_index() {
            Err(e) => panic!(e),
            Ok(last) => assert_eq!(last, 5),
        }
    }

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let tests = vec![
            (2, Err(Error::Storage(StorageError::Compacted)), 3, 3, 3),
            (3, Err(Error::Storage(StorageError::Compacted)), 3, 3, 3),
            (4, Ok(()), 4, 4, 2),
            (5, Ok(()), 5, 5, 1),
        ];

        for (i, werr, windex, wterm, wlen) in tests {
            let mut s = new_memory_storage(ents.to_vec());

            assert_eq!(s.compact(i), werr);
            assert_eq!(s.get_entries()[0].get_index(), windex);
            assert_eq!(s.get_entries()[0].get_term(), wterm);
            assert_eq!(s.get_entries().len(), wlen);
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);
        let data: Vec<u8> = Vec::from("data");

        let s = new_memory_storage(ents.clone());
        match s
            .write_lock()
            .create_snapshot(4, Some(cs.clone()), data.clone())
        {
            Err(e) => panic!(e),
            Ok(snapshot) => {
                let wsnapshot = new_snapshot(4, 4, cs.clone(), data.clone());
                assert_eq!(snapshot, &wsnapshot);
            }
        }

        let s = new_memory_storage(ents);
        s.write_lock()
            .create_snapshot(5, Some(cs.clone()), data.clone())
            .unwrap();
        let wsnapshot = new_snapshot(5, 5, cs.clone(), data.clone());
        assert_eq!(s.snapshot(), Ok(wsnapshot));
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);
        let data: Vec<u8> = Vec::from("data");

        let mut s = new_memory_storage(vec![]);
        assert_eq!(
            Ok(()),
            s.apply_snapshot(new_snapshot(4, 4, cs.clone(), data.clone()))
        );

        assert_eq!(
            Err(Error::Storage(StorageError::SnapshotOutOfDate)),
            s.apply_snapshot(new_snapshot(3, 3, cs.clone(), data.clone()))
        );
    }
}
