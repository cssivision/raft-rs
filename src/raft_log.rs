use std::cmp;

use storage::Storage;
use log_unstable::Unstable;
use raftpb::{Entry};
use errors::{Error, Result, StorageError};
use util::limit_size;

#[derive(Default)]
pub struct RaftLog<T: Storage> {
    /// storage contains all stable entries since the last snapshot.
    pub storage: T,

    /// unstable contains all unstable entries and snapshot.
	/// they will be saved into storage.
    unstable: Unstable,

    /// committed is the highest log position that is known to be in
	/// stable storage on a quorum of nodes.
	pub committed: u64,

    /// applied is the highest log position that the application has
	/// been instructed to apply to its state machine.
	/// Invariant: applied <= committed
	pub applied: u64,

    /// tag only used for logger.
    tag: String,
}

impl<T: Storage> ToString for RaftLog<T> {
    fn to_string(&self) -> String {
        format!(
            "committed={}, applied={}, unstable.offset={}, unstable.entries.len()={}",
            self.committed,
            self.applied,
            self.unstable.offset,
            self.unstable.entries.len()
        )
    }
}

impl<T: Storage> RaftLog<T> {
    pub fn new(storage: T, tag: String) -> RaftLog<T> {
        let first_index = storage.first_index().unwrap();
	    let last_index = storage.last_index().unwrap();
        RaftLog{
            storage: storage,
            committed: first_index - 1,
            applied: first_index - 1,
            unstable: Unstable::new(last_index+1, tag.clone()),
            tag: tag,
        }
    }

    pub fn last_index(&self) -> u64 {
        if let Some(last_index) = self.unstable.maybe_last_index() {
            return last_index;
        }

        match self.storage.last_index() {
            Ok(last_index) => last_index,
            Err(err) => panic!(err)
        }
    }

    pub fn first_index(&self) -> u64 {
        if let Some(fi) = self.unstable.maybe_first_index() {
            return fi;
        }
        self.storage.first_index().unwrap()
    }

    pub fn applied_to(&mut self, i: u64) {
        if i == 0 {
            return 
        }

        if i > self.committed || i < self.applied {
            panic!(
                "applied({}) is out of range [prev applied({}), committed({})]", 
                i,
                self.applied, 
                self.committed,
            );
        }

        self.applied = i;
    }

    pub fn last_term(&self) -> u64 {
        match self.term(self.last_index()) {
            Ok(t) => t,
            Err(e) => panic!("unexpected error when getting the last term ({})", e)
        }
    }

    fn term(&self, i: u64) -> Result<u64> {
        let dummy_index = self.first_index() - 1;
        if i < dummy_index || i > self.last_index() {
            return Ok(0);
        }
        if let Some(t) = self.unstable.maybe_term(i) {
            return Ok(t);
        }

        match self.storage.term(i) {
            Ok(t) => return Ok(t),
            Err(e) => {
                match e {
                    Error::Storage(StorageError::Compacted) | Error::Storage(StorageError::Unavailable) => {},
                    _ => panic!("unexpected error: {:?}", e)
                }
                Err(e)
            }
        }
    }

    pub fn get_applied(&self) -> u64 {
        self.applied
    }

    pub fn get_storage(&self) -> &T {
        &self.storage
    }

    pub fn append(&mut self, ents: &[Entry]) -> u64 {
        if ents.is_empty() {
            return self.last_index();
        }
        let after = ents[0].get_index() - 1;
        if after < self.committed {
            panic!("after({}) is out of range [committed({})]", after, self.committed);
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    } 

    pub fn maybe_commit(&mut self, max_index: u64, term: u64) -> bool { 
        if max_index > self.committed && self.zero_term_on_err_compacted(self.term(max_index)) == term {
            self.commit_to(max_index);
            return true;
        }
        false
    }

    fn commit_to(&mut self, tocommit: u64) {
        if self.committed < tocommit {
            if self.last_index() < tocommit {
                panic!(
                    "tocommit({}) is out of range [last_index({})]. Was the raft log corrupted, truncated, or lost?", 
                    tocommit, 
                    self.last_index(),
                )
            }
            self.committed = tocommit
        }
    }

    fn zero_term_on_err_compacted(&self, t: Result<u64>) -> u64 {
        match t {
            Ok(t) => t,
            Err(e) => {
                match e {
                    Error::Storage(StorageError::Compacted) => {
                        0
                    },
                    e => panic!("unexpected error ({})", e)
                }
            }
        }
    }

    pub fn must_check_out_of_bounds(&self, low: u64, hight: u64) -> Result<()> {
        if low > hight {
            panic!("invlid unstable slice {} > {}", low, hight);
        }

        let fi = self.first_index();
        if low < fi {
            return Err(Error::Storage(StorageError::Compacted));
        }

        let hi = self.last_index() + 1;
        if low < fi || hight > hi {
            panic!("slice[{},{}) out of bound [{},{}]", low, hight, fi, hi);
        }
        
        Ok(())
    }

    pub fn slice(&self, lo: u64, hi: u64, max_size: u64) -> Result<Vec<Entry>> {
        if let Err(e) = self.must_check_out_of_bounds(lo, hi) {
            return Err(e);
        }

        if lo == hi {
            return Ok(vec![]);
        }

        let mut ents = Vec::new();
        if lo < self.unstable.offset {
            let sorted_ents = match self.storage.entries(lo, cmp::min(hi, self.unstable.offset), max_size) {
                Ok(ents) => ents,
                Err(e) => {
                    match e {
                        Error::Storage(StorageError::Compacted) => {
                            return Err(e);
                        },
                        Error::Storage(StorageError::Unavailable) => {
                            panic!("entries[{}:{}) is unavailable from storage", lo, hi);
                        },
                        _ => panic!(e)
                    }
                }
            };

            // check if has reached the size limitation
            if (sorted_ents.len() as u64) < cmp::min(hi, self.unstable.offset) - lo {
                return Ok(sorted_ents);
            }
            ents.extend_from_slice(&sorted_ents);
        }
        
        if hi > self.unstable.offset {
            let unstable = self.unstable.slice(cmp::max(self.unstable.offset, lo), hi);
            ents.extend_from_slice(unstable);
        }

        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    pub fn entries(&self, i: u64, max_size: u64) -> Result<Vec<Entry>> {
        if i > self.last_index() {
            Ok(vec![])
        } else {
            self.slice(i, self.last_index()+1, max_size)
        }
    }
}