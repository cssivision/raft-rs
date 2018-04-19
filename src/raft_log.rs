use std::cmp;

use errors::{Error, Result, StorageError};
use log_unstable::Unstable;
use raftpb::{Entry, Snapshot};
use storage::Storage;
use util::{limit_size, NO_LIMIT};

#[derive(Debug, Default)]
pub struct RaftLog<T: Storage> {
    /// storage contains all stable entries since the last snapshot.
    pub storage: T,

    /// unstable contains all unstable entries and snapshot.
    /// they will be saved into storage.
    pub unstable: Unstable,

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
        RaftLog {
            storage,
            committed: first_index - 1,
            applied: first_index - 1,
            unstable: Unstable::new(last_index + 1, tag.clone()),
            tag,
        }
    }

    pub fn last_index(&self) -> u64 {
        if let Some(last_index) = self.unstable.maybe_last_index() {
            return last_index;
        }

        match self.storage.last_index() {
            Ok(last_index) => last_index,
            Err(err) => panic!(err),
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
            return;
        }

        if i > self.committed || i < self.applied {
            panic!(
                "applied({}) is out of range [prev applied({}), committed({})]",
                i, self.applied, self.committed,
            );
        }

        self.applied = i;
    }

    pub fn last_term(&self) -> u64 {
        match self.term(self.last_index()) {
            Ok(t) => t,
            Err(e) => panic!("unexpected error when getting the last term ({})", e),
        }
    }

    pub fn term(&self, i: u64) -> Result<u64> {
        let dummy_index = self.first_index() - 1;
        if i < dummy_index || i > self.last_index() {
            return Ok(0);
        }
        if let Some(t) = self.unstable.maybe_term(i) {
            return Ok(t);
        }

        match self.storage.term(i) {
            Ok(t) => Ok(t),
            Err(e) => {
                match e {
                    Error::Storage(StorageError::Compacted)
                    | Error::Storage(StorageError::Unavailable) => {}
                    _ => panic!("unexpected error: {:?}", e),
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
            panic!(
                "after({}) is out of range [committed({})]",
                after, self.committed
            );
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    }

    pub fn maybe_commit(&mut self, max_index: u64, term: u64) -> bool {
        if max_index > self.committed
            && self.zero_term_on_err_compacted(self.term(max_index)) == term
        {
            self.commit_to(max_index);
            return true;
        }
        false
    }

    pub fn commit_to(&mut self, tocommit: u64) {
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

    pub fn zero_term_on_err_compacted(&self, t: Result<u64>) -> u64 {
        match t {
            Ok(t) => t,
            Err(e) => match e {
                Error::Storage(StorageError::Compacted) => 0,
                e => panic!("unexpected error ({})", e),
            },
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
            let sorted_ents =
                match self.storage
                    .entries(lo, cmp::min(hi, self.unstable.offset), max_size)
                {
                    Ok(ents) => ents,
                    Err(e) => match e {
                        Error::Storage(StorageError::Compacted) => {
                            return Err(e);
                        }
                        Error::Storage(StorageError::Unavailable) => {
                            panic!("entries[{}:{}) is unavailable from storage", lo, hi);
                        }
                        _ => panic!(e),
                    },
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
            self.slice(i, self.last_index() + 1, max_size)
        }
    }

    /// is_up_to_date determines if the given (last_index,term) log is more up-to-date
    /// by comparing the index and term of the last entries in the existing logs.
    /// If the logs have last entries with different terms, then the log with the
    /// later term is more up-to-date. If the logs end with the same term, then
    /// whichever log has the larger last_index is more up-to-date. If the logs are
    /// the same, the given log is up-to-date.
    pub fn is_up_to_date(&self, index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && index >= self.last_index())
    }

    pub fn match_term(&self, i: u64, term: u64) -> bool {
        if let Ok(t) = self.term(i) {
            t == term
        } else {
            false
        }
    }

    /// find_conflict finds the index of the conflict.
    /// It returns the first pair of conflicting entries between the existing
    /// entries and the given entries, if there are any.
    /// If there is no conflicting entries, and the existing entries contains
    /// all the given entries, zero will be returned.
    /// If there is no conflicting entries, but the given entries contains new
    /// entries, the index of the first new entry will be returned.
    /// An entry is considered to be conflicting if it has the same index but
    /// a different term.
    /// The first entry MUST have an index equal to the argument 'from'.
    /// The index of the given entries MUST be continuously increasing.
    fn find_conflict(&self, ents: &[Entry]) -> u64 {
        for e in ents {
            if !self.match_term(e.get_index(), e.get_term()) {
                if e.get_index() <= self.last_index() {
                    info!(
                        "{} found conflict at index {} [existing term: {}, conflicting term: {}]",
                        self.tag,
                        e.get_index(),
                        self.zero_term_on_err_compacted(self.term(e.get_index())),
                        e.get_term(),
                    );
                    return e.get_index();
                }
            }
        }
        0
    }

    // maybe_append returns None if the entries cannot be appended. Otherwise,
    // it returns Some(last index of new entries).
    pub fn maybe_append(
        &mut self,
        index: u64,
        log_term: u64,
        committed: u64,
        ents: &[Entry],
    ) -> Option<u64> {
        if self.match_term(index, log_term) {
            let last_new_index = index + ents.len() as u64;
            let ci = self.find_conflict(ents);
            if ci == 0 {
                // no conflict, existing entries contain "ents".
            } else if ci <= self.committed {
                panic!(
                    "entry {} conflict with committed entry [committed({})]",
                    ci, self.committed
                );
            } else {
                self.append(&ents[(ci - index - 1) as usize..]);
            }
            self.commit_to(cmp::min(committed, last_new_index));
            return Some(last_new_index);
        }
        None
    }

    pub fn restore(&mut self, s: Snapshot) {
        info!(
            "{} log [{}] starts to restore snapshot [index: {}, term: {}]",
            self.tag,
            self.to_string(),
            s.get_metadata().get_index(),
            s.get_metadata().get_term()
        );

        self.unstable.restore(s);
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        if let Some(s) = self.unstable.snapshot.as_ref() {
            return Ok(s.clone());
        }

        self.storage.snapshot()
    }

    pub fn unstable_entries(&self) -> Vec<Entry> {
        self.unstable.entries.to_vec()
    }

    pub fn next_ents(&self) -> Vec<Entry> {
        let off = cmp::max(self.applied + 1, self.first_index());
        if self.committed + 1 > off {
            match self.slice(off, self.committed + 1, NO_LIMIT) {
                Ok(ents) => return ents,
                Err(e) => panic!("unexpected error when getting unapplied entries ({})", e),
            }
        }
        vec![]
    }

    pub fn has_next_ents(&self) -> bool {
        let off = cmp::max(self.applied + 1, self.first_index());
        self.committed + 1 > off
    }

    pub fn stable_to(&mut self, index: u64, term: u64) {
        self.unstable.stable_to(index, term);
    }

    pub fn stable_snap_to(&mut self, index: u64) {
        self.unstable.stable_snap_to(index);
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_find_conflict() {}
}
