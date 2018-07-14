use raftpb::{Entry, Snapshot};

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
        Unstable {
            snapshot: None,
            entries: vec![],
            offset,
            tag,
        }
    }

    /// maybe_term returns the term of the entry at index i, if there
    /// is any.
    pub(crate) fn maybe_term(&self, i: u64) -> Option<u64> {
        if i < self.offset {
            if let Some(sn) = self.snapshot.as_ref() {
                if sn.get_metadata().get_index() == i {
                    return Some(sn.get_metadata().get_term());
                }
            }
            return None;
        }

        if let Some(last_index) = self.maybe_last_index() {
            if i > last_index {
                return None;
            }
            return Some(self.entries[(i - self.offset) as usize].get_term());
        }
        None
    }

    /// maybe_first_index returns the index of the first possible entry in entries
    /// if it has a snapshot.
    pub(crate) fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map(|snap| snap.get_metadata().get_index() + 1)
    }

    // maybe_last_index returns the last index if it has at least one
    // unstable entry or snapshot.
    pub(crate) fn maybe_last_index(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self
                .snapshot
                .as_ref()
                .map(|snap| snap.get_metadata().get_index()),
            len => Some(self.offset + len as u64 - 1),
        }
    }

    pub(crate) fn truncate_and_append(&mut self, ents: &[Entry]) {
        if ents.is_empty() {
            return;
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
            self.entries.truncate((after - off) as usize);
            self.entries.extend_from_slice(ents);
        }
    }

    pub(crate) fn slice(&self, lo: u64, hi: u64) -> &[Entry] {
        self.must_check_out_of_bounds(lo, hi);
        let l = lo as usize;
        let h = hi as usize;
        let off = self.offset as usize;
        &self.entries[l - off..h - off]
    }

    pub(crate) fn must_check_out_of_bounds(&self, low: u64, hight: u64) {
        if low > hight {
            panic!("invlid unstable slice {} > {}", low, hight);
        }

        let upper = self.offset + self.entries.len() as u64;
        if low < self.offset || hight > upper {
            panic!(
                "unstable.slice[{},{}) out of bound [{},{}]",
                low, hight, self.offset, upper,
            );
        }
    }

    pub(crate) fn restore(&mut self, s: Snapshot) {
        self.offset = s.get_metadata().get_index() + 1;
        self.entries.clear();
        self.snapshot = Some(s);
    }

    pub(crate) fn stable_to(&mut self, i: u64, t: u64) {
        if let Some(gt) = self.maybe_term(i) {
            // if i < offset, term is matched with the snapshot
            // only update the unstable entries if term is matched with
            // an unstable entry.
            if gt == t && i >= self.offset {
                let start = i - self.offset + 1;
                self.entries.drain(..start as usize);
                self.offset = i + 1;
            }
        }
    }

    pub(crate) fn stable_snap_to(&mut self, i: u64) {
        if self.snapshot.is_none() {
            return;
        }

        if self.snapshot.as_ref().unwrap().get_metadata().get_index() == i {
            self.snapshot = None;
        }
    }
}

#[cfg(test)]
mod test {
    use log_unstable::Unstable;
    use raftpb::{Entry, Snapshot, SnapshotMetadata};

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        e
    }

    fn new_snapshot(index: u64, term: u64) -> Snapshot {
        let mut snap = Snapshot::new();
        let mut meta = SnapshotMetadata::new();
        meta.set_index(index);
        meta.set_term(term);
        snap.set_metadata(meta);
        snap
    }

    #[test]
    fn test_maybe_first_index() {
        // entry, offset, snap, wok, windex,
        let tests = vec![
            // no snapshot
            (Some(new_entry(5, 1)), 5, None, false, 0),
            (None, 0, None, false, 0),
            // has snapshot
            (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), true, 5),
            (None, 5, Some(new_snapshot(4, 1)), true, 5),
        ];

        for (entries, offset, snapshot, wok, windex) in tests {
            let u = Unstable {
                entries: entries.map_or(vec![], |entry| vec![entry]),
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            let index = u.maybe_first_index();
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_maybe_last_index() {
        // entry, offset, snap, wok, windex,
        let tests = vec![
            (Some(new_entry(5, 1)), 5, None, true, 5),
            (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), true, 5),
            (None, 5, Some(new_snapshot(4, 1)), true, 4),
            (None, 0, None, false, 0),
        ];

        for (entries, offset, snapshot, wok, windex) in tests {
            let u = Unstable {
                entries: entries.map_or(vec![], |entry| vec![entry]),
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            let index = u.maybe_last_index();
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_maybe_term() {
        let tests = vec![
            (Some(new_entry(5, 1)), 5, None, 5, true, 1),
            (Some(new_entry(5, 1)), 5, None, 6, false, 0),
            (Some(new_entry(5, 1)), 5, None, 4, false, 0),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                5,
                true,
                1,
            ),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                6,
                false,
                0,
            ),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                4,
                true,
                1,
            ),
            (
                Some(new_entry(5, 1)),
                5,
                Some(new_snapshot(4, 1)),
                3,
                false,
                0,
            ),
            (None, 5, Some(new_snapshot(4, 1)), 5, false, 0),
            (None, 5, Some(new_snapshot(4, 1)), 4, true, 1),
            (None, 0, None, 5, false, 0),
        ];

        for (entries, offset, snapshot, index, wok, windex) in tests {
            let u = Unstable {
                entries: entries.map_or(vec![], |entry| vec![entry]),
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            let index = u.maybe_term(index);
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_unstable_restore() {
        let mut u = Unstable {
            entries: vec![new_entry(5, 1)],
            offset: 5,
            snapshot: Some(new_snapshot(4, 1)),
            ..Default::default()
        };

        let s = new_snapshot(6, 2);
        u.restore(s.clone());
        assert_eq!(u.offset, s.get_metadata().get_index() + 1);
        assert_eq!(u.entries.len(), 0);
        assert_eq!(u.snapshot.unwrap(), s);
    }

    #[test]
    fn test_unstable_stable_to() {
        let tests = vec![
            (vec![], 0, None, 5, 1, 0, 0),
            (vec![new_entry(5, 1)], 5, None, 5, 1, 6, 0),
            (vec![new_entry(6, 2)], 6, None, 4, 1, 6, 1),
            (vec![new_entry(5, 1)], 5, None, 4, 1, 5, 1),
            (vec![new_entry(5, 1)], 5, None, 4, 2, 5, 1),
            (
                vec![new_entry(5, 1), new_entry(6, 1)],
                5,
                Some(new_snapshot(4, 1)),
                5,
                1,
                6,
                1,
            ),
            (
                vec![new_entry(6, 2)],
                6,
                Some(new_snapshot(5, 1)),
                6,
                1,
                6,
                1,
            ),
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                4,
                1,
                5,
                1,
            ),
            (
                vec![new_entry(5, 2)],
                5,
                Some(new_snapshot(4, 2)),
                4,
                1,
                5,
                1,
            ),
        ];

        for (entries, offset, snapshot, index, term, woffset, wlen) in tests {
            let mut u = Unstable {
                entries: entries,
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };

            u.stable_to(index, term);
            assert_eq!(woffset, u.offset);
            assert_eq!(wlen, u.entries.len());
        }
    }

    #[test]
    fn test_unstable_truncate_and_append() {
        let tests = vec![
            (
                vec![new_entry(5, 1)],
                5,
                None,
                vec![new_entry(6, 1), new_entry(7, 1)],
                5,
                vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
            ),
            (
                vec![new_entry(5, 1)],
                5,
                None,
                vec![new_entry(5, 2), new_entry(6, 2)],
                5,
                vec![new_entry(5, 2), new_entry(6, 2)],
            ),
            (
                vec![new_entry(5, 1)],
                5,
                None,
                vec![new_entry(4, 2), new_entry(5, 2), new_entry(6, 2)],
                4,
                vec![new_entry(4, 2), new_entry(5, 2), new_entry(6, 2)],
            ),
            (
                vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
                5,
                None,
                vec![new_entry(6, 2)],
                5,
                vec![new_entry(5, 1), new_entry(6, 2)],
            ),
            (
                vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
                5,
                None,
                vec![new_entry(7, 2), new_entry(8, 2)],
                5,
                vec![
                    new_entry(5, 1),
                    new_entry(6, 1),
                    new_entry(7, 2),
                    new_entry(8, 2),
                ],
            ),
        ];

        for (entries, offset, snapshot, toappend, woffset, wentries) in tests {
            let mut u = Unstable {
                entries: entries,
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };

            u.truncate_and_append(&toappend);
            assert_eq!(u.offset, woffset);
            assert_eq!(u.entries, wentries);
        }
    }
}
