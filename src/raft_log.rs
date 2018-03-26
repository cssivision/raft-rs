use storage::Storage;
use log_unstable::Unstable;

#[derive(Default)]
pub struct RaftLog<T: Storage> {
    /// storage contains all stable entries since the last snapshot.
    storage: T,

    /// unstable contains all unstable entries and snapshot.
	/// they will be saved into storage.
    unstable: Unstable,

    /// committed is the highest log position that is known to be in
	/// stable storage on a quorum of nodes.
	committed: u64,

    /// applied is the highest log position that the application has
	/// been instructed to apply to its state machine.
	/// Invariant: applied <= committed
	applied: u64,

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
}