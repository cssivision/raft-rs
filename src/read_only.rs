use std::collections::{HashMap, HashSet, VecDeque};

use raftpb::{Entry, Message};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    Safe, 
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

impl Default for ReadOnlyOption {
    fn default() -> ReadOnlyOption {
        ReadOnlyOption::Safe
    }
}

#[derive(Default, Debug, Clone)]
pub struct ReadIndexStatus {
    req: Message,
    index: u64,
    acks: HashSet<u64>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub pending_read_index: HashMap<String, ReadIndexStatus>,
    pub read_index_queue: VecDeque<String>,
}

impl ReadOnly {
    pub fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly{
            option: option,
            pending_read_index: HashMap::new(),
            read_index_queue: VecDeque::new(),
        }
    }
}