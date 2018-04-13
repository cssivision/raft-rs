use std::collections::{HashMap, HashSet, VecDeque};

use raftpb::{Message};

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through request_ctx, eg. given a unique id as
// request_ctx
#[derive(Debug, Clone)]
pub struct ReadState {
    pub index: u64,
    pub request_ctx: Vec<u8>,
}

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
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: VecDeque<Vec<u8>>,
}

impl ReadOnly {
    pub fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly{
            option,
            pending_read_index: HashMap::new(),
            read_index_queue: VecDeque::new(),
        }
    }

    pub fn last_pending_request_ctx(&mut self) -> Option<Vec<u8>> {
        self.read_index_queue.back().cloned()
    }

    // add_request adds a read only reuqest into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    pub fn add_request(&mut self, index: u64, msg: Message) {
        let ctx = msg.get_entries()[0].get_data().to_vec();
        if self.pending_read_index.contains_key(&ctx) {
            return;
        }
        let ris = ReadIndexStatus{
            index: index,
            req: msg,
            acks: HashSet::new(),
        };
        self.pending_read_index.insert(ctx.clone(), ris);
        self.read_index_queue.push_back(ctx);
    }
}