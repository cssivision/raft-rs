use std::collections::{HashMap, HashSet, VecDeque};

use raftpb::Message;

// ReadState provides state for read only query.
// It's caller's responsibility to send MsgReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through request_ctx, eg. given a unique id as
// request_ctx
#[derive(Debug, Clone, PartialEq)]
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
    pub req: Message,
    pub index: u64,
    acks: HashSet<u64>,
}

#[derive(Default, Debug, Clone)]
pub struct ReadOnly {
    pub option: ReadOnlyOption,
    pub pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub read_index_queue: VecDeque<Vec<u8>>,
}

impl ReadOnly {
    pub(crate) fn new(option: ReadOnlyOption) -> ReadOnly {
        ReadOnly {
            option,
            pending_read_index: HashMap::new(),
            read_index_queue: VecDeque::new(),
        }
    }

    pub(crate) fn last_pending_request_ctx(&mut self) -> Option<Vec<u8>> {
        self.read_index_queue.back().cloned()
    }

    // add_request adds a read only reuqest into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    pub(crate) fn add_request(&mut self, index: u64, msg: Message) {
        let ctx = msg.get_entries()[0].get_data().to_vec();
        if self.pending_read_index.contains_key(&ctx) {
            return;
        }
        let ris = ReadIndexStatus {
            index,
            req: msg,
            acks: HashSet::new(),
        };
        self.pending_read_index.insert(ctx.clone(), ris);
        self.read_index_queue.push_back(ctx);
    }

    // recv_ack notifies the readonly struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the read only request
    // context.
    pub(crate) fn recv_ack(&mut self, msg: &Message) -> usize {
        if let Some(rs) = self.pending_read_index.get_mut(msg.get_context()) {
            rs.acks.insert(msg.get_from());
            rs.acks.len() + 1
        } else {
            0
        }
    }

    // advance advances the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given `msg`.
    pub(crate) fn advance(&mut self, msg: &Message) -> Vec<ReadIndexStatus> {
        let mut rss = vec![];
        if let Some(i) = self.read_index_queue.iter().position(|x| {
            if !self.pending_read_index.contains_key(x) {
                panic!("cannot find correspond read state from pending map");
            }
            *x == msg.get_context()
        }) {
            for _ in 0..i + 1 {
                let rs = self.read_index_queue.pop_front().unwrap();
                let status = self.pending_read_index.remove(&rs).unwrap();
                rss.push(status);
            }
        }
        rss
    }
}
