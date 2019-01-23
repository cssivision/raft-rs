use libraft::raft::{self, Peer, Raft};
use libraft::raw_node::RawNode;
use libraft::storage::{MemStorage, Storage};
use libraft::util::NO_LIMIT;
use std::sync::mpsc::Receiver;

use crate::config::Config;

struct RaftNode<T: Storage> {
    raft: RawNode<T>,
}

fn new_raft_node<T: Storage>(cfg: Config, proposal_c: Receiver<Vec<u8>>) -> RaftNode<T> {
    let mut c = raft::Config {
        id: cfg.id,
        election_tick: cfg.election_tick,
        heartbeat_tick: cfg.heartbeat_tick,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    };

    let mut peers = vec![];

    for id in cfg.peers {
        peers.push(Peer {
            id: id,
            context: vec![],
        });
    }

    let mut r = RawNode::new(&mut c, MemStorage::new(), peers);
    unimplemented!()
}
