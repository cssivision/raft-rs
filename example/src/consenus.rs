use libraft::raft::{self, Peer, Raft};
use libraft::raw_node::RawNode;
use libraft::raftpb::Snapshot;
use libraft::storage::MemStorage;
use libraft::util::{NO_LIMIT, is_empty_snap};

use log::debug;

use crossbeam::channel::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use crate::config::Config;

struct RaftNode {
    raft: RawNode<MemStorage>,
    propc_rx: Receiver<Vec<u8>>,
}

impl RaftNode {
    fn new(cfg: Config, propc_rx: Receiver<Vec<u8>>) -> RaftNode {
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

        let mut r = RawNode::new(&mut c, MemStorage::new(), peers).unwrap();
        RaftNode {
            raft: r,
            propc_rx: propc_rx,
        }
    }

    fn start(&mut self) {
        let mut now = Instant::now();
        let mut timeout = Duration::from_millis(100);

        loop {
            match self.propc_rx.try_recv() {
                Ok(_) => {}
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    debug!("recv disconnected");
                    return;
                }
            }

            let d = now.elapsed();
            now = Instant::now();
            if d >= timeout {
                timeout = Duration::from_millis(100);
                self.raft.tick();
            } else {
                timeout -= d;
            }

            on_ready(&mut self.raft);
        }
    }

}

fn on_ready(rn: &mut RawNode<MemStorage>) {
    if !rn.has_ready() {
        return;
    }
    let mut ready = rn.ready();

    let is_leader = rn.raft.lead == rn.raft.id;

    if is_leader {
        let msgs = ready.messages.drain(..);

        for _ in msgs {
            // todo
        }
    }

    if !is_empty_snap(ready.snapshot) {

    }
}