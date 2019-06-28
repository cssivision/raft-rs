use libraft::raft::{self, Peer};
use libraft::raw_node::RawNode;
use libraft::storage::MemStorage;
use libraft::util::{is_empty_snap, NO_LIMIT};

use log::debug;

use crossbeam::channel::{Receiver, TryRecvError};
use std::time::{Duration, Instant};

use crate::config::Config;

pub struct RaftNode {
    raft: RawNode<MemStorage>,
    propc_rx: Receiver<Vec<u8>>,
}

impl RaftNode {
    pub fn new(cfg: Config, propc_rx: Receiver<Vec<u8>>) -> RaftNode {
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

        let r = RawNode::new(&mut c, MemStorage::new(), peers).unwrap();
        RaftNode {
            raft: r,
            propc_rx: propc_rx,
        }
    }

    pub fn start(&mut self) {
        let mut now = Instant::now();
        let timeout = Duration::from_millis(100);

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
            if d >= timeout {
                now = Instant::now();
                self.raft.tick();
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

    if !is_empty_snap(&ready.snapshot) {
        rn.raft
            .raft_log
            .storage
            .apply_snapshot(ready.snapshot.clone())
            .unwrap();
    }

    rn.raft.raft_log.storage.set_hard_state(ready.hard_state.clone());
    if !ready.entries.is_empty() {
        rn.raft.raft_log.storage.append(&ready.entries).unwrap();
    }

    rn.advance(ready);
}