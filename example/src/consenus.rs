
use crossbeam::channel::unbounded;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use libraft::raft::{self, Peer};
use libraft::raw_node::RawNode;
use libraft::raftpb;
use libraft::storage::MemStorage;
use libraft::util::{is_empty_snap, NO_LIMIT};

use log::debug;

use crate::config::Config;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::thread;


// Use a HashMap to hold the `propose` callbacks.

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
                Ok(data) => {
                    self.raft.propose(data).unwrap();
                }
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

    rn.raft
        .raft_log
        .storage
        .set_hard_state(ready.hard_state.clone());
    if !ready.entries.is_empty() {
        rn.raft.raft_log.storage.append(&ready.entries).unwrap();
    }

    for entry in ready.committed_entries.drain(..) {
        if entry.data.is_empty() {
            continue;
        }

        if entry.get_entry_type() == raftpb::EntryType::EntryNormal {
            // handle normal message
        } else if entry.get_entry_type() == raftpb::EntryType::EntryConfChange {
            // handle config change message
        }
    }

    rn.advance(ready);
}


fn send_propose(sender: Sender<Vec<u8>>) {
    let mut cbs = HashMap::new();
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        println!("propose a request");
        let data = Vec::from("data");
        let (s1, r1) = unbounded::<u8>();
        cbs.insert(
            data,
            Box::new(move || {
                s1.send(1).unwrap();
            }),
        );
        sender.send(Vec::from("data1")).unwrap();
        let n = r1.recv().unwrap();
        assert_eq!(n, 0);
    });
}
