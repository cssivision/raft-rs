use errors::Result;
use raft::{Raft, Config, NONE, StateType, Peer};
use storage::{Storage, MemStorage};
use raftpb::{HardState, Entry, ConfChange, ConfChangeType, EntryType};

use protobuf;

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq, Debug)]
pub struct SoftState {
    pub lead: u64,
    pub raft_state: StateType,
}

pub struct RawNode<T: Storage> {
    pub raft: Raft<T>,
    pub pre_soft_state: SoftState,
    pub pre_hard_state: HardState,
}

impl<T: Storage> RawNode<T> {
    pub fn new(c: &mut Config, storage: T, mut peers: Vec<Peer>) -> Result<RawNode<T>> {
        if c.id == 0 {
            panic!("config id must not be zero");
        }
        let r = Raft::new(c, storage);
        let mut rn = RawNode{
            raft: r,
            pre_soft_state: Default::default(),
            pre_hard_state: Default::default(),
        };

        let last_index = rn.raft.raft_log.get_storage().last_index().unwrap();
        
        // If the log is empty, this is a new RawNode; otherwise it's
	    // restoring an existing RawNode.
        if last_index == 0 {
            rn.raft.become_follower(1, NONE);
            let mut ents: Vec<Entry> = Vec::with_capacity(peers.len());
            for (i, peer) in peers.iter_mut().enumerate() {
                let mut cc = ConfChange::new();
                cc.set_change_type(ConfChangeType::ConfChangeAddNode);
                cc.set_node_id(peer.id);
                cc.set_context(peer.context.to_vec());
                let data = protobuf::Message::write_to_bytes(&cc).expect("unexpected marshal error");
                let mut ent = Entry::new();
                ent.set_entry_type(EntryType::EntryConfChange);
                ent.set_term(1);
                ent.set_data(data);
                ent.set_index(i as u64+1);
                ents.push(ent);
            }
            rn.raft.raft_log.append(&ents);
            rn.raft.raft_log.committed = ents.len() as u64;

            for peer in peers {
                rn.raft.add_node(peer.id);
            }
        }
        rn.pre_soft_state = rn.raft.soft_state();
        if last_index == 0 {
            rn.pre_hard_state = HardState::new();
        } else {
            rn.pre_hard_state = rn.raft.hard_state();
        }
        Ok(rn)
    }
}