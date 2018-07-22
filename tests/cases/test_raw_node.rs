use libraft::errors::Error;
use libraft::raft::{Peer, StateType, NONE};
use libraft::raftpb::{ConfChange, ConfChangeType, EntryType, HardState, Message, MessageType};
use libraft::raw_node::RawNode;
use libraft::storage::{MemStorage, Storage};
use libraft::util::{is_local_msg, NO_LIMIT};

use protobuf::{self, ProtobufEnum};

use cases::test_raft::new_test_config;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_raw_node_step() {
        for &msg_t in MessageType::values() {
            let s = MemStorage::new();
            let mut raw_node = RawNode::new(
                &mut new_test_config(1, vec![], 10, 1),
                s,
                vec![Peer {
                    context: Default::default(),
                    id: 1,
                }],
            ).unwrap();

            let mut m = Message::new();
            m.set_msg_type(msg_t);
            let err = raw_node.step(m);
            if is_local_msg(msg_t) {
                assert_eq!(Err(Error::StepLocalMsg), err);
            }
        }
    }

    #[test]
    fn test_raw_node_proposal_and_conf_change() {
        let s = MemStorage::new();
        let mut raw_node = RawNode::new(
            &mut new_test_config(1, vec![], 10, 1),
            s,
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        assert_eq!(raw_node.raft.state, StateType::Follower);
        assert_eq!(raw_node.raft.lead, NONE);
        assert_eq!(raw_node.raft.raft_log.unstable.entries.len(), 1);
        assert_eq!(raw_node.raft.raft_log.committed, 1);
        assert_eq!(raw_node.pre_hard_state, HardState::new());
        assert_eq!(raw_node.pre_soft_state.lead, NONE);
        assert_eq!(raw_node.pre_soft_state.raft_state, StateType::Follower);

        let rd = raw_node.ready();

        assert_eq!(rd.entries.len(), 1);
        assert_eq!(rd.committed_entries.len(), 1);
        assert_eq!(rd.soft_state, None);
        assert_eq!(rd.hard_state.term, 1);
        assert_eq!(rd.hard_state.vote, NONE);
        assert_eq!(rd.hard_state.commit, 1);

        let _ = raw_node.raft.raft_log.storage.append(&rd.entries);
        raw_node.advance(rd);
        let _ = raw_node.campaign();

        let mut proposed = false;
        let mut last_index = 0;
        let mut ccdata: Vec<u8> = vec![];

        loop {
            let rd = raw_node.ready();
            let _ = raw_node.raft.raft_log.storage.append(&rd.entries);
            if !proposed && rd.soft_state.as_ref().unwrap().lead == raw_node.raft.id {
                let _ = raw_node.propose(Vec::from("somedata"));
                let mut cc = ConfChange::new();
                cc.set_change_type(ConfChangeType::ConfChangeAddNode);
                cc.set_node_id(1);
                ccdata = protobuf::Message::write_to_bytes(&cc).expect("unexpected marshal error");

                let _ = raw_node.propose_conf_change(&cc);
                proposed = true;
            }

            raw_node.advance(rd);

            last_index = raw_node.raft.raft_log.storage.last_index().unwrap();
            if last_index >= 4 {
                break;
            }
        }

        let mut ents = raw_node
            .raft
            .raft_log
            .storage
            .entries(last_index - 1, last_index + 1, NO_LIMIT)
            .unwrap();

        assert_eq!(ents.len(), 2);
        assert_eq!(ents[0].take_data(), Vec::from("somedata"));
        assert_eq!(ents[1].get_entry_type(), EntryType::EntryConfChange);
        assert_eq!(ents[1].take_data(), ccdata);
    }
}
