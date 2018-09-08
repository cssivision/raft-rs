use libraft::errors::Error;
use libraft::raft::{Peer, StateType, Status, NONE};
use libraft::raftpb::{
    ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot, SnapshotMetadata,
};
use libraft::raw_node::RawNode;
use libraft::read_only::ReadState;
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
        let mut s = MemStorage::new();
        let mut raw_node = RawNode::new(
            &mut new_test_config(1, vec![], 10, 1),
            s.clone(),
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

        let _ = s.append(&rd.entries);
        raw_node.advance(rd);
        let _ = raw_node.campaign();

        let mut proposed = false;
        let mut last_index = 0;
        let mut ccdata: Vec<u8> = vec![];

        loop {
            let rd = raw_node.ready();
            let _ = s.append(&rd.entries);
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
            last_index = s.last_index().unwrap();
            if last_index >= 4 {
                break;
            }
        }

        let mut ents = s.entries(last_index - 1, last_index + 1, NO_LIMIT).unwrap();

        assert_eq!(ents.len(), 2);
        assert_eq!(ents[0].take_data(), Vec::from("somedata"));
        assert_eq!(ents[1].get_entry_type(), EntryType::EntryConfChange);
        assert_eq!(ents[1].take_data(), ccdata);
    }

    // ensures that two proposes to add the same node should
    // not affect the later propose to add new node.
    #[test]
    fn test_raw_node_proposal_add_duplicate_node() {
        let mut s = MemStorage::new();
        let mut raw_node = RawNode::new(
            &mut new_test_config(1, vec![], 10, 1),
            s.clone(),
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        let rd = raw_node.ready();
        let _ = s.append(&rd.entries);
        raw_node.advance(rd);
        let _ = raw_node.campaign();

        loop {
            let rd = raw_node.ready();
            let _ = s.append(&rd.entries);
            let lead = rd.soft_state.as_ref().unwrap().lead;
            if lead == raw_node.raft.id {
                raw_node.advance(rd);
                break;
            }
            raw_node.advance(rd);
        }

        let mut propose_conf_change_and_apply = |cc: &ConfChange| {
            let _ = raw_node.propose_conf_change(cc);
            let rd = raw_node.ready();
            let _ = s.write_lock().append(&rd.entries);
            for e in &rd.committed_entries {
                if e.get_entry_type() == EntryType::EntryConfChange {
                    let conf_change = protobuf::parse_from_bytes(e.get_data()).unwrap();
                    raw_node.apply_conf_change(&conf_change);
                }
            }

            raw_node.advance(rd);
        };

        let mut cc1 = ConfChange::new();
        cc1.set_change_type(ConfChangeType::ConfChangeAddNode);
        cc1.set_node_id(1);
        let ccdata1 = protobuf::Message::write_to_bytes(&cc1).expect("unexpected marshal error");
        propose_conf_change_and_apply(&cc1);
        propose_conf_change_and_apply(&cc1);

        let mut cc2 = ConfChange::new();
        cc2.set_change_type(ConfChangeType::ConfChangeAddNode);
        cc2.set_node_id(2);
        let ccdata2 = protobuf::Message::write_to_bytes(&cc2).expect("unexpected marshal error");

        propose_conf_change_and_apply(&cc2);

        let last_index = s.last_index().unwrap();
        let mut entries = s.entries(last_index - 2, last_index + 1, NO_LIMIT).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].take_data(), ccdata1);
        assert_eq!(entries[2].take_data(), ccdata2);
    }

    #[test]
    fn test_raw_node_read_index() {
        let wrs = vec![ReadState {
            index: 1,
            request_ctx: Vec::from("somedata"),
        }];

        let mut s = MemStorage::new();
        let mut c = new_test_config(1, vec![], 10, 1);
        let mut raw_node = RawNode::new(
            &mut c,
            s.clone(),
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        raw_node.raft.read_states = wrs.clone();
        let has_ready = raw_node.has_ready();
        assert!(has_ready);

        let rd = raw_node.ready();
        assert_eq!(rd.read_states, wrs);

        let _ = s.append(&rd.entries);
        raw_node.advance(rd);

        assert!(raw_node.raft.read_states.is_empty());

        let wrequest_ctx = Vec::from("somedata2");
        let _ = raw_node.campaign();

        loop {
            let rd = raw_node.ready();
            let _ = s.append(&rd.entries);

            if rd.soft_state.as_ref().unwrap().lead == raw_node.raft.id {
                raw_node.advance(rd);

                raw_node.read_index(wrequest_ctx.clone());
                break;
            }

            raw_node.advance(rd);
        }

        assert_eq!(raw_node.raft.read_states.len(), 1);
        assert_eq!(raw_node.raft.read_states[0].request_ctx, wrequest_ctx);
    }

    #[test]
    fn test_raw_node_start() {
        let mut s = MemStorage::new();
        let mut c = new_test_config(1, vec![], 10, 1);
        let mut raw_node = RawNode::new(
            &mut c,
            s.clone(),
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        let rd = raw_node.ready();

        assert_eq!(rd.hard_state.get_term(), 1);
        assert_eq!(rd.hard_state.get_commit(), 1);
        assert_eq!(rd.hard_state.get_vote(), 0);
        assert_eq!(rd.must_sync, true);

        let mut cc = ConfChange::new();
        cc.set_change_type(ConfChangeType::ConfChangeAddNode);
        cc.set_node_id(1);
        let ccdata = protobuf::Message::write_to_bytes(&cc).expect("unexpected marshal error");
        assert_eq!(rd.entries.len(), 1);
        assert_eq!(rd.entries[0].get_entry_type(), EntryType::EntryConfChange);
        assert_eq!(rd.entries[0].get_term(), 1);
        assert_eq!(rd.entries[0].get_index(), 1);
        assert_eq!(Vec::from(rd.entries[0].get_data()), ccdata);

        let _ = s.append(&rd.entries);
        raw_node.advance(rd.clone());
        let _ = s.append(&rd.entries);
        raw_node.advance(rd);
        let _ = raw_node.campaign();
        let rd = raw_node.ready();
        let _ = s.append(&rd.entries);
        raw_node.advance(rd.clone());

        let _ = raw_node.propose(Vec::from("foo"));
        let rd = raw_node.ready();

        assert_eq!(rd.hard_state.get_term(), 2);
        assert_eq!(rd.hard_state.get_commit(), 3);
        assert_eq!(rd.hard_state.get_vote(), 1);
        assert_eq!(rd.must_sync, true);

        assert_eq!(rd.entries.len(), 1);
        assert_eq!(rd.entries[0].get_term(), 2);
        assert_eq!(rd.entries[0].get_index(), 3);
        assert_eq!(Vec::from(rd.entries[0].get_data()), Vec::from("foo"));
    }

    #[test]
    fn test_raw_node_restart() {
        let mut st = HardState::new();
        st.set_term(1);
        st.set_commit(1);
        let mut e1 = Entry::new();
        e1.set_term(1);
        e1.set_index(1);
        let mut e2 = Entry::new();
        e2.set_term(1);
        e2.set_index(2);
        e2.set_data(Vec::from("foo"));
        let entries = vec![e1];
        let mut s = MemStorage::new();
        s.set_hard_state(st);
        let _ = s.append(&entries);

        let mut c = new_test_config(1, vec![], 10, 1);
        let mut raw_node = RawNode::new(
            &mut c,
            s.clone(),
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        let rd = raw_node.ready();
        assert_eq!(rd.hard_state, HardState::new());
        assert_eq!(rd.committed_entries.len(), 1);
        assert_eq!(rd.committed_entries[0].get_term(), 1);
        assert_eq!(rd.committed_entries[0].get_index(), 1);
        assert_eq!(rd.must_sync, true);
        raw_node.advance(rd);
        assert_eq!(raw_node.has_ready(), false);
    }

    #[test]
    fn test_raw_node_restart_from_snapshot() {
        let mut snap = Snapshot::new();
        let mut md = SnapshotMetadata::new();
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2]);
        md.set_index(2);
        md.set_term(1);
        md.set_conf_state(cs);
        snap.set_metadata(md);
        let mut st = HardState::new();
        st.set_term(1);
        st.set_commit(3);
        let mut e2 = Entry::new();
        e2.set_term(1);
        e2.set_index(3);
        e2.set_data(Vec::from("foo"));
        let entries = vec![e2];
        let mut s = MemStorage::new();
        s.set_hard_state(st);
        let _ = s.apply_snapshot(snap);
        let _ = s.append(&entries);

        let mut c = new_test_config(1, vec![], 10, 1);
        let mut raw_node = RawNode::new(
            &mut c,
            s.clone(),
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        let rd = raw_node.ready();
        assert_eq!(rd.hard_state, HardState::new());
        assert_eq!(rd.committed_entries.len(), 1);
        assert_eq!(rd.committed_entries[0].get_term(), 1);
        assert_eq!(rd.committed_entries[0].get_index(), 3);
        assert_eq!(rd.must_sync, true);
        raw_node.advance(rd);
        assert_eq!(raw_node.has_ready(), false);
    }

    #[test]
    fn test_raw_node_status() {
        let mut c = new_test_config(1, vec![], 10, 1);
        let raw_node = RawNode::new(
            &mut c,
            MemStorage::new(),
            vec![Peer {
                context: Default::default(),
                id: 1,
            }],
        ).unwrap();

        let status = raw_node.status();
        let mut hs = HardState::new();
        hs.set_commit(1);
        hs.set_term(1);
        assert_eq!(
            status,
            Status {
                id: 1,
                hard_state: hs,
                ..Default::default()
            }
        );
    }
}
