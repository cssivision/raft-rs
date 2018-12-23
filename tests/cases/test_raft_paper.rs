use libraft::raft::StateType;
use cases::test_raft::new_test_raft;
use libraft::storage::{MemStorage, Storage};
use libraft::raftpb::{Message, MessageType};

// tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
fn update_term_from_message(state: StateType) {
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    match state {
        StateType::Follower => {
            r.become_follower(1, 2);
        },
        StateType::Candidate => {
            r.become_candidate();
        },
        StateType::Leader => {
            r.become_candidate();
            r.become_leader();
        },
        _ => {}
    }

    let mut m = Message::new();
    m.set_msg_type(MessageType::MsgApp);
    m.set_term(2);
    let _ = r.step(m);
    assert_eq!(r.term, 2);
    assert_eq!(r.state, StateType::Follower);
}

#[test]
fn test_follower_update_term_from_message() {
    update_term_from_message(StateType::Follower);
}

#[test]
fn test_candidate_update_term_from_message() {
    update_term_from_message(StateType::Candidate);
}

#[test]
fn test_leader_update_term_from_message() {
    update_term_from_message(StateType::Leader);
}