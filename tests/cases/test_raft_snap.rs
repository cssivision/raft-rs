use libraft::raftpb::{Entry, Message, MessageType, Snapshot};
use libraft::storage::{MemStorage, Storage};
use protobuf::RepeatedField;

use cases::test_raft::{new_snapshot, new_test_raft};

fn test_snapshot() -> Snapshot {
    new_snapshot(11, 11, vec![], vec![1, 2])
}

#[test]
fn test_sending_snapshot_set_pending_snapshot() {
    let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());

    sm.restore(test_snapshot());
    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next = sm.raft_log.first_index();

    let mut m = Message::new();
    m.set_from(2);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgAppResp);
    m.set_index(sm.prs.get(&2).unwrap().next - 1);
    m.set_reject(true);
    let _ = sm.step(m);
    assert_eq!(sm.prs.get(&2).unwrap().pending_snapshot, 11);
}

#[test]
fn test_pending_snapshot_pause_replcation() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    sm.restore(test_snapshot());
    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().become_snapshot(11);

    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgProp);
    let mut e = Entry::new();
    e.set_data(Vec::from("somedata"));
    m.set_entries(RepeatedField::from_vec(vec![e]));
    let _ = sm.step(m);
    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert!(msgs.is_empty());
}

#[test]
fn test_snapshot_failure() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    sm.restore(test_snapshot());
    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next = 1;
    sm.prs.get_mut(&2).unwrap().become_snapshot(11);

    let mut m = Message::new();
    m.set_from(2);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgSnapStatus);
    m.set_reject(true);
    let _ = sm.step(m);
    assert_eq!(sm.prs.get(&2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs.get(&2).unwrap().next, 1);
    assert!(sm.prs.get(&2).unwrap().paused);
}

#[test]
fn test_snapshot_succeed() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    sm.restore(test_snapshot());
    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next = 1;
    sm.prs.get_mut(&2).unwrap().become_snapshot(11);
    let mut m = Message::new();
    m.set_from(2);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgSnapStatus);
    m.set_reject(false);
    let _ = sm.step(m);
    assert_eq!(sm.prs.get(&2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs.get(&2).unwrap().next, 12);
    assert!(sm.prs.get(&2).unwrap().paused);
}

#[test]
fn test_snapshot_abort() {
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    sm.restore(test_snapshot());
    sm.become_candidate();
    sm.become_leader();

    sm.prs.get_mut(&2).unwrap().next = 1;
    sm.prs.get_mut(&2).unwrap().become_snapshot(11);
    let mut m = Message::new();
    m.set_from(2);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgAppResp);
    m.set_index(11);
    let _ = sm.step(m);
    assert_eq!(sm.prs.get(&2).unwrap().pending_snapshot, 0);
    assert_eq!(sm.prs.get(&2).unwrap().next, 12);
}
