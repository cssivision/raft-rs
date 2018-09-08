use libraft::raftpb::{Message, MessageType, Snapshot};
use libraft::storage::{MemStorage, Storage};

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
