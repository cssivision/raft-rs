use cases::test_raft::new_test_raft;
use libraft::storage::{MemStorage, Storage};

// ensure
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
#[test]
fn test_msg_app_flow_control_full() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    r.prs.get_mut(&2).unwrap().become_replicate();
}
