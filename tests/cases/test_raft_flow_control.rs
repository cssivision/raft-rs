use cases::test_raft::new_test_raft;
use libraft::raftpb::{Entry, Message, MessageType};
use libraft::storage::{MemStorage, Storage};
use protobuf::RepeatedField;

// ensure
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
// when in replicated state and inflights is full, progress will be paused.
#[test]
fn test_msg_app_flow_control_full() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    r.prs.get_mut(&2).unwrap().become_replicate();

    for _ in 0..r.max_inflight {
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgProp);
        let mut e = Entry::new();
        e.set_data(Vec::from("somedata"));
        m.set_entries(RepeatedField::from_vec(vec![e]));
        let _ = r.step(m);
        let ms: Vec<Message> = r.msgs.drain(..).collect();
        assert_eq!(ms.len(), 1);
    }

    assert!(r.prs.get(&2).unwrap().ins.full());

    for _ in 0..10 {
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgProp);
        let mut e = Entry::new();
        e.set_data(Vec::from("somedata"));
        m.set_entries(RepeatedField::from_vec(vec![e]));
        let _ = r.step(m);
        let ms: Vec<Message> = r.msgs.drain(..).collect();
        assert_eq!(ms.len(), 0);
    }
}

#[test]
fn test_msg_app_flow_control_move_forward() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    r.prs.get_mut(&2).unwrap().become_replicate();

    for _ in 0..r.max_inflight {
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgProp);
        let mut e = Entry::new();
        e.set_data(Vec::from("somedata"));
        m.set_entries(RepeatedField::from_vec(vec![e]));
        let _ = r.step(m);
        let ms: Vec<Message> = r.msgs.drain(..).collect();
        assert_eq!(ms.len(), 1);
    }

    // 1 is noop, 2 is the first proposal we just sent.
	// so we start with 2.
    for i in 2..r.max_inflight {
        // move forward the window
        let mut m = Message::new();
        m.set_from(2);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgAppResp);
        m.set_index(i);
        let _ = r.step(m);
        let _: Vec<Message> = r.msgs.drain(..).collect();

        // fill in the inflights window again
        {
            let mut m = Message::new();
            m.set_from(1);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgProp);
            let mut e = Entry::new();
            e.set_data(Vec::from("somedata"));
            m.set_entries(RepeatedField::from_vec(vec![e]));
            let _ = r.step(m);
            let ms: Vec<Message> = r.msgs.drain(..).collect();
            assert_eq!(ms.len(), 1);
        }

        assert!(r.prs.get(&2).unwrap().ins.full());

         for ii in 0..i {
            let mut m = Message::new();
            m.set_from(2);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgAppResp);
            m.set_index(ii);
            let _ = r.step(m);
            let _: Vec<Message> = r.msgs.drain(..).collect();
            assert!(r.prs.get(&2).unwrap().ins.full());
        }
    }
}

// ensures a heartbeat response
// frees one slot if the window is full.
#[test]
fn test_msg_app_flow_control_recv_heartbeat() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    r.prs.get_mut(&2).unwrap().become_replicate();

    for _ in 0..r.max_inflight {
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgProp);
        let mut e = Entry::new();
        e.set_data(Vec::from("somedata"));
        m.set_entries(RepeatedField::from_vec(vec![e]));
        let _ = r.step(m);
        let ms: Vec<Message> = r.msgs.drain(..).collect();
        assert_eq!(ms.len(), 1);
    }

    for tt in 0..5 {
        assert!(r.prs.get_mut(&2).unwrap().ins.full());

        // recv tt msgHeartbeatResp and expect one free slot
        for _ in 0..tt {
            let mut m = Message::new();
            m.set_from(2);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgHeartbeatResp);
            let _ = r.step(m);
            let _: Vec<Message> = r.msgs.drain(..).collect();
            assert!(!r.prs.get_mut(&2).unwrap().ins.full());

            // one slot
            let mut m = Message::new();
            m.set_from(1);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgProp);
            let mut e = Entry::new();
            e.set_data(Vec::from("somedata"));
            m.set_entries(RepeatedField::from_vec(vec![e]));
            let _ = r.step(m);
            let ms: Vec<Message> = r.msgs.drain(..).collect();
            assert_eq!(ms.len(), 1);

            for _ in 0..10 {
                let mut m = Message::new();
                m.set_from(1);
                m.set_to(1);
                m.set_msg_type(MessageType::MsgProp);
                let mut e = Entry::new();
                e.set_data(Vec::from("somedata"));
                m.set_entries(RepeatedField::from_vec(vec![e]));
                let _ = r.step(m);
                let ms: Vec<Message> = r.msgs.drain(..).collect();
                assert_eq!(ms.len(), 0);
            }

            // clear all pending messages.
            let mut m = Message::new();
            m.set_from(2);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgHeartbeatResp);
            let _ = r.step(m);
            let _: Vec<Message> = r.msgs.drain(..).collect();
        }
    }
}
