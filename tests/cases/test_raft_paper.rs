use std::collections::HashMap;
use std::iter::Iterator;

use cases::test_raft::{
    ltoa, new_entry, new_message, new_message_with_entries, new_test_raft, Network, StateMachine,
    NOP_STEPPER,
};
use libraft::raft::{Raft, StateType, NONE};
use libraft::raftpb::{Entry, HardState, Message, MessageType};
use libraft::storage::{MemStorage, Storage};
use protobuf::RepeatedField;

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
        }
        StateType::Candidate => {
            r.become_candidate();
        }
        StateType::Leader => {
            r.become_candidate();
            r.become_leader();
        }
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

// tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
#[test]
fn test_reject_stale_term_message() {
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut hd = HardState::new();
    hd.set_term(2);
    r.load_state(&hd);

    let mut m = Message::new();
    m.set_msg_type(MessageType::MsgApp);
    m.set_term(r.term - 1);
    let _ = r.step(m);
}

#[test]
fn test_start_as_follower() {
    let r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    assert_eq!(r.state, StateType::Follower);
}

// tests that if the leader receives a heartbeat tick,
// it will send a MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
#[test]
fn test_leader_bcast_beat() {
    // heartbeat interval
    let hi = 1;
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();

    for i in 0..10 {
        let mut e = Entry::new();
        e.set_index(i + 1);
        r.append_entry(&mut vec![e]);
    }

    for _ in 0..hi {
        r.tick();
    }

    let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
    msgs.sort_by(|a, b| a.get_to().cmp(&b.get_to()));
    assert_eq!(msgs.len(), 2);
    for m in msgs {
        assert_eq!(m.get_msg_type(), MessageType::MsgHeartbeat);
        assert_eq!(m.get_from(), 1);
        assert_eq!(m.get_term(), 1);
    }
}

#[test]
fn test_follower_start_election() {
    non_leader_start_election(StateType::Follower);
}

#[test]
fn test_candidate_start_election() {
    non_leader_start_election(StateType::Candidate);
}

// tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
fn non_leader_start_election(state: StateType) {
    let et = 10;
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, MemStorage::new());

    match state {
        StateType::Follower => {
            r.become_follower(1, 2);
        }
        StateType::Candidate => {
            r.become_candidate();
        }
        _ => {}
    }

    for _ in 1..2 * et {
        r.tick();
    }

    assert_eq!(r.term, 2);
    assert_eq!(r.state, StateType::Candidate);
    assert_eq!(r.votes.get(&r.id).unwrap(), &true);

    let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
    msgs.sort_by(|a, b| a.get_to().cmp(&b.get_to()));

    assert_eq!(msgs.len(), 2);
    for i in 0..2 {
        assert_eq!(msgs[i].get_msg_type(), MessageType::MsgVote);
        assert_eq!(msgs[i].get_from(), 1);
        assert_eq!(msgs[i].get_term(), 2);
    }
}

// tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
#[test]
fn test_leader_election_in_one_round_rpc() {
    let tests = vec![
        (1, vec![], StateType::Leader),
        (3, vec![(2, true), (3, true)], StateType::Leader),
        (3, vec![(2, true)], StateType::Leader),
        (
            5,
            vec![(2, true), (3, true), (4, true), (5, true)],
            StateType::Leader,
        ),
        (5, vec![(2, true), (3, true), (4, true)], StateType::Leader),
        (5, vec![(2, true), (3, true)], StateType::Leader),
        (3, vec![(2, false), (3, false)], StateType::Follower),
        (
            5,
            vec![(2, false), (3, false), (4, false), (5, false)],
            StateType::Follower,
        ),
        (
            5,
            vec![(2, true), (3, false), (4, false), (5, false)],
            StateType::Follower,
        ),
        (3, vec![], StateType::Candidate),
        (5, vec![(2, true)], StateType::Candidate),
        (5, vec![(2, false), (3, false)], StateType::Candidate),
        (5, vec![], StateType::Candidate),
    ];

    for (size, votes, state) in tests {
        let ids: Vec<u64> = (1..size + 1).collect();
        let mut r = new_test_raft(1, ids, 10, 1, MemStorage::new());
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgHup);
        let _ = r.step(m);

        for (id, vote) in votes {
            let mut m = Message::new();
            m.set_from(id);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgVoteResp);
            m.set_term(r.term);
            m.set_reject(!vote);
            let _ = r.step(m);
        }

        assert_eq!(state, r.state);
        assert_eq!(r.term, 1);
    }
}

// tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
#[test]
fn test_follower_vote() {
    let tests = vec![
        (NONE, 1, false),
        (NONE, 2, false),
        (1, 1, false),
        (2, 2, false),
        (1, 2, true),
        (2, 1, true),
    ];

    for (vote, nvote, wreject) in tests {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
        let mut hd = HardState::new();
        hd.set_term(1);
        hd.set_vote(vote);
        r.load_state(&hd);

        let mut m = Message::new();
        m.set_from(nvote);
        m.set_to(1);
        m.set_term(1);
        m.set_msg_type(MessageType::MsgVote);

        let _ = r.step(m);
        let msgs: Vec<Message> = r.msgs.drain(..).collect();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].get_from(), 1);
        assert_eq!(msgs[0].get_to(), nvote);
        assert_eq!(msgs[0].get_msg_type(), MessageType::MsgVoteResp);
        assert_eq!(msgs[0].get_reject(), wreject);
    }
}

// tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
#[test]
fn test_candidate_fallback() {
    let mut m = Message::new();
    m.set_from(2);
    m.set_to(1);
    m.set_term(1);
    m.set_msg_type(MessageType::MsgApp);
    let mut tests = vec![m.clone()];
    let mut m2 = m.clone();
    m2.set_term(2);
    tests.push(m2);

    for mm in tests {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgHup);

        let _ = r.step(m);
        assert_eq!(r.state, StateType::Candidate);

        let term = mm.get_term();
        let _ = r.step(mm);
        assert_eq!(r.state, StateType::Follower);
        assert_eq!(r.term, term);
    }
}

#[test]
fn test_follower_election_timeout_randomized() {
    non_leader_eletion_timeout_randomized(StateType::Follower);
}

// tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
#[test]
fn test_candidate_election_timeout_randomized() {
    non_leader_eletion_timeout_randomized(StateType::Candidate);
}

fn non_leader_eletion_timeout_randomized(state: StateType) {
    let et = 10;
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, MemStorage::new());
    let mut timeout = HashMap::new();

    for _ in 0..50 * et {
        let term = r.term;
        match state {
            StateType::Follower => {
                r.become_follower(term + 1, 2);
            }
            StateType::Candidate => {
                r.become_candidate();
            }
            _ => {}
        }

        let mut time = 0;
        loop {
            let msgs: Vec<Message> = r.msgs.drain(..).collect();
            if !msgs.is_empty() {
                break;
            }
            r.tick();
            time += 1;
        }
        timeout.insert(time, true);
    }

    for i in et + 1..2 * et {
        assert!(timeout.contains_key(&i));
    }
}

#[test]
fn follower_leader_election_timeout_non_conflict() {
    non_leader_election_timeout_non_conflict(StateType::Follower);
}

#[test]
fn candidate_leader_election_timeout_non_conflict() {
    non_leader_election_timeout_non_conflict(StateType::Candidate);
}

// tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
fn non_leader_election_timeout_non_conflict(state: StateType) {
    let et = 10;
    let size = 5;
    let mut rs = vec![];
    let ids: Vec<u64> = (1..size as u64 + 1).collect();

    for i in 0..size {
        rs.push(new_test_raft(ids[i], ids.clone(), et, 1, MemStorage::new()));
    }

    let mut conflicts = 0;

    for _ in 0..1000 {
        for r in rs.iter_mut() {
            let term = r.term;
            match state {
                StateType::Follower => {
                    r.become_follower(term, NONE);
                }
                StateType::Candidate => {
                    r.become_candidate();
                }
                _ => {}
            }
        }

        let mut timeout_num = 0;
        while timeout_num == 0 {
            for r in rs.iter_mut() {
                r.tick();
                let msgs: Vec<Message> = r.msgs.drain(..).collect();
                if !msgs.is_empty() {
                    timeout_num += 1;
                }
            }
        }

        if timeout_num > 1 {
            conflicts += 1;
        }
    }

    assert!(conflicts as f64 / 1000.0 <= 0.3);
}

// tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
#[test]
fn test_leader_start_replication() {
    let s = MemStorage::new();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s.clone());
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r, s.clone());

    let li = r.raft_log.last_index();
    assert_eq!(li, 1);

    let mut e = Entry::new();
    e.set_data(Vec::from("some data"));
    let ents = vec![e];
    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgProp);
    m.set_entries(RepeatedField::from_vec(ents));

    let _ = r.step(m);
    assert_eq!(r.raft_log.last_index(), li + 1);
    assert_eq!(r.raft_log.committed, li);

    let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
    msgs.sort_by(|a, b| a.get_to().cmp(&b.get_to()));
    assert_eq!(msgs[0].get_to(), 2);
    assert_eq!(msgs[1].get_to(), 3);

    for m in msgs {
        assert_eq!(m.get_from(), 1);
        assert_eq!(m.get_term(), 1);
        assert_eq!(m.get_msg_type(), MessageType::MsgApp);
        assert_eq!(m.get_log_term(), 1);
        assert_eq!(m.get_commit(), li);
        assert_eq!(m.get_index(), li);
    }
}

// tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
#[test]
fn test_leader_commit_entry() {
    let s = MemStorage::new();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s.clone());
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r, s.clone());
    let li = r.raft_log.last_index();

    let mut e = Entry::new();
    e.set_data(Vec::from("some data"));
    let ents = vec![e];
    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgProp);
    m.set_entries(RepeatedField::from_vec(ents));

    let _ = r.step(m);

    let msgs: Vec<Message> = r.msgs.drain(..).collect();

    for m in msgs {
        let _ = r.step(accept_and_reply(m));
    }

    assert_eq!(r.raft_log.committed, li + 1);

    let ents = r.raft_log.next_ents();
    assert_eq!(ents.len(), 1);
    assert_eq!(ents[0].get_index(), li + 1);
    assert_eq!(ents[0].get_term(), 1);
    assert_eq!(ents[0].get_data().to_vec(), Vec::from("some data"));

    let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
    msgs.sort_by(|a, b| a.get_to().cmp(&b.get_to()));

    for (i, m) in msgs.iter().enumerate() {
        assert_eq!(m.get_msg_type(), MessageType::MsgApp);
        assert_eq!(i as u64 + 2, m.get_to());
        assert_eq!(li + 1, m.get_commit());
    }
}

// tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
#[test]
fn test_leader_acknowledge_commit() {
    let tests = vec![
        (1, vec![], true),
        (3, vec![], false),
        (3, vec![(2, true)], true),
        (3, vec![(2, true), (3, true)], true),
        (5, vec![], false),
        (5, vec![(2, true)], false),
        (5, vec![(2, true), (3, true)], true),
        (5, vec![(2, true), (3, true), (4, true)], true),
        (5, vec![(2, true), (3, true), (4, true), (5, true)], true),
    ];

    for (size, acceptors, wack) in tests {
        let s = MemStorage::new();
        let mut r = new_test_raft(1, (1..size + 1).collect(), 10, 1, s.clone());
        r.become_candidate();
        r.become_leader();
        commit_noop_entry(&mut r, s.clone());

        let li = r.raft_log.last_index();
        assert_eq!(li, 1);
        assert_eq!(r.raft_log.committed, 1);

        let mut e = Entry::new();
        e.set_data(Vec::from("some data"));
        let ents = vec![e];

        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgProp);
        m.set_entries(RepeatedField::from_vec(ents));
        let _ = r.step(m);

        let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
        msgs.sort_by(|a, b| a.get_to().cmp(&b.get_to()));

        for m in msgs {
            for (to, ack) in &acceptors {
                if *to == m.get_to() && *ack {
                    let _ = r.step(accept_and_reply(m));
                    break;
                }
            }
        }

        assert_eq!(r.raft_log.committed > li, wack);
    }
}

// tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
#[test]
fn test_leader_commit_preceding_entries() {
    let tests = vec![
        vec![],
        vec![new_entry(2, 1)],
        vec![new_entry(1, 1), new_entry(2, 2)],
        vec![new_entry(1, 1)],
    ];

    for mut tt in tests {
        let mut s = MemStorage::new();
        let _ = s.append(&tt);
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        let mut hd = HardState::new();
        hd.set_term(2);
        r.load_state(&hd);
        r.become_candidate();
        r.become_leader();

        let mut e = Entry::new();
        e.set_data(Vec::from("some data"));
        let mut ents = vec![e];

        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgProp);
        m.set_entries(RepeatedField::from_vec(ents.clone()));
        let _ = r.step(m);

        let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
        for m in msgs {
            let _ = r.step(accept_and_reply(m));
        }

        let li = tt.len() as u64;
        let mut e = new_entry(3, li + 2);
        e.set_data(Vec::from("some data"));
        tt.extend_from_slice(&vec![new_entry(3, li + 1), e]);
        assert_eq!(tt, r.raft_log.next_ents());
    }
}

// tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
#[test]
fn test_follower_commit_entry() {
    let tests = vec![
        (vec![new_entry_with_data(1, 1, Vec::from("some data"))], 1),
        (
            vec![
                new_entry_with_data(1, 1, Vec::from("some data")),
                new_entry_with_data(1, 2, Vec::from("some data2")),
            ],
            2,
        ),
        (
            vec![
                new_entry_with_data(1, 1, Vec::from("some data2")),
                new_entry_with_data(1, 2, Vec::from("some data")),
            ],
            2,
        ),
        (
            vec![
                new_entry_with_data(1, 1, Vec::from("some data")),
                new_entry_with_data(1, 2, Vec::from("some data2")),
            ],
            1,
        ),
    ];

    for (ents, commit) in tests {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
        r.become_follower(1, 2);

        let mut m = Message::new();
        m.set_from(2);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgApp);
        m.set_term(1);
        m.set_entries(RepeatedField::from_vec(ents.clone()));
        m.set_commit(commit);

        let _ = r.step(m);
        assert_eq!(r.raft_log.committed, commit);

        let wents = r.raft_log.next_ents();
        assert_eq!(wents, ents[..commit as usize].to_vec());
    }
}

// tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
#[test]
fn test_follower_check_msg_app() {
    let ents = vec![new_entry(1, 1), new_entry(2, 2)];

    let tests = vec![
        (0, 0, 1, false, 0),
        (ents[0].get_term(), ents[0].get_index(), 1, false, 0),
        (ents[1].get_term(), ents[1].get_index(), 2, false, 0),
        (
            ents[0].get_term(),
            ents[1].get_index(),
            ents[1].get_index(),
            true,
            2,
        ),
        (
            ents[0].get_term() + 1,
            ents[1].get_index() + 1,
            ents[1].get_index() + 1,
            true,
            2,
        ),
    ];

    for (term, index, windex, wreject, wreject_hint) in tests {
        let mut s = MemStorage::new();
        let _ = s.append(&ents);
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        let mut hd = HardState::new();
        hd.set_commit(1);
        r.load_state(&hd);
        r.become_follower(2, 2);

        let mut m = Message::new();
        m.set_from(2);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgApp);
        m.set_term(2);
        m.set_log_term(term);
        m.set_index(index);
        let _ = r.step(m);
        let msgs: Vec<Message> = r.msgs.drain(..).collect();

        let mut m1 = Message::new();
        m1.set_from(1);
        m1.set_to(2);
        m1.set_msg_type(MessageType::MsgAppResp);
        m1.set_term(2);
        m1.set_index(windex);
        m1.set_reject(wreject);
        m1.set_reject_hint(wreject_hint);

        let wmsgs = vec![m1];
        assert_eq!(wmsgs, msgs);
    }
}

// tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
#[test]
fn test_follower_append_entries() {
    let tests = vec![
        (
            2,
            2,
            vec![new_entry(3, 3)],
            vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)],
            vec![new_entry(3, 3)],
        ),
        (
            1,
            1,
            vec![new_entry(3, 2), new_entry(4, 3)],
            vec![new_entry(1, 1), new_entry(3, 2), new_entry(4, 3)],
            vec![new_entry(3, 2), new_entry(4, 3)],
        ),
        (
            0,
            0,
            vec![new_entry(1, 1)],
            vec![new_entry(1, 1), new_entry(2, 2)],
            vec![],
        ),
        (
            0,
            0,
            vec![new_entry(3, 1)],
            vec![new_entry(3, 1)],
            vec![new_entry(3, 1)],
        ),
    ];

    for (term, index, ents, wents, wunstable) in tests {
        let mut s = MemStorage::new();
        let _ = s.append(&vec![new_entry(1, 1), new_entry(2, 2)]);
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        r.become_follower(2, 2);

        let mut m = Message::new();
        m.set_from(2);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgApp);
        m.set_term(2);
        m.set_log_term(term);
        m.set_index(index);
        m.set_entries(RepeatedField::from_vec(ents));

        let _ = r.step(m);

        assert_eq!(r.raft_log.all_entries(), wents);
        assert_eq!(r.raft_log.unstable_entries(), wunstable);
    }
}

// tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
#[test]
fn test_leader_sync_follower_log() {
    let ents = vec![
        Entry::new(),
        new_entry(1, 1),
        new_entry(1, 2),
        new_entry(1, 3),
        new_entry(4, 4),
        new_entry(4, 5),
        new_entry(5, 6),
        new_entry(5, 7),
        new_entry(6, 8),
        new_entry(6, 9),
        new_entry(6, 10),
    ];

    let term: u64 = 8;

    let tests = vec![
        vec![
            Entry::new(),
            new_entry(1, 1),
            new_entry(1, 2),
            new_entry(1, 3),
            new_entry(4, 4),
            new_entry(4, 5),
            new_entry(5, 6),
            new_entry(5, 7),
            new_entry(6, 8),
            new_entry(6, 9),
        ],
        vec![
            Entry::new(),
            new_entry(1, 1),
            new_entry(1, 2),
            new_entry(1, 3),
            new_entry(4, 4),
        ],
        vec![
            Entry::new(),
            new_entry(1, 1),
            new_entry(1, 2),
            new_entry(1, 3),
            new_entry(4, 4),
            new_entry(4, 5),
            new_entry(5, 6),
            new_entry(5, 7),
            new_entry(6, 8),
            new_entry(6, 9),
            new_entry(6, 10),
            new_entry(6, 11),
        ],
        vec![
            Entry::new(),
            new_entry(1, 1),
            new_entry(1, 2),
            new_entry(1, 3),
            new_entry(4, 4),
            new_entry(4, 5),
            new_entry(5, 6),
            new_entry(5, 7),
            new_entry(6, 8),
            new_entry(6, 9),
            new_entry(6, 10),
            new_entry(7, 11),
            new_entry(7, 12),
        ],
        vec![
            Entry::new(),
            new_entry(1, 1),
            new_entry(1, 2),
            new_entry(1, 3),
            new_entry(4, 4),
            new_entry(4, 5),
            new_entry(4, 6),
            new_entry(4, 7),
        ],
        vec![
            Entry::new(),
            new_entry(1, 1),
            new_entry(1, 2),
            new_entry(1, 3),
            new_entry(2, 4),
            new_entry(2, 5),
            new_entry(2, 6),
            new_entry(3, 7),
            new_entry(3, 8),
            new_entry(3, 9),
            new_entry(3, 10),
            new_entry(3, 11),
        ],
    ];

    for tents in tests {
        let mut lead_storage = MemStorage::new();
        let _ = lead_storage.append(&ents);
        let mut lead = new_test_raft(1, vec![1, 2, 3], 10, 1, lead_storage);
        let mut hd = HardState::new();
        hd.set_commit(lead.raft_log.last_index());
        hd.set_term(term);

        lead.load_state(&hd);

        let mut follower_storage = MemStorage::new();
        let _ = follower_storage.append(&tents);
        let mut follower = new_test_raft(2, vec![1, 2, 3], 10, 1, follower_storage);
        let mut fhd = HardState::new();
        fhd.set_term(term - 1);
        follower.load_state(&fhd);

        // It is necessary to have a three-node cluster.
        // The second may have more up-to-date log than the first one, so the
        // first node needs the vote from the third node to become the leader.
        let mut n = Network::new(vec![
            Some(StateMachine::new(lead)),
            Some(StateMachine::new(follower)),
            NOP_STEPPER,
        ]);

        n.send(vec![new_message(1, 1, MessageType::MsgHup)]);
        // The election occurs in the term after the one we loaded with
        // lead.loadState above.

        let mut m = new_message(3, 1, MessageType::MsgVoteResp);
        m.set_term(term + 1);
        n.send(vec![m]);
        n.send(vec![new_message_with_entries(
            1,
            1,
            MessageType::MsgProp,
            vec![Entry::new()],
        )]);

        assert_eq!(
            ltoa(&n.peers.get(&1).unwrap().raft_log),
            ltoa(&n.peers.get(&2).unwrap().raft_log)
        );
    }
}

// that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
#[test]
fn test_vote_request() {
    let tests = vec![
        (vec![new_entry(1, 1)], 2),
        (vec![new_entry(1, 1), new_entry(2, 2)], 2),
    ];

    for (ents, wterm) in tests {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
        let mut m = Message::new();
        m.set_from(2);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgApp);
        m.set_term(wterm - 1);
        m.set_log_term(0);
        m.set_index(0);
        m.set_entries(RepeatedField::from_vec(ents.clone()));
        let _ = r.step(m);

        let _: Vec<Message> = r.msgs.drain(..).collect();

        for _ in 0..r.election_timeout * 2 {
            r.tick();
        }

        let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
        msgs.sort_by(|a, b| a.get_to().cmp(&b.get_to()));
        assert_eq!(msgs.len(), 2);

        for (i, m) in msgs.iter().enumerate() {
            assert_eq!(m.get_msg_type(), MessageType::MsgVote);
            assert_eq!(m.get_to(), i as u64 + 2);
            assert_eq!(m.get_term(), wterm);
            let windex = ents[ents.len() - 1].get_index();
            let wlog_term = ents[ents.len() - 1].get_term();
            assert_eq!(windex, m.get_index());
            assert_eq!(wlog_term, m.get_log_term());
        }
    }
}

// tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
#[test]
fn test_voter() {
    let tests = vec![
        // same logterm
        (vec![new_entry(1, 1)], 1, 1, false),
        (vec![new_entry(1, 1)], 1, 2, false),
        (vec![new_entry(1, 1), new_entry(1, 2)], 1, 1, true),
        // candidate higher logterm
        (vec![new_entry(1, 1)], 2, 1, false),
        (vec![new_entry(1, 1)], 2, 2, false),
        (vec![new_entry(1, 1), new_entry(1, 2)], 2, 1, false),
        // voter higher logterm
        (vec![new_entry(2, 1)], 1, 1, true),
        (vec![new_entry(2, 1)], 1, 2, true),
        (vec![new_entry(2, 1), new_entry(1, 2)], 1, 1, true),
    ];

    for (ents, log_term, index, wreject) in tests {
        let mut s = MemStorage::new();
        let _ = s.append(&ents);
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        let mut m = Message::new();
        m.set_from(2);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgVote);
        m.set_term(3);
        m.set_log_term(log_term);
        m.set_index(index);

        let _ = r.step(m);

        let mut msgs: Vec<Message> = r.msgs.drain(..).collect();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].get_msg_type(), MessageType::MsgVoteResp);
        assert_eq!(msgs[0].get_reject(), wreject);
    }
}

// tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
#[test]
fn test_leader_only_commit_log_from_current_term() {}

fn new_entry_with_data(term: u64, index: u64, data: Vec<u8>) -> Entry {
    let mut e = new_entry(term, index);
    e.set_data(data);
    e
}

fn commit_noop_entry<T: Storage>(r: &mut Raft<T>, mut s: MemStorage) {
    if r.state != StateType::Leader {
        panic!("it should only be used when it is the leader")
    }

    r.bcast_append();

    let msgs: Vec<Message> = r.msgs.drain(..).collect();
    for m in msgs {
        if m.get_msg_type() != MessageType::MsgApp
            || m.get_entries().len() != 1
            || m.get_entries()[0].get_data().len() != 0
        {
            panic!("not a message to append noop entry");
        }
        let _ = r.step(accept_and_reply(m));
    }

    let _: Vec<Message> = r.msgs.drain(..).collect();
    let _ = s.append(&r.raft_log.unstable_entries());
    let committed = r.raft_log.committed;
    assert_eq!(committed, 1);
    r.raft_log.applied_to(committed);
    let last_index = r.raft_log.last_index();
    let last_term = r.raft_log.last_term();
    r.raft_log.stable_to(last_index, last_term);
}

fn accept_and_reply(m: Message) -> Message {
    if m.get_msg_type() != MessageType::MsgApp {
        panic!("type should be MsgApp");
    }

    let mut mm = Message::new();
    mm.set_from(m.get_to());
    mm.set_to(m.get_from());
    mm.set_term(m.get_term());
    mm.set_msg_type(MessageType::MsgAppResp);
    mm.set_index(m.get_index() + m.get_entries().len() as u64);
    return mm;
}
