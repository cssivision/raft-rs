use std::collections::HashMap;

use cases::test_raft::new_test_raft;
use libraft::raft::{StateType, NONE};
use libraft::raftpb::{Entry, HardState, Message, MessageType};
use libraft::storage::{MemStorage, Storage};

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

// tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
