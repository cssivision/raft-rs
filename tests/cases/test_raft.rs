use std::cmp;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;

use libraft::errors::{Error, Result};
use libraft::log_unstable::Unstable;
use libraft::progress::{Inflights, Progress, ProgressState};
use libraft::raft::{Config, Raft, StateType, NONE};
use libraft::raft_log::RaftLog;
use libraft::raftpb::{
    ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot, SnapshotMetadata,
};
use libraft::read_only::ReadOnlyOption;
use libraft::storage::{MemStorage, Storage};
use libraft::util::{vote_msg_resp_type, NO_LIMIT};

use protobuf::{self, RepeatedField};
use rand::{self, Rng};

pub fn new_snapshot(index: u64, term: u64, learners: Vec<u64>, nodes: Vec<u64>) -> Snapshot {
    let mut s = Snapshot::new();
    let mut metadata = SnapshotMetadata::new();
    metadata.set_index(index);
    metadata.set_term(term);
    let mut conf_state = ConfState::new();
    conf_state.set_learners(learners);
    conf_state.set_nodes(nodes);
    metadata.set_conf_state(conf_state);
    s.set_metadata(metadata);
    s
}

pub fn new_pre_vote_migration_cluster() -> Network {
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());

    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    n3.become_follower(1, NONE);

    n1.pre_vote = true;
    n2.pre_vote = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
        Some(StateMachine::new(n3)),
    ]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("some data"))],
    )]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Candidate);

    assert_eq!(nt.peers.get(&1).unwrap().term, 2);
    assert_eq!(nt.peers.get(&2).unwrap().term, 2);
    assert_eq!(nt.peers.get(&3).unwrap().term, 4);

    nt.peers.get_mut(&3).unwrap().pre_vote = true;
    nt.recover();

    nt
}

fn new_entry(term: u64, index: u64) -> Entry {
    let mut e = Entry::new();
    e.set_index(index);
    e.set_term(term);
    e
}

fn ltoa<T: Storage>(l: &RaftLog<T>) -> String {
    let mut s = format!("committed: {}\n", l.committed);
    s.push_str(&format!("applied:  {}\n", l.applied));
    for (i, e) in l.all_entries().iter().enumerate() {
        s.push_str(&format!("#{}: {:?}\n", i, e));
    }
    s
}

fn new_entry_with_data(data: Vec<u8>) -> Entry {
    let mut e = Entry::new();
    e.set_data(data);
    e
}

fn new_message_with_entries(
    from: u64,
    to: u64,
    msg_type: MessageType,
    entries: Vec<Entry>,
) -> Message {
    let mut msg = new_message(from, to, msg_type);
    msg.set_entries(RepeatedField::from_vec(entries));
    msg
}

fn new_message(from: u64, to: u64, msg_type: MessageType) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(msg_type);
    m
}

pub fn new_test_config(id: u64, peers: Vec<u64>, election: u64, heartbeat: u64) -> Config {
    Config {
        id,
        peers,
        election_tick: election,
        heartbeat_tick: heartbeat,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

pub fn new_test_raft<T: Storage>(
    id: u64,
    peers: Vec<u64>,
    election: u64,
    heartbeat: u64,
    storage: T,
) -> Raft<T> {
    Raft::new(
        &mut new_test_config(id, peers, election, heartbeat),
        storage,
    )
}

fn new_test_learner_raft<T: Storage>(
    id: u64,
    peers: Vec<u64>,
    learners: Vec<u64>,
    election: u64,
    heartbeat: u64,
    storage: T,
) -> Raft<T> {
    let mut cfg = new_test_config(id, peers, election, heartbeat);
    cfg.learners = learners;
    Raft::new(&mut cfg, storage)
}

#[derive(Default, Debug, PartialEq, Eq, Hash)]
pub struct Connem {
    from: u64,
    to: u64,
}

#[derive(Default)]
pub struct Network {
    peers: HashMap<u64, StateMachine>,
    storage: HashMap<u64, MemStorage>,
    dropm: HashMap<Connem, f64>,
    ignorem: HashMap<MessageType, bool>,
}

const NOP_STEPPER: Option<StateMachine> = Some(StateMachine { raft: None });

#[derive(Default)]
pub struct StateMachine {
    raft: Option<Raft<MemStorage>>,
}

impl StateMachine {
    fn new(r: Raft<MemStorage>) -> StateMachine {
        StateMachine { raft: Some(r) }
    }

    fn step(&mut self, m: Message) -> Result<()> {
        match self.raft {
            None => Ok(()),
            Some(_) => Raft::step(self, m),
        }
    }

    pub fn read_messages(&mut self) -> Vec<Message> {
        match self.raft {
            Some(_) => self.msgs.drain(..).collect(),
            None => vec![],
        }
    }
}

impl Deref for StateMachine {
    type Target = Raft<MemStorage>;
    fn deref(&self) -> &Raft<MemStorage> {
        self.raft.as_ref().unwrap()
    }
}

impl DerefMut for StateMachine {
    fn deref_mut(&mut self) -> &mut Raft<MemStorage> {
        self.raft.as_mut().unwrap()
    }
}

impl Network {
    fn new(peers: Vec<Option<StateMachine>>) -> Network {
        Network::new_with_config(peers, false)
    }

    fn new_with_config(mut peers: Vec<Option<StateMachine>>, pre_vote: bool) -> Network {
        let size = peers.len();
        let peer_addrs: Vec<u64> = (1..size as u64 + 1).collect();
        let mut npeers = HashMap::new();
        let mut nstorage = HashMap::new();

        for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
            match p {
                None => {
                    let s = MemStorage::new();
                    nstorage.insert(id, s.clone());
                    let mut cfg = new_test_config(id, peer_addrs.clone(), 10, 1);
                    if pre_vote {
                        cfg.pre_vote = true;
                    }
                    let sm = StateMachine::new(Raft::new(&mut cfg, s.clone()));
                    npeers.insert(id, sm);
                }
                Some(mut p) => {
                    if p.raft.is_some() {
                        p.id = id;
                        let learner_prs = p.take_learner_prs();
                        p.learner_prs = HashMap::new();
                        p.prs = HashMap::new();

                        for i in &peer_addrs {
                            if learner_prs.contains_key(i) {
                                p.learner_prs.insert(
                                    *i,
                                    Progress {
                                        is_learner: true,
                                        ..Default::default()
                                    },
                                );
                            } else {
                                p.prs.insert(
                                    *i,
                                    Progress {
                                        ..Default::default()
                                    },
                                );
                            }
                        }

                        let term = p.term;
                        p.reset(term);
                    }
                    npeers.insert(id, p);
                }
            }
        }

        Network {
            peers: npeers,
            storage: nstorage,
            ..Default::default()
        }
    }

    fn send(&mut self, msgs: Vec<Message>) {
        let mut msgs = msgs;
        while !msgs.is_empty() {
            let mut new_msgs = vec![];
            for m in msgs.drain(..) {
                let resp = {
                    let p = self.peers.get_mut(&m.get_to()).unwrap();
                    let _ = p.step(m);
                    p.read_messages()
                };
                new_msgs.append(&mut self.filter(resp));
            }
            msgs.append(&mut new_msgs);
        }
    }

    fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert(Connem { from, to }, perc);
    }

    fn cut(&mut self, one: u64, other: u64) {
        self.drop(one, other, 2.0);
        self.drop(other, one, 2.0);
    }

    fn ignore(&mut self, msg_type: MessageType) {
        self.ignorem.insert(msg_type, true);
    }

    fn recover(&mut self) {
        self.ignorem.clear();
        self.dropm.clear();
    }

    fn isolate(&mut self, id: u64) {
        for i in 0..self.peers.len() {
            let nid = i as u64 + 1;
            if nid != id {
                self.drop(id, nid, 1.0);
                self.drop(nid, id, 1.0);
            }
        }
    }

    fn filter(&self, msgs: Vec<Message>) -> Vec<Message> {
        let mut mm = vec![];

        for m in msgs {
            if self.ignorem.contains_key(&m.get_msg_type()) {
                continue;
            }

            match m.get_msg_type() {
                MessageType::MsgHup => {
                    panic!("unexpected msgHup");
                }
                _ => {
                    let perc = self.dropm.get(&Connem {
                        from: m.get_from(),
                        to: m.get_to(),
                    });
                    if let Some(&perc) = perc {
                        let n: f64 = rand::thread_rng().gen_range(0f64, 1f64);
                        if n < perc {
                            continue;
                        }
                    }
                }
            }
            mm.push(m);
        }

        mm
    }
}

#[test]
fn test_become_probe() {
    let matched = 1;
    let tests = vec![
        (
            Progress {
                state: ProgressState::Replicate,
                next: 5,
                matched: matched,
                ins: Inflights::new(256),
                ..Default::default()
            },
            2,
        ),
        (
            Progress {
                state: ProgressState::Snapshot,
                next: 5,
                pending_snapshot: 10,
                matched: matched,
                ins: Inflights::new(256),
                ..Default::default()
            },
            11,
        ),
        (
            Progress {
                state: ProgressState::Snapshot,
                next: 5,
                pending_snapshot: 0,
                matched: matched,
                ins: Inflights::new(256),
                ..Default::default()
            },
            2,
        ),
    ];

    for (mut p, wnext) in tests {
        p.become_probe();
        assert_eq!(matched, p.matched);
        assert_eq!(wnext, p.next);
    }
}

#[test]
fn test_become_replicate() {
    let mut p = Progress {
        state: ProgressState::Probe,
        next: 5,
        matched: 1,
        ins: Inflights::new(256),
        ..Default::default()
    };

    p.become_replicate();
    assert_eq!(p.state, ProgressState::Replicate);
    assert_eq!(p.matched, 1);
    assert_eq!(p.matched + 1, p.next);
}

#[test]
fn test_become_snapshot() {
    let mut p = Progress {
        state: ProgressState::Probe,
        next: 5,
        matched: 1,
        ins: Inflights::new(256),
        ..Default::default()
    };
    p.become_snapshot(10);
    assert_eq!(p.state, ProgressState::Snapshot);
    assert_eq!(p.pending_snapshot, 10);
}

#[test]
fn test_progress_update() {
    let prev_m = 3;
    let prev_n = 5;

    let tests = vec![
        (prev_m - 1, prev_m, prev_n, false),
        (prev_m, prev_m, prev_n, false),
        (prev_m + 1, prev_m + 1, prev_n, true),
        (prev_m + 2, prev_m + 2, prev_n + 1, true),
    ];

    for (update, wm, wn, wok) in tests {
        let mut p = Progress {
            matched: prev_m,
            next: prev_n,
            ..Default::default()
        };

        assert_eq!(p.maybe_update(update), wok);
        assert_eq!(p.matched, wm);
        assert_eq!(p.next, wn);
    }
}

#[test]
fn test_progress_maybe_decr() {
    let tests = vec![
        (ProgressState::Replicate, 5, 10, 5, 5, false, 10),
        (ProgressState::Replicate, 5, 10, 4, 4, false, 10),
        (ProgressState::Replicate, 5, 10, 9, 9, true, 6),
        (ProgressState::Probe, 0, 0, 0, 0, false, 0),
        (ProgressState::Probe, 0, 10, 5, 5, false, 10),
        (ProgressState::Probe, 0, 10, 9, 9, true, 9),
        (ProgressState::Probe, 0, 2, 1, 1, true, 1),
        (ProgressState::Probe, 0, 1, 0, 0, true, 1),
        (ProgressState::Probe, 0, 10, 9, 2, true, 3),
        (ProgressState::Probe, 0, 10, 9, 0, true, 1),
    ];

    for (state, m, n, rejected, last, w, wn) in tests {
        let mut p = Progress {
            state,
            matched: m,
            next: n,
            ..Default::default()
        };

        assert_eq!(w, p.maybe_decr_to(rejected, last));
        assert_eq!(p.matched, m);
        assert_eq!(p.next, wn);
    }
}

#[test]
fn test_progress_is_paused() {
    let tests = vec![
        (ProgressState::Probe, false, false),
        (ProgressState::Probe, true, true),
        (ProgressState::Replicate, false, false),
        (ProgressState::Replicate, true, false),
        (ProgressState::Snapshot, false, true),
        (ProgressState::Snapshot, true, true),
    ];

    for (state, paused, w) in tests {
        let mut p = Progress {
            state,
            paused,
            ins: Inflights::new(256),
            ..Default::default()
        };

        assert_eq!(p.is_paused(), w);
    }
}

#[test]
fn test_progress_resume() {
    let mut p = Progress {
        next: 2,
        paused: true,
        ..Default::default()
    };

    p.maybe_decr_to(1, 1);
    assert!(!p.paused);
    p.paused = true;
    p.maybe_update(2);
    assert!(!p.paused);
}

#[test]
fn test_progress_resume_by_heartbeat_resp() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    r.prs.get_mut(&2).unwrap().paused = true;

    r.step(new_message(1, 1, MessageType::MsgHeartbeat)).is_ok();
    assert!(r.prs.get(&2).unwrap().paused);
    r.prs.get_mut(&2).unwrap().become_replicate();
    r.step(new_message(2, 1, MessageType::MsgHeartbeatResp))
        .is_ok();
    assert!(!r.prs.get(&2).unwrap().paused);
}

#[test]
fn test_progress_paused() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgProp);
    let mut e = Entry::new();
    e.set_data(Vec::from("somedata"));
    m.set_entries(RepeatedField::from_vec(vec![e]));

    r.step(m.clone()).is_ok();
    r.step(m.clone()).is_ok();
    r.step(m).is_ok();
    assert_eq!(r.msgs.len(), 1);
}

#[test]
fn test_leader_election() {
    leader_election(false);
}

#[test]
fn test_leader_election_pre_vote() {
    leader_election(true);
}

fn leader_election(pre_vote: bool) {
    let tests = vec![
        (
            Network::new_with_config(vec![None, None, None], pre_vote),
            StateType::Leader,
            1,
        ),
        (
            Network::new_with_config(vec![None, None, NOP_STEPPER], pre_vote),
            StateType::Leader,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER], pre_vote),
            StateType::Candidate,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER, None], pre_vote),
            StateType::Candidate,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER, None, None], pre_vote),
            StateType::Leader,
            1,
        ),
    ];

    for (mut network, state, exp_term) in tests {
        network.send(vec![new_message(1, 1, MessageType::MsgHup)]);
        let sm = network.peers.get(&1).unwrap();
        let (exp_state, exp_term) = if state == StateType::Candidate && pre_vote {
            (StateType::PreCandidate, 0)
        } else {
            (state, exp_term)
        };

        assert_eq!(sm.state, exp_state);
        assert_eq!(sm.term, exp_term);
    }
}

#[test]
fn test_learner_election_timeout() {
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, MemStorage::new());
    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, MemStorage::new());
    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);

    n2.randomized_election_timeout = n2.election_timeout;
    for _ in 0..n2.election_timeout {
        n2.tick();
    }

    assert_eq!(n2.state, StateType::Follower);
}

#[test]
fn test_learner_promotion() {
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, MemStorage::new());
    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, MemStorage::new());
    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
    ]);

    assert!(nt.peers[&1].state != StateType::Leader);
    let election_timeout = nt.peers[&1].election_timeout;
    nt.peers.get_mut(&1).unwrap().randomized_election_timeout = election_timeout;
    for _ in 0..election_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    assert_eq!(nt.peers[&1].state, StateType::Leader);
    assert_eq!(nt.peers[&2].state, StateType::Follower);

    nt.send(vec![new_message(1, 1, MessageType::MsgBeat)]);

    nt.peers.get_mut(&1).unwrap().add_node(2);
    nt.peers.get_mut(&2).unwrap().add_node(2);
    assert!(!nt.peers.get_mut(&2).unwrap().is_learner);

    let election_timeout = nt.peers[&2].election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = election_timeout;
    for _ in 0..election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(2, 2, MessageType::MsgBeat)]);
    assert_eq!(nt.peers[&1].state, StateType::Follower);
    assert_eq!(nt.peers[&2].state, StateType::Leader);
}

#[test]
fn test_learner_can_not_vote() {
    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, MemStorage::new());
    n2.become_follower(1, NONE);
    let mut m = new_message(1, 2, MessageType::MsgVote);
    m.set_index(11);
    m.set_log_term(11);
    let _ = n2.step(m);
    assert!(n2.msgs.is_empty());
}

#[test]
fn test_leader_cycle() {
    leader_cycle(false);
}

#[test]
fn test_leader_cycle_pre_vote() {
    leader_cycle(true);
}

// leader_cycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean slate (as they do in
// test_leader_election)
fn leader_cycle(pre_vote: bool) {
    let mut n = Network::new_with_config(vec![None, None, None], pre_vote);

    for campaigner_id in 1..4 {
        n.send(vec![new_message(
            campaigner_id,
            campaigner_id,
            MessageType::MsgHup,
        )]);

        for (_, peer) in &n.peers {
            if peer.id == campaigner_id {
                assert_eq!(peer.state, StateType::Leader);
            } else {
                assert_eq!(peer.state, StateType::Follower);
            }
        }
    }
}

#[test]
fn test_leader_election_overwrite_newer_logs() {
    leader_election_overwrite_newer_logs(false);
}

#[test]
fn test_leader_election_overwrite_newer_logs_pre_vote() {
    leader_election_overwrite_newer_logs(true);
}

fn ents_with_config(pre_vote: bool, terms: Vec<u64>) -> StateMachine {
    let mut storage = MemStorage::new();
    for (i, &term) in terms.iter().enumerate() {
        let mut e = Entry::new();
        e.set_index(i as u64 + 1);
        e.set_term(term);
        storage.append(&vec![e]).is_ok();
    }

    let mut cfg = new_test_config(1, vec![], 5, 1);
    cfg.pre_vote = pre_vote;
    let mut r = Raft::new(&mut cfg, storage);
    r.reset(terms[terms.len() - 1]);
    StateMachine::new(r)
}

fn vote_with_config(pre_vote: bool, vote: u64, term: u64) -> StateMachine {
    let mut storage = MemStorage::new();
    let mut hs = HardState::new();
    hs.set_term(term);
    hs.set_vote(vote);
    storage.set_hard_state(hs);

    let mut cfg = new_test_config(1, vec![], 5, 1);
    cfg.pre_vote = pre_vote;
    let mut r = Raft::new(&mut cfg, storage);
    r.reset(term);
    StateMachine::new(r)
}

fn leader_election_overwrite_newer_logs(pre_vote: bool) {
    // This network represents the results of the following sequence of
    // events:
    // - Node 1 won the election in term 1.
    // - Node 1 replicated a log entry to node 2 but died before sending
    //   it to other nodes.
    // - Node 3 won the second election in term 2.
    // - Node 3 wrote an entry to its logs but died without sending it
    //   to any other nodes.
    //
    // At this point, nodes 1, 2, and 3 all have uncommitted entries in
    // their logs and could win an election at term 3. The winner's log
    // entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
    // the case where older log entries are overwritten, so this test
    // focuses on the case where the newer entries are lost).
    let mut n = Network::new_with_config(
        vec![
            Some(ents_with_config(pre_vote, vec![1])), // Node 1: Won first election
            Some(ents_with_config(pre_vote, vec![1])), // Node 2: Got logs from node 1
            Some(ents_with_config(pre_vote, vec![2])), // Node 3: Won second election
            Some(vote_with_config(pre_vote, 3, 2)),    // Node 4: Voted but didn't get logs
            Some(vote_with_config(pre_vote, 3, 2)),    // Node 5: Voted but didn't get logs
        ],
        pre_vote,
    );

    // Node 1 campaigns. The election fails because a quorum of nodes
    // know about the election that already happened at term 2. Node 1's
    // term is pushed ahead to 2.
    n.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(n.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(n.peers.get(&1).unwrap().term, 2);

    // Node 1 campaigns again with a higher term. This time it succeeds.
    n.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(n.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(n.peers.get(&1).unwrap().term, 3);

    // Now all nodes agree on a log entry with term 1 at index 1 (and
    // term 3 at index 2).
    for i in 1..n.peers.len() + 1 {
        let entries = n.peers.get(&(i as u64)).unwrap().raft_log.all_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].term, 1);
        assert_eq!(entries[1].term, 3);
    }
}

#[test]
fn test_vote_from_any_state() {
    vote_from_any_state(MessageType::MsgVote);
}

#[test]
fn test_pre_vote_from_any_state() {
    vote_from_any_state(MessageType::MsgPreVote);
}

fn vote_from_any_state(vt: MessageType) {
    for st in vec![
        StateType::Follower,
        StateType::Candidate,
        StateType::PreCandidate,
        StateType::Leader,
    ] {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
        r.term = 1;
        let term = r.term;
        match st {
            StateType::Follower => {
                r.become_follower(term, 3);
            }
            StateType::PreCandidate => {
                r.become_pre_candidate();
            }
            StateType::Candidate => {
                r.become_candidate();
            }
            StateType::Leader => {
                r.become_candidate();
                r.become_leader();
            }
        }

        let orig_term = r.term;
        let new_term = r.term + 1;

        let mut msg = Message::new();
        msg.set_from(2);
        msg.set_to(1);
        msg.set_msg_type(vt);
        msg.set_term(new_term);
        msg.set_log_term(orig_term);
        msg.set_index(42);
        assert!(r.step(msg).is_ok());
        assert_eq!(r.msgs.len(), 1);
        let resp = &r.msgs[0];
        assert_eq!(resp.get_msg_type(), vote_msg_resp_type(vt));
        assert!(!resp.get_reject());

        if vt == MessageType::MsgVote {
            assert_eq!(r.state, StateType::Follower);
            assert_eq!(r.term, new_term);
            assert_eq!(r.vote, 2);
        } else {
            assert_eq!(r.state, st);
            assert_eq!(r.term, orig_term);
            assert!(r.vote == NONE || r.vote == 1);
        }
    }
}

#[test]
fn test_log_replication() {
    let tests = vec![
        (
            Network::new(vec![None, None, None]),
            vec![new_message_with_entries(
                1,
                1,
                MessageType::MsgProp,
                vec![new_entry_with_data(Vec::from("somedata"))],
            )],
            2,
        ),
        (
            Network::new(vec![None, None, None]),
            vec![
                new_message_with_entries(
                    1,
                    1,
                    MessageType::MsgProp,
                    vec![new_entry_with_data(Vec::from("somedata"))],
                ),
                new_message(1, 2, MessageType::MsgHup),
                new_message_with_entries(
                    1,
                    2,
                    MessageType::MsgProp,
                    vec![new_entry_with_data(Vec::from("somedata"))],
                ),
            ],
            4,
        ),
    ];

    for (mut network, msgs, wcommitted) in tests {
        network.send(vec![new_message(1, 1, MessageType::MsgHup)]);
        let mut props_data = vec![];
        for m in msgs {
            if m.get_msg_type() == MessageType::MsgProp {
                props_data.push(m.get_entries()[0].get_data().to_vec());
            }
            network.send(vec![m]);
        }

        for (j, mut sm) in network.peers {
            assert_eq!(sm.raft_log.committed, wcommitted);

            let mut ents = vec![];
            for e in next_ents(&mut sm, network.storage.get_mut(&j).unwrap()) {
                if !e.get_data().is_empty() {
                    ents.push(e);
                }
            }

            for k in 0..props_data.len() {
                assert_eq!(props_data[0], ents[k].get_data());
            }
        }
    }
}

#[test]
fn test_learner_log_replication() {
    let n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, MemStorage::new());
    let n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, MemStorage::new());

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
    ]);

    nt.peers.get_mut(&1).unwrap().become_follower(1, NONE);
    nt.peers.get_mut(&2).unwrap().become_follower(1, NONE);

    let election_timeout = nt.peers[&1].election_timeout;
    nt.peers.get_mut(&1).unwrap().randomized_election_timeout = election_timeout;
    for _ in 0..election_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    assert_eq!(StateType::Leader, nt.peers.get(&1).unwrap().state);
    assert!(nt.peers.get(&2).unwrap().is_learner);

    let next_committed = nt.peers.get(&1).unwrap().raft_log.committed + 1;

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    assert_eq!(next_committed, nt.peers.get(&1).unwrap().raft_log.committed);
    assert_eq!(
        nt.peers.get(&1).unwrap().raft_log.committed,
        nt.peers.get(&2).unwrap().raft_log.committed
    );

    assert_eq!(
        nt.peers.get(&2).unwrap().raft_log.committed,
        nt.peers.get(&1).unwrap().get_progress(2).unwrap().matched,
    )
}

fn next_ents(sm: &mut Raft<MemStorage>, s: &mut MemStorage) -> Vec<Entry> {
    let _ = s.append(&sm.raft_log.unstable_entries());
    let (last_index, last_term) = (sm.raft_log.last_index(), sm.raft_log.last_term());
    sm.raft_log.stable_to(last_index, last_term);
    let ents = sm.raft_log.next_ents();
    let committed = sm.raft_log.committed;
    sm.raft_log.applied_to(committed);
    ents
}

fn stable_ents(sm: &mut Raft<MemStorage>) -> Vec<Entry> {
    let unstable_ents = sm.raft_log.unstable_entries();
    let _ = sm.raft_log.storage.append(&unstable_ents);
    let (last_index, last_term) = (sm.raft_log.last_index(), sm.raft_log.last_term());
    sm.raft_log.stable_to(last_index, last_term);
    let ents = sm.raft_log.next_ents();
    let committed = sm.raft_log.committed;
    sm.raft_log.applied_to(committed);
    ents
}

#[test]
fn test_single_node_committed() {
    let mut tt = Network::new(vec![None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);
    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    assert_eq!(tt.peers.get(&1).unwrap().raft_log.committed, 3);
}

#[test]
fn test_can_not_commit_without_new_term_entry() {
    let mut tt = Network::new(vec![None, None, None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    assert_eq!(tt.peers.get(&1).unwrap().raft_log.committed, 1);

    tt.recover();
    tt.ignore(MessageType::MsgApp);

    tt.send(vec![new_message(2, 2, MessageType::MsgHup)]);
    assert_eq!(tt.peers.get(&2).unwrap().raft_log.committed, 1);

    tt.recover();
    tt.send(vec![new_message(2, 2, MessageType::MsgBeat)]);
    tt.send(vec![new_message_with_entries(
        2,
        2,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    assert_eq!(tt.peers.get(&2).unwrap().raft_log.committed, 5);
}

#[test]
fn test_commit_without_new_term_entry() {
    let mut tt = Network::new(vec![None, None, None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    assert_eq!(tt.peers.get(&1).unwrap().raft_log.committed, 1);

    tt.recover();

    tt.send(vec![new_message(2, 2, MessageType::MsgHup)]);
    assert_eq!(tt.peers.get(&2).unwrap().raft_log.committed, 4);
}

#[test]
fn test_dueling_candidates() {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    nt.cut(1, 3);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Candidate);

    nt.recover();

    // candidate 3 now increases its term and tries to vote again
    // we expect it to disrupt the leader 1 since it has a higher term
    // 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    let mut s = MemStorage::new();
    let _ = s.append(&vec![new_entry(1, 1)]);

    let us = Unstable::new(2, String::default());
    let wlog = RaftLog {
        storage: s,
        committed: 1,
        unstable: us,
        ..Default::default()
    };

    let rl = RaftLog::new(MemStorage::new(), String::default());
    let tests = vec![
        (nt.peers.get(&1).unwrap(), StateType::Follower, 2, &wlog),
        (nt.peers.get(&2).unwrap(), StateType::Follower, 2, &wlog),
        (nt.peers.get(&3).unwrap(), StateType::Follower, 2, &rl),
    ];

    for (sm, state, term, rl) in tests {
        assert_eq!(sm.state, state);
        assert_eq!(sm.term, term);

        assert_eq!(ltoa(rl), ltoa(&sm.raft_log));
    }
}

#[test]
fn test_dueling_pre_candidates() {
    let mut cfg_a = new_test_config(1, vec![1, 2, 3], 10, 1);
    let mut cfg_b = new_test_config(1, vec![1, 2, 3], 10, 1);
    let mut cfg_c = new_test_config(1, vec![1, 2, 3], 10, 1);
    cfg_a.pre_vote = true;
    cfg_b.pre_vote = true;
    cfg_c.pre_vote = true;

    let a = Raft::new(&mut cfg_a, MemStorage::new());
    let b = Raft::new(&mut cfg_b, MemStorage::new());
    let c = Raft::new(&mut cfg_c, MemStorage::new());

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    nt.cut(1, 3);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Follower);

    nt.recover();

    // Candidate 3 now increases its term and tries to vote again.
    // With PreVote, it does not disrupt the leader.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    let mut s = MemStorage::new();
    let _ = s.append(&vec![new_entry(1, 1)]);

    let wlog = RaftLog {
        storage: s,
        committed: 1,
        unstable: Unstable::new(2, String::default()),
        ..Default::default()
    };

    let rl = RaftLog::new(MemStorage::new(), String::default());
    let tests = vec![
        (nt.peers.get(&1).unwrap(), StateType::Leader, 1, &wlog),
        (nt.peers.get(&2).unwrap(), StateType::Follower, 1, &wlog),
        (nt.peers.get(&3).unwrap(), StateType::Follower, 1, &rl),
    ];

    for (sm, state, term, rl) in tests {
        assert_eq!(sm.state, state);
        assert_eq!(sm.term, term);

        assert_eq!(ltoa(rl), ltoa(&sm.raft_log));
    }
}

#[test]
fn test_candidate_concede() {
    let mut tt = Network::new(vec![None, None, None]);
    tt.isolate(1);

    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    tt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    tt.recover();

    tt.send(vec![new_message(3, 3, MessageType::MsgBeat)]);

    let data: Vec<u8> = Vec::from("force follower");

    tt.send(vec![new_message_with_entries(
        3,
        3,
        MessageType::MsgProp,
        vec![new_entry_with_data(data.clone())],
    )]);

    tt.send(vec![new_message(3, 3, MessageType::MsgBeat)]);

    assert_eq!(tt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(tt.peers.get(&1).unwrap().term, 1);

    let mut s = MemStorage::new();
    let mut e2 = new_entry(1, 2);
    e2.set_data(data.clone());
    let _ = s.append(&vec![new_entry(1, 1), e2]);

    let wlog = RaftLog {
        storage: s,
        committed: 2,
        unstable: Unstable::new(3, String::default()),
        ..Default::default()
    };

    for (_, p) in tt.peers {
        assert_eq!(ltoa(&p.raft_log), ltoa(&wlog));
    }
}

#[test]
fn test_single_node_candidate() {
    let mut tt = Network::new(vec![None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(tt.peers.get(&1).unwrap().state, StateType::Leader);
}

#[test]
fn test_single_node_pre_candidate() {
    let mut tt = Network::new_with_config(vec![None], true);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(tt.peers.get(&1).unwrap().state, StateType::Leader);
}

#[test]
fn test_old_message() {
    let mut tt = Network::new(vec![None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    tt.send(vec![new_message(2, 2, MessageType::MsgHup)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    let mut m = new_message_with_entries(2, 1, MessageType::MsgApp, vec![new_entry(2, 3)]);
    m.set_term(2);
    tt.send(vec![m]);

    tt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);

    let mut s = MemStorage::new();
    let mut e = new_entry(3, 4);
    e.set_data(Vec::from("somedata"));
    let _ = s.append(&vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3), e]);

    let wlog = RaftLog {
        storage: s,
        committed: 4,
        unstable: Unstable::new(5, String::default()),
        ..Default::default()
    };

    for (_, p) in tt.peers {
        assert_eq!(ltoa(&p.raft_log), ltoa(&wlog));
    }
}

#[test]
fn test_proposal() {
    let tests = vec![
        (Network::new(vec![None, None, None]), true),
        (Network::new(vec![None, None, NOP_STEPPER]), true),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER]), false),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None]),
            false,
        ),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None, None]),
            true,
        ),
    ];

    let data: Vec<u8> = Vec::from("somedata");
    for (mut nt, sucsess) in tests {
        nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
        nt.send(vec![new_message_with_entries(
            1,
            1,
            MessageType::MsgProp,
            vec![new_entry_with_data(data.clone())],
        )]);

        let mut wlog = RaftLog::new(MemStorage::new(), String::default());
        if sucsess {
            let mut s = MemStorage::new();
            let mut e = new_entry(1, 2);
            e.set_data(data.clone());
            let _ = s.append(&vec![new_entry(1, 1), e]);

            wlog = RaftLog {
                storage: s,
                committed: 2,
                unstable: Unstable::new(3, String::default()),
                ..Default::default()
            };
        }

        for (_, p) in &nt.peers {
            if p.raft.is_some() {
                assert_eq!(ltoa(&p.raft_log), ltoa(&wlog));
            }
        }

        assert_eq!(nt.peers.get(&1).unwrap().term, 1);
    }
}

#[test]
fn test_proposal_by_proxy() {
    let data: Vec<u8> = Vec::from("somedata");
    let tests = vec![
        Network::new(vec![None, None, None]),
        Network::new(vec![None, None, NOP_STEPPER]),
    ];

    for mut tt in tests {
        tt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
        tt.send(vec![new_message_with_entries(
            2,
            2,
            MessageType::MsgProp,
            vec![new_entry_with_data(data.clone())],
        )]);

        let mut s = MemStorage::new();
        let mut e = new_entry(1, 2);
        e.set_data(data.clone());
        let _ = s.append(&vec![new_entry(1, 1), e]);

        let wlog = RaftLog {
            storage: s,
            committed: 2,
            unstable: Unstable::new(3, String::default()),
            ..Default::default()
        };

        for (_, p) in &tt.peers {
            if p.raft.is_some() {
                assert_eq!(ltoa(&p.raft_log), ltoa(&wlog));
            }
        }

        assert_eq!(tt.peers.get(&1).unwrap().term, 1);
    }
}

#[test]
fn test_commit() {
    let tests = vec![
        // single
        (vec![1], vec![new_entry(1, 1)], 1, 1),
        (vec![1], vec![new_entry(1, 1)], 2, 0),
        (vec![2], vec![new_entry(1, 1), new_entry(2, 2)], 2, 2),
        (vec![1], vec![new_entry(2, 1)], 2, 1),
        // old
        (vec![2, 1, 1], vec![new_entry(1, 1), new_entry(2, 2)], 1, 1),
        (vec![2, 1, 1], vec![new_entry(1, 1), new_entry(1, 2)], 2, 0),
        (vec![2, 1, 2], vec![new_entry(1, 1), new_entry(2, 2)], 2, 2),
        (vec![2, 1, 2], vec![new_entry(1, 1), new_entry(1, 2)], 2, 0),
        // even
        (
            vec![2, 1, 1, 1],
            vec![new_entry(1, 1), new_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1, 1],
            vec![new_entry(1, 1), new_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 1, 2],
            vec![new_entry(1, 1), new_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1, 2],
            vec![new_entry(1, 1), new_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 2, 2],
            vec![new_entry(1, 1), new_entry(2, 2)],
            2,
            2,
        ),
        (
            vec![2, 1, 2, 2],
            vec![new_entry(1, 1), new_entry(1, 2)],
            2,
            0,
        ),
    ];

    for (matches, logs, term, w) in tests {
        let mut storage = MemStorage::new();
        let _ = storage.append(&logs);
        let mut hs = HardState::new();
        hs.set_term(term);
        storage.set_hard_state(hs);

        let mut sm = new_test_raft(1, vec![1], 10, 2, storage);
        for j in 0..matches.len() {
            sm.set_progress(j as u64 + 1, matches[j], matches[j] + 1, false);
        }

        sm.maybe_commit();
        assert_eq!(sm.raft_log.committed, w);
    }
}

#[test]
fn test_past_election_timeout() {
    let tests = vec![
        (5, 0.0, false),
        (10, 0.1, true),
        (13, 0.4, true),
        (15, 0.6, true),
        (18, 0.9, true),
        (20, 1.0, false),
    ];

    for (elapse, wprobability, round) in tests {
        let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
        sm.election_elapsed = elapse;
        let mut c = 0;
        for _ in 0..10000 {
            sm.reset_randomized_election_timeout();
            if sm.past_election_timeout() {
                c += 1;
            }
        }

        let mut got = c as f64 / 10000.0;
        if round {
            got = (got * 10.0 + 0.5).floor() / 10.0;
        }
        assert_eq!(got, wprobability);
    }
}

#[test]
fn test_step_ignore_old_term_msg() {
    let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
    sm.before_step_state = Some(Box::new(|_: &Message| {
        panic!("before step state function hook called unexpectedly")
    }));
    sm.term = 2;
    let mut m = Message::new();
    m.set_msg_type(MessageType::MsgApp);
    m.set_term(1);
    sm.step(m).unwrap();
}

#[test]
fn test_handle_msg_app() {
    let tests = vec![
        (
            new_append_message_with_entries(2, 3, 2, 3, vec![]),
            2,
            0,
            true,
        ),
        (
            new_append_message_with_entries(2, 3, 3, 3, vec![]),
            2,
            0,
            true,
        ),
        (
            new_append_message_with_entries(2, 1, 1, 1, vec![]),
            2,
            1,
            false,
        ),
        (
            new_append_message_with_entries(2, 0, 0, 1, vec![new_entry(2, 1)]),
            1,
            1,
            false,
        ),
        (
            new_append_message_with_entries(2, 2, 2, 3, vec![new_entry(2, 3), new_entry(2, 4)]),
            4,
            3,
            false,
        ),
        (
            new_append_message_with_entries(2, 2, 2, 4, vec![new_entry(2, 3)]),
            3,
            3,
            false,
        ),
        (
            new_append_message_with_entries(2, 1, 1, 4, vec![new_entry(2, 2)]),
            2,
            2,
            false,
        ),
        (
            new_append_message_with_entries(2, 2, 2, 3, vec![]),
            2,
            2,
            false,
        ),
        (
            new_append_message_with_entries(2, 2, 2, 4, vec![]),
            2,
            2,
            false,
        ),
    ];

    for (m, windex, wcommit, wreject) in tests {
        let mut s = MemStorage::new();
        let _ = s.append(&vec![new_entry(1, 1), new_entry(2, 2)]);
        let mut sm = new_test_raft(1, vec![1], 10, 1, s);
        sm.become_follower(2, NONE);

        sm.handle_append_entries(&m);
        assert_eq!(sm.raft_log.last_index(), windex);
        assert_eq!(sm.raft_log.committed, wcommit);
        let m: Vec<Message> = sm.msgs.drain(..).collect();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].get_reject(), wreject);
    }
}

fn new_append_message_with_entries(
    term: u64,
    log_term: u64,
    index: u64,
    commit: u64,
    ents: Vec<Entry>,
) -> Message {
    let mut m = Message::new();
    m.set_msg_type(MessageType::MsgApp);
    m.set_term(term);
    m.set_log_term(log_term);
    m.set_index(index);
    m.set_commit(commit);
    if !ents.is_empty() {
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

#[test]
fn test_handle_heartbeat() {
    let commit: u64 = 2;
    let tests = vec![
        (new_heartbeat_message(2, 1, 2, commit + 1), commit + 1),
        (new_heartbeat_message(2, 1, 2, commit - 1), commit),
    ];

    for (mut m, wcommit) in tests {
        let mut s = MemStorage::new();
        let _ = s.append(&vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)]);
        let mut sm = new_test_raft(1, vec![1, 2], 5, 1, s);
        sm.become_follower(2, 2);
        sm.raft_log.commit_to(commit);
        sm.handle_heartbeat(m);
        assert_eq!(sm.raft_log.committed, wcommit);
        let m: Vec<Message> = sm.msgs.drain(..).collect();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].get_msg_type(), MessageType::MsgHeartbeatResp);
    }
}

fn new_heartbeat_message(from: u64, to: u64, term: u64, commit: u64) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_term(term);
    m.set_commit(commit);
    m.set_msg_type(MessageType::MsgHeartbeat);

    m
}

#[test]
fn test_handle_heartbeat_resp() {
    let mut s = MemStorage::new();
    let _ = s.append(&vec![new_entry(1, 1), new_entry(2, 2), new_entry(3, 3)]);
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, s);
    sm.become_candidate();
    sm.become_leader();
    let committed = sm.raft_log.last_index();
    sm.raft_log.commit_to(committed);

    let _ = sm.step(new_heartbeat_resp_message(2));
    let m: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].get_msg_type(), MessageType::MsgApp);

    let _ = sm.step(new_heartbeat_resp_message(2));
    let m: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].get_msg_type(), MessageType::MsgApp);

    let mut msg = Message::new();
    msg.set_from(2);
    msg.set_msg_type(MessageType::MsgAppResp);
    msg.set_index(m[0].get_index() + m[0].get_entries().len() as u64);
    let _ = sm.step(msg);
    sm.msgs.drain(..);

    let _ = sm.step(new_heartbeat_resp_message(2));
    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 0);
}

fn new_heartbeat_resp_message(from: u64) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_msg_type(MessageType::MsgHeartbeatResp);

    m
}

#[test]
fn test_raft_frees_read_only_mem() {
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    sm.become_candidate();
    sm.become_leader();
    let last_index = sm.raft_log.last_index();
    sm.raft_log.commit_to(last_index);

    let ctx: Vec<u8> = Vec::from("ctx");

    let _ = sm.step(new_message_with_entries(
        2,
        NONE,
        MessageType::MsgReadIndex,
        vec![new_entry_with_data(ctx.clone())],
    ));
    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgHeartbeat);
    assert_eq!(msgs[0].get_context().to_vec(), ctx);
    assert_eq!(sm.read_only.read_index_queue.len(), 1);
    assert_eq!(sm.read_only.pending_read_index.len(), 1);
    assert!(sm.read_only.pending_read_index.contains_key(&ctx));

    let mut m = new_message(2, NONE, MessageType::MsgHeartbeatResp);
    m.set_context(ctx.clone());
    let _ = sm.step(m);

    assert_eq!(sm.read_only.read_index_queue.len(), 0);
    assert_eq!(sm.read_only.pending_read_index.len(), 0);
    assert!(!sm.read_only.pending_read_index.contains_key(&ctx));
}

#[test]
fn test_msg_app_resp_wait_reset() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, MemStorage::new());
    sm.become_candidate();
    sm.become_leader();

    sm.bcast_append();
    sm.msgs.drain(..);

    let mut m = new_message(2, NONE, MessageType::MsgAppResp);
    m.set_index(1);
    // Node 2 acks the first entry, making it committed.
    let _ = sm.step(m);
    assert_eq!(sm.raft_log.committed, 1);

    // Also consume the MsgApp messages that update Commit on the followers.
    sm.msgs.drain(..);

    let m = new_message_with_entries(1, NONE, MessageType::MsgProp, vec![Entry::new()]);

    // A new command is now proposed on node 1.
    let _ = sm.step(m);

    // The command is broadcast to all nodes not in the wait state.
    // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 1);

    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgApp);
    assert_eq!(msgs[0].get_to(), 2);
    assert_eq!(msgs[0].get_entries().len(), 1);
    assert_eq!(msgs[0].get_entries()[0].get_index(), 2);

    let mut m = new_message(3, NONE, MessageType::MsgAppResp);
    m.set_index(1);
    let _ = sm.step(m);

    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 1);

    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgApp);
    assert_eq!(msgs[0].get_to(), 3);
    assert_eq!(msgs[0].get_entries().len(), 1);
    assert_eq!(msgs[0].get_entries()[0].get_index(), 2);
}

#[test]
fn test_recv_msg_vote() {
    recv_msg_vote(MessageType::MsgVote)
}

#[test]
fn test_msg_pre_vote() {
    recv_msg_vote(MessageType::MsgPreVote)
}

fn recv_msg_vote(msg_type: MessageType) {
    let tests = vec![
        (StateType::Follower, 0, 0, NONE, true),
        (StateType::Follower, 0, 1, NONE, true),
        (StateType::Follower, 0, 2, NONE, true),
        (StateType::Follower, 0, 3, NONE, false),
        (StateType::Follower, 1, 0, NONE, true),
        (StateType::Follower, 1, 1, NONE, true),
        (StateType::Follower, 1, 2, NONE, true),
        (StateType::Follower, 1, 3, NONE, false),
        (StateType::Follower, 2, 0, NONE, true),
        (StateType::Follower, 2, 1, NONE, true),
        (StateType::Follower, 2, 2, NONE, false),
        (StateType::Follower, 2, 3, NONE, false),
        (StateType::Follower, 3, 0, NONE, true),
        (StateType::Follower, 3, 1, NONE, true),
        (StateType::Follower, 3, 2, NONE, false),
        (StateType::Follower, 3, 3, NONE, false),
        (StateType::Follower, 3, 2, 2, false),
        (StateType::Follower, 3, 2, 1, true),
        (StateType::Leader, 3, 3, 1, true),
        (StateType::PreCandidate, 3, 3, 1, true),
        (StateType::Candidate, 3, 3, 1, true),
    ];

    for (state, index, log_term, vote_for, wreject) in tests {
        let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
        sm.state = state;
        sm.vote = vote_for;
        let mut s = MemStorage::new();
        let _ = s.append(&vec![new_entry(1, 1), new_entry(2, 2)]);
        sm.raft_log = RaftLog {
            storage: s,
            unstable: Unstable::new(3, String::default()),
            ..Default::default()
        };

        let term = cmp::max(sm.raft_log.last_term(), log_term);
        sm.term = term;
        let mut m = new_message(2, NONE, msg_type);
        m.set_term(term);
        m.set_log_term(log_term);
        m.set_index(index);
        let _ = sm.step(m);

        let msgs: Vec<Message> = sm.msgs.drain(..).collect();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].get_msg_type(), vote_msg_resp_type(msg_type));
        assert_eq!(msgs[0].get_reject(), wreject);
    }
}

#[test]
fn test_state_transition() {
    let tests = vec![
        (StateType::Follower, StateType::Follower, true, 1, NONE),
        (StateType::Follower, StateType::PreCandidate, true, 0, NONE),
        (StateType::Follower, StateType::Candidate, true, 1, NONE),
        (StateType::Follower, StateType::Leader, false, 0, NONE),
        (StateType::PreCandidate, StateType::Follower, true, 0, NONE),
        (
            StateType::PreCandidate,
            StateType::PreCandidate,
            true,
            0,
            NONE,
        ),
        (StateType::PreCandidate, StateType::Candidate, true, 1, NONE),
        (StateType::PreCandidate, StateType::Leader, true, 0, 1),
        (StateType::Candidate, StateType::Follower, true, 0, NONE),
        (StateType::Candidate, StateType::PreCandidate, true, 0, NONE),
        (StateType::Candidate, StateType::Candidate, true, 1, NONE),
        (StateType::Candidate, StateType::Leader, true, 0, 1),
        (StateType::Leader, StateType::Follower, true, 1, NONE),
        (StateType::Leader, StateType::PreCandidate, false, 0, NONE),
        (StateType::Leader, StateType::Candidate, false, 1, NONE),
        (StateType::Leader, StateType::Leader, true, 0, 1),
    ];

    for (from, to, wallow, wterm, wlead) in tests {
        use std::panic;
        let result = panic::catch_unwind(|| {
            let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
            sm.state = from;

            match to {
                StateType::Follower => sm.become_follower(wterm, wlead),
                StateType::PreCandidate => sm.become_pre_candidate(),
                StateType::Candidate => sm.become_candidate(),
                StateType::Leader => sm.become_leader(),
            }

            assert_eq!(sm.term, wterm);
            assert_eq!(sm.lead, wlead);
        });
        if wallow {
            assert!(result.is_ok());
        }
    }
}

#[test]
fn test_all_server_step_down() {
    let tests = vec![
        (StateType::Follower, StateType::Follower, 3, 0),
        (StateType::PreCandidate, StateType::Follower, 3, 0),
        (StateType::Candidate, StateType::Follower, 3, 0),
        (StateType::Leader, StateType::Follower, 3, 1),
    ];

    let t_msg_types = vec![MessageType::MsgVote, MessageType::MsgApp];
    let t_term = 3;

    for (state, wstate, wterm, windex) in tests {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
        match state {
            StateType::Follower => sm.become_follower(1, NONE),
            StateType::PreCandidate => sm.become_pre_candidate(),
            StateType::Candidate => sm.become_candidate(),
            StateType::Leader => {
                sm.become_candidate();
                sm.become_leader();
            }
        }

        for msg_type in &t_msg_types {
            let mut m = new_message(2, NONE, msg_type.clone());
            m.set_term(t_term);
            m.set_log_term(t_term);
            let _ = sm.step(m);

            assert_eq!(sm.state, wstate);
            assert_eq!(sm.term, wterm);
            assert_eq!(sm.raft_log.last_index(), windex);
            assert_eq!(sm.raft_log.all_entries().len() as u64, windex);
            let mut wlead = 2;
            if msg_type == &MessageType::MsgVote {
                wlead = NONE;
            }
            assert_eq!(sm.lead, wlead);
        }
    }
}

#[test]
fn test_candidate_reset_term_msg_app() {
    candidate_reset_term(MessageType::MsgApp);
}

#[test]
fn test_candidate_reset_term_msg_heartbeat() {
    candidate_reset_term(MessageType::MsgHeartbeat);
}

fn candidate_reset_term(msg_type: MessageType) {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);

    nt.isolate(3);

    nt.send(vec![new_message(2, 2, MessageType::MsgHup)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.peers
        .get_mut(&3)
        .unwrap()
        .reset_randomized_election_timeout();
    for _ in 0..nt.peers.get(&3).unwrap().randomized_election_timeout {
        nt.peers.get_mut(&3).unwrap().tick();
    }

    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Candidate);
    nt.recover();
    let mut m = new_message(1, 3, msg_type);
    m.set_term(nt.peers.get(&1).unwrap().term);
    nt.send(vec![m]);
    assert_eq!(
        nt.peers.get(&1).unwrap().term,
        nt.peers.get(&3).unwrap().term
    );
}

#[test]
fn test_leader_stepdown_when_quorum_active() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, MemStorage::new());
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();

    for _ in 0..sm.election_timeout + 1 {
        let mut m = new_message(2, NONE, MessageType::MsgHeartbeatResp);
        m.set_term(sm.term);
        let _ = sm.step(m);
        sm.tick();
    }

    assert_eq!(sm.state, StateType::Leader);
}

#[test]
fn test_leader_step_down_when_quorum_lost() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, MemStorage::new());
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();
    for _ in 0..sm.election_timeout + 1 {
        sm.tick();
    }

    assert_eq!(sm.state, StateType::Follower);
}

#[test]
fn test_leader_superseding_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);
    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 1;
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Follower);

    // peer b rejected c's vote since its election_elapsed had not reached to election_timeout
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Candidate);

    // Letting b's election_elapsed reach to election_timeout
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Leader);
}

#[test]
fn test_leader_election_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    let timeout = nt.peers.get(&1).unwrap().election_timeout;
    nt.peers.get_mut(&1).unwrap().randomized_election_timeout = timeout + 1;

    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 2;

    // Immediately after creation, votes are cast regardless of the
    // election timeout.
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Follower);

    // need to reset randomizedElectionTimeout larger than electionTimeout again,
    // because the value might be reset to electionTimeout since the last state changes
    let timeout = nt.peers.get(&1).unwrap().election_timeout;
    nt.peers.get_mut(&1).unwrap().randomized_election_timeout = timeout + 1;
    for _ in 0..timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 2;
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Leader);
}

#[test]
fn test_free_stuck_candidate_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 1;
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(1);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Candidate);
    assert_eq!(
        nt.peers.get(&3).unwrap().term,
        nt.peers.get(&2).unwrap().term + 1
    );

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Candidate);
    assert_eq!(
        nt.peers.get(&3).unwrap().term,
        nt.peers.get(&2).unwrap().term + 2
    );

    nt.recover();
    let mut m = new_message(1, 3, MessageType::MsgHeartbeat);
    m.set_term(nt.peers.get(&1).unwrap().term);
    nt.send(vec![m]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Follower);
    assert_eq!(
        nt.peers.get(&3).unwrap().term,
        nt.peers.get(&1).unwrap().term
    );

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Leader);
}

#[test]
fn test_non_promotable_voter_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    let mut b = new_test_raft(2, vec![1], 10, 1, MemStorage::new());
    a.check_quorum = true;
    b.check_quorum = true;

    let mut nt = Network::new(vec![Some(StateMachine::new(a)), Some(StateMachine::new(b))]);
    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 1;
    nt.peers.get_mut(&2).unwrap().del_progress(2);
    assert!(!nt.peers.get_mut(&2).unwrap().promotable());
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&2).unwrap().lead, 1);
}

#[test]
fn test_disruptive_follower() {
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    n1.check_quorum = true;
    n2.check_quorum = true;
    n3.check_quorum = true;

    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    n3.become_follower(1, NONE);

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
        Some(StateMachine::new(n3)),
    ]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Follower);
    let timeout = nt.peers.get(&3).unwrap().election_timeout;
    nt.peers.get_mut(&3).unwrap().randomized_election_timeout = timeout + 2;
    for _ in 0..nt.peers.get_mut(&3).unwrap().randomized_election_timeout - 1 {
        nt.peers.get_mut(&3).unwrap().tick();
    }
    nt.peers.get_mut(&3).unwrap().tick();

    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Candidate);

    assert_eq!(nt.peers.get_mut(&1).unwrap().term, 2);
    assert_eq!(nt.peers.get_mut(&2).unwrap().term, 2);
    assert_eq!(nt.peers.get_mut(&3).unwrap().term, 3);

    let mut m = new_message(1, 3, MessageType::MsgHeartbeat);
    m.set_term(nt.peers.get_mut(&1).unwrap().term);
    nt.send(vec![m]);

    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Candidate);

    assert_eq!(nt.peers.get_mut(&1).unwrap().term, 3);
    assert_eq!(nt.peers.get_mut(&2).unwrap().term, 2);
    assert_eq!(nt.peers.get_mut(&3).unwrap().term, 3);
}

#[test]
fn test_disruptive_follower_pre_vote() {
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    n1.check_quorum = true;
    n2.check_quorum = true;
    n3.check_quorum = true;

    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    n3.become_follower(1, NONE);

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
        Some(StateMachine::new(n3)),
    ]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::Follower);

    nt.isolate(3);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);;
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);;
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somedata"))],
    )]);;
    nt.peers.get_mut(&1).unwrap().pre_vote = true;
    nt.peers.get_mut(&2).unwrap().pre_vote = true;
    nt.peers.get_mut(&3).unwrap().pre_vote = true;
    nt.recover();
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get_mut(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get_mut(&3).unwrap().state, StateType::PreCandidate);

    assert_eq!(nt.peers.get_mut(&1).unwrap().term, 2);
    assert_eq!(nt.peers.get_mut(&2).unwrap().term, 2);
    assert_eq!(nt.peers.get_mut(&3).unwrap().term, 2);

    let mut m = new_message(1, 3, MessageType::MsgHeartbeat);
    m.set_term(nt.peers.get_mut(&1).unwrap().term);
    nt.send(vec![m]);

    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);
}

#[test]
fn test_read_only_option_safe() {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 1;
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);

    let tests = vec![
        (1, 10, 11, Vec::from("ctx1")),
        (2, 10, 21, Vec::from("ctx2")),
        (3, 10, 31, Vec::from("ctx3")),
        (1, 10, 41, Vec::from("ctx4")),
        (2, 10, 51, Vec::from("ctx5")),
        (3, 10, 61, Vec::from("ctx6")),
    ];

    for (sm, proposals, wri, wctx) in tests {
        for _ in 0..proposals {
            nt.send(vec![new_message_with_entries(
                1,
                1,
                MessageType::MsgProp,
                vec![Entry::new()],
            )]);
        }

        let id = nt.peers.get_mut(&sm).unwrap().id;
        let mut m = new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![new_entry_with_data(wctx.clone())],
        );
        nt.send(vec![m]);

        assert!(!nt.peers.get_mut(&sm).unwrap().read_states.is_empty());
        assert_eq!(nt.peers.get_mut(&sm).unwrap().read_states[0].index, wri);
        assert_eq!(
            nt.peers.get_mut(&sm).unwrap().read_states[0].request_ctx,
            wctx
        );

        nt.peers.get_mut(&sm).unwrap().read_states.clear();
    }
}

#[test]
fn test_read_only_option_leased() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(a)),
        Some(StateMachine::new(b)),
        Some(StateMachine::new(c)),
    ]);

    let timeout = nt.peers.get(&2).unwrap().election_timeout;
    nt.peers.get_mut(&2).unwrap().randomized_election_timeout = timeout + 1;
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().state, StateType::Leader);

    let tests = vec![
        (1, 10, 11, Vec::from("ctx1")),
        (2, 10, 21, Vec::from("ctx2")),
        (3, 10, 31, Vec::from("ctx3")),
        (1, 10, 41, Vec::from("ctx4")),
        (2, 10, 51, Vec::from("ctx5")),
        (3, 10, 61, Vec::from("ctx6")),
    ];

    for (sm, proposals, wri, wctx) in tests {
        for _ in 0..proposals {
            nt.send(vec![new_message_with_entries(
                1,
                1,
                MessageType::MsgProp,
                vec![Entry::new()],
            )]);
        }

        let id = nt.peers.get_mut(&sm).unwrap().id;
        let mut m = new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![new_entry_with_data(wctx.clone())],
        );
        nt.send(vec![m]);

        assert!(!nt.peers.get_mut(&sm).unwrap().read_states.is_empty());
        assert_eq!(nt.peers.get_mut(&sm).unwrap().read_states[0].index, wri);
        assert_eq!(
            nt.peers.get_mut(&sm).unwrap().read_states[0].request_ctx,
            wctx
        );

        nt.peers.get_mut(&sm).unwrap().read_states.clear();
    }
}

#[test]
fn test_read_only_for_new_leader() {
    let node_configs = vec![(1, 1, 1, 0), (2, 2, 2, 2), (3, 2, 2, 2)];
    let mut peers = vec![];

    for (id, committed, applied, compact_index) in node_configs {
        let mut storage = MemStorage::new();
        let _ = storage.append(&vec![new_entry(1, 1), new_entry(1, 2)]);

        let mut hs = HardState::new();
        hs.set_term(1);
        hs.set_commit(committed);
        storage.set_hard_state(hs);

        if compact_index != 0 {
            let _ = storage.compact(compact_index);
        }
        let mut cfg = new_test_config(id, vec![1, 2, 3], 10, 1);
        cfg.applied = applied;
        let r = Raft::new(&mut cfg, storage);
        peers.push(Some(StateMachine::new(r)));
    }
    let mut nt = Network::new(peers);

    // Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
    nt.ignore(MessageType::MsgApp);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);

    let windex = 4;
    let wctx = Vec::from("ctx");
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry_with_data(wctx.clone())],
    )]);
    assert!(nt.peers.get(&1).unwrap().read_states.is_empty());

    nt.recover();

    let timeout = nt.peers.get(&1).unwrap().election_timeout;
    for _ in 0..timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    assert_eq!(nt.peers.get(&1).unwrap().raft_log.committed, 4);
    let last_log_term = nt
        .peers
        .get(&1)
        .unwrap()
        .raft_log
        .zero_term_on_err_compacted(
            nt.peers
                .get(&1)
                .unwrap()
                .raft_log
                .term(nt.peers.get(&1).unwrap().raft_log.committed),
        );

    assert_eq!(last_log_term, nt.peers.get(&1).unwrap().term);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry_with_data(wctx.clone())],
    )]);

    assert_eq!(nt.peers.get(&1).unwrap().read_states[0].index, windex);
    assert_eq!(nt.peers.get(&1).unwrap().read_states[0].request_ctx, wctx);
}

// verfies that a normal peer can't become learner again
// when restores snapshot.
#[test]
fn test_restore_invalid_learner() {
    let s = new_snapshot(11, 11, vec![3], vec![1, 2]);

    let mut sm = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    assert!(!sm.is_learner);
    assert!(!sm.restore(s));
}

#[test]
fn test_restore_learner_promotion() {
    let s = new_snapshot(11, 11, vec![], vec![1, 2, 3]);
    let mut sm = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, MemStorage::new());

    assert!(sm.is_learner);
    assert!(sm.restore(s));
    assert!(!sm.is_learner);
}

#[test]
fn test_learner_receive_snapshot() {
    let s = new_snapshot(11, 11, vec![1], vec![2]);

    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, MemStorage::new());
    let n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, MemStorage::new());

    n1.restore(s);

    let committed = n1.raft_log.committed;
    n1.raft_log.applied_to(committed);

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
    ]);

    let timeout = nt.peers.get(&1).unwrap().election_timeout;
    nt.peers.get_mut(&1).unwrap().randomized_election_timeout = timeout;
    for _ in 0..timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgBeat)]);

    assert_eq!(
        nt.peers.get(&1).unwrap().raft_log.committed,
        nt.peers.get(&2).unwrap().raft_log.committed,
    );
}

#[test]
fn test_restore_ignore_snapshot() {
    let previous_ents = vec![new_entry(1, 1), new_entry(1, 2), new_entry(1, 3)];
    let commit: u64 = 1;
    let storage = MemStorage::new();
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, storage);
    sm.raft_log.append(&previous_ents);
    sm.raft_log.commit_to(commit);

    let mut s = new_snapshot(commit, 1, vec![], vec![1, 2]);
    assert!(!sm.restore(s.clone()));
    assert_eq!(sm.raft_log.committed, commit);

    s.mut_metadata().set_index(commit + 1);
    assert!(!sm.restore(s));
    assert_eq!(sm.raft_log.committed, commit + 1);
}

#[test]
fn test_provide_snap() {
    let s = new_snapshot(11, 11, vec![], vec![1, 2]);
    let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
    sm.restore(s);

    sm.become_candidate();
    sm.become_leader();
    let first_index = sm.raft_log.first_index();
    sm.prs.get_mut(&2).unwrap().next = first_index;
    let mut m = new_message(2, 1, MessageType::MsgAppResp);
    m.set_index(first_index - 1);
    m.set_reject(true);
    let _ = sm.step(m);

    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgSnap);
}

#[test]
fn test_ignore_providing_snap() {
    let s = new_snapshot(11, 11, vec![], vec![1, 2]);
    let mut sm = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
    sm.restore(s);
    sm.become_candidate();
    sm.become_leader();

    let first_index = sm.raft_log.first_index();
    sm.prs.get_mut(&2).unwrap().next = first_index - 1;
    sm.prs.get_mut(&2).unwrap().recent_active = false;

    let _ = sm.step(new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![new_entry_with_data(Vec::from("somdata"))],
    ));

    let msgs: Vec<Message> = sm.msgs.drain(..).collect();
    assert_eq!(msgs.len(), 0);
}

#[test]
fn test_restore_from_snap_msg() {
    let s = new_snapshot(11, 11, vec![], vec![1, 2]);
    let mut m = new_message(1, NONE, MessageType::MsgSnap);
    m.set_snapshot(s);
    let mut sm = new_test_raft(2, vec![1, 2], 10, 1, MemStorage::new());
    let _ = sm.step(m);

    assert_eq!(sm.lead, 1);
}

#[test]
fn test_slow_node_restore() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    nt.isolate(3);

    for _ in 0..101 {
        nt.send(vec![new_message_with_entries(
            1,
            1,
            MessageType::MsgProp,
            vec![Entry::new()],
        )]);
    }

    next_ents(
        nt.peers.get_mut(&1).unwrap(),
        nt.storage.get_mut(&1).unwrap(),
    );

    let mut cs = ConfState::new();
    cs.set_nodes(nt.peers.get(&1).unwrap().nodes());

    let _ = nt
        .storage
        .get_mut(&1)
        .unwrap()
        .write_lock()
        .create_snapshot(nt.peers.get(&1).unwrap().raft_log.applied, Some(cs), vec![]);

    let _ = nt
        .storage
        .get_mut(&1)
        .unwrap()
        .compact(nt.peers.get(&1).unwrap().raft_log.applied);

    nt.recover();

    loop {
        nt.send(vec![new_message_with_entries(
            1,
            1,
            MessageType::MsgProp,
            vec![Entry::new()],
        )]);

        if nt.peers.get(&1).unwrap().prs.get(&3).unwrap().recent_active {
            break;
        }
    }

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    assert_eq!(
        nt.peers.get(&3).unwrap().raft_log.committed,
        nt.peers.get(&1).unwrap().raft_log.committed,
    );
}

#[test]
fn test_step_config() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();
    let index = r.raft_log.last_index();
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    let m = new_message_with_entries(1, 1, MessageType::MsgProp, vec![e]);
    let _ = r.step(m);
    assert_eq!(r.raft_log.last_index(), index + 1);
    assert_eq!(r.pending_conf_index, index + 1);
}

#[test]
fn test_step_ignore_config() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();

    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    let m = new_message_with_entries(1, 1, MessageType::MsgProp, vec![e]);
    let _ = r.step(m.clone());

    let index = r.raft_log.last_index();
    let pending_conf_index = r.pending_conf_index;
    let _ = r.step(m);

    let mut e = new_entry(1, 3);
    e.set_entry_type(EntryType::EntryNormal);
    let wents = vec![e];
    let ents = r.raft_log.entries(index + 1, NO_LIMIT).unwrap();

    assert_eq!(pending_conf_index, r.pending_conf_index);
    assert_eq!(wents, ents);
}

#[test]
fn test_new_leader_pending_conf() {
    let tests = vec![(false, 0), (true, 1)];

    for (add_entry, wpending_conf_index) in tests {
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
        if add_entry {
            let mut e = Entry::new();
            e.set_entry_type(EntryType::EntryNormal);
            r.append_entry(&mut vec![e]);
        }
        r.become_candidate();
        r.become_leader();
        assert_eq!(wpending_conf_index, r.pending_conf_index);
    }
}

#[test]
fn test_add_node() {
    let mut r = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
    r.add_node(2);
    let nodes = r.nodes();
    let wnode = vec![1, 2];
    assert_eq!(nodes, wnode);
}

#[test]
fn test_add_learner() {
    let mut r = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
    r.add_learner(2);
    let nodes = r.learner_nodes();
    let wnodes = vec![2];
    assert_eq!(wnodes, nodes);
    assert!(r.learner_prs.get(&2).unwrap().is_learner);
}

#[test]
fn test_add_node_check_quorum() {
    let mut r = new_test_raft(1, vec![1], 10, 1, MemStorage::new());
    r.check_quorum = true;

    r.become_candidate();
    r.become_leader();

    for _ in 0..r.election_timeout - 1 {
        r.tick();
    }

    r.add_node(2);

    // This tick will reach electionTimeout, which triggers a quorum check.
    r.tick();

    // Node 1 should still be the leader after a single tick.
    assert_eq!(r.state, StateType::Leader);
    for _ in 0..r.election_timeout {
        r.tick();
    }

    assert_eq!(r.state, StateType::Follower);
}

#[test]
fn test_remove_node() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, MemStorage::new());
    r.remove_node(2);

    let w = vec![1];
    assert_eq!(w, r.nodes());
}

#[test]
fn test_remove_learner() {
    let mut r = new_test_learner_raft(1, vec![1], vec![2], 10, 1, MemStorage::new());
    r.remove_node(2);
    assert_eq!(r.nodes(), vec![1]);
    assert!(r.learner_nodes().is_empty());

    r.remove_node(1);
    assert!(r.nodes().is_empty());
}

#[test]
fn test_promotable() {
    let id = 1;
    let tests = vec![
        (vec![1], true),
        (vec![1, 2, 3], true),
        (vec![], false),
        (vec![2, 3], false),
    ];

    for (peers, wp) in tests {
        let mut r = new_test_raft(id, peers, 5, 1, MemStorage::new());
        assert_eq!(r.promotable(), wp);
    }
}

#[test]
fn test_raft_nodes() {
    let tests = vec![
        (vec![1, 2, 3], vec![1, 2, 3]),
        (vec![3, 2, 1], vec![1, 2, 3]),
    ];

    for (ids, wids) in tests {
        let mut r = new_test_raft(1, ids, 10, 1, MemStorage::new());
        assert_eq!(r.nodes(), wids);
    }
}

#[test]
fn test_campaign_while_leader() {
    campaign_while_leader(false)
}

#[test]
fn test_pre_campaign_while_leader() {
    campaign_while_leader(true)
}

fn campaign_while_leader(pre_vote: bool) {
    let mut cfg = new_test_config(1, vec![1], 5, 1);
    cfg.pre_vote = pre_vote;

    let mut r = Raft::new(&mut cfg, MemStorage::new());
    assert_eq!(r.state, StateType::Follower);

    let _ = r.step(new_message(1, 1, MessageType::MsgHup));
    assert_eq!(r.state, StateType::Leader);

    let term = r.term;
    let _ = r.step(new_message(1, 1, MessageType::MsgHup));
    assert_eq!(r.state, StateType::Leader);
    assert_eq!(term, r.term);
}

#[test]
fn test_commit_after_remove_node() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, MemStorage::new());
    r.become_candidate();
    r.become_leader();

    let mut cc = ConfChange::new();
    cc.set_change_type(ConfChangeType::ConfChangeRemoveNode);
    cc.set_node_id(2);
    let data = protobuf::Message::write_to_bytes(&cc).expect("unexpected marshal error");
    let mut e = new_entry_with_data(data);
    e.set_entry_type(EntryType::EntryConfChange);
    let m = new_message_with_entries(NONE, NONE, MessageType::MsgProp, vec![e]);
    let _ = r.step(m);

    // Stabilize the log and make sure nothing is committed yet.
    assert_eq!(stable_ents(&mut r).len(), 0);

    let cc_index = r.raft_log.last_index();

    let mut e = new_entry_with_data(Vec::from("data"));
    e.set_entry_type(EntryType::EntryNormal);

    // While the config change is pending, make another proposal.
    let _ = r.step(new_message_with_entries(
        NONE,
        NONE,
        MessageType::MsgProp,
        vec![e],
    ));

    // Node 2 acknowledges the config change, committing it.
    let mut m = new_message(2, NONE, MessageType::MsgAppResp);
    m.set_index(cc_index);
    let _ = r.step(m);

    let ents = stable_ents(&mut r);
    assert_eq!(ents.len(), 2);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert!(ents[0].get_data().is_empty());
    assert_eq!(ents[1].get_entry_type(), EntryType::EntryConfChange);

    r.remove_node(2);
    let ents = stable_ents(&mut r);
    assert_eq!(ents.len(), 1);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert_eq!(ents[0].get_data().to_vec(), Vec::from("data"));
}

// TestLeaderTransferToUpToDateNode verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
#[test]
fn test_leader_transfer_to_up_to_date_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);

    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 2);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_to_up_to_date_node_from_follower() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);

    nt.send(vec![new_message(2, 2, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 2);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_with_check_quorum() {
    let mut nt = Network::new(vec![None, None, None]);
    for i in 1..4 {
        nt.peers.get_mut(&i).unwrap().check_quorum = true;
        nt.peers.get_mut(&i).unwrap().randomized_election_timeout =
            nt.peers.get_mut(&i).unwrap().election_timeout + 1;
    }

    let timeout = nt.peers.get_mut(&2).unwrap().election_timeout;
    for _ in 0..timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get_mut(&1).unwrap().lead, 1);

    nt.send(vec![new_message(2, 2, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 2);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_to_slow_follwer() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    nt.recover();

    assert_eq!(nt.peers.get(&1).unwrap().prs.get(&3).unwrap().matched, 1);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 3);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_after_snapshot() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    next_ents(
        nt.peers.get_mut(&1).unwrap(),
        nt.storage.get_mut(&1).unwrap(),
    );

    let applied = nt.peers.get(&1).unwrap().raft_log.applied;

    let mut cs = ConfState::new();
    cs.set_nodes(nt.peers.get(&1).unwrap().nodes());
    let _ = nt
        .storage
        .get(&1)
        .unwrap()
        .write_lock()
        .create_snapshot(applied, Some(cs), vec![]);

    let _ = nt.storage.get(&1).unwrap().write_lock().compact(applied);

    nt.recover();

    assert_eq!(nt.peers.get(&1).unwrap().prs.get(&3).unwrap().matched, 1);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    nt.send(vec![new_message(3, 1, MessageType::MsgHeartbeatResp)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 3);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_to_self() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_to_non_existing_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.send(vec![new_message(4, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_timeout() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    let heartbeat_timeout = nt.peers.get(&1).unwrap().heartbeat_timeout;
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    let election_timeout = nt.peers.get(&1).unwrap().election_timeout;
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_ignore_proposal() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    )]);

    let err = nt.peers.get_mut(&1).unwrap().step(new_message_with_entries(
        1,
        1,
        MessageType::MsgProp,
        vec![Entry::new()],
    ));

    assert_eq!(err, Err(Error::ProposalDropped));
    assert_eq!(nt.peers.get(&1).unwrap().prs.get(&1).unwrap().matched, 1);
}

#[test]
fn test_leader_transfer_receive_higher_term() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    let mut m = new_message(2, 2, MessageType::MsgHup);
    m.set_index(1);
    m.set_term(2);
    nt.send(vec![m]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 2);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_remove_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    nt.ignore(MessageType::MsgTimeoutNow);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);
    nt.peers.get_mut(&1).unwrap().remove_node(3);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_back() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_second_transfer_to_another_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 2);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_leader_transfer_second_transfer_to_same_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    nt.isolate(3);
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, 3);

    let heartbeat_timeout = nt.peers.get(&1).unwrap().heartbeat_timeout;

    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader)]);
    let election_timeout = nt.peers.get(&1).unwrap().election_timeout;

    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&1).unwrap().lead, 1);
    assert_eq!(nt.peers.get(&1).unwrap().lead_transferee, NONE);
}

#[test]
fn test_transfer_non_member() {
    let mut r = new_test_raft(1, vec![2, 3, 4], 5, 1, MemStorage::new());
    let _ = r.step(new_message(2, 1, MessageType::MsgTimeoutNow));
    let _ = r.step(new_message(2, 1, MessageType::MsgVoteResp));
    let _ = r.step(new_message(3, 1, MessageType::MsgVoteResp));
    assert_eq!(r.state, StateType::Follower);
}

// tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
#[test]
fn test_node_with_smaller_term_can_complete_election() {
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());
    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    n3.become_follower(1, NONE);

    n1.pre_vote = true;
    n2.pre_vote = true;
    n3.pre_vote = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
        Some(StateMachine::new(n3)),
    ]);

    nt.cut(1, 3);
    nt.cut(2, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::PreCandidate);

    nt.send(vec![new_message(2, 2, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().term, 3);
    assert_eq!(nt.peers.get(&2).unwrap().term, 3);
    assert_eq!(nt.peers.get(&3).unwrap().term, 1);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::PreCandidate);

    nt.recover();
    nt.cut(2, 1);
    nt.cut(2, 3);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
}

#[test]
fn test_pre_vote_with_split_vote() {
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, MemStorage::new());
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, MemStorage::new());

    n1.become_follower(1, NONE);
    n2.become_follower(1, NONE);
    n3.become_follower(1, NONE);

    n1.pre_vote = true;
    n2.pre_vote = true;
    n3.pre_vote = true;

    let mut nt = Network::new(vec![
        Some(StateMachine::new(n1)),
        Some(StateMachine::new(n2)),
        Some(StateMachine::new(n3)),
    ]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup)]);
    nt.isolate(1);

    nt.send(vec![
        new_message(2, 2, MessageType::MsgHup),
        new_message(3, 3, MessageType::MsgHup),
    ]);

    assert_eq!(nt.peers.get(&2).unwrap().term, 3);
    assert_eq!(nt.peers.get(&3).unwrap().term, 3);

    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Candidate);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Candidate);
}

#[test]
fn test_pre_vote_migration_can_complete_election() {
    let mut nt = new_pre_vote_migration_cluster();

    nt.isolate(1);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::PreCandidate);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::Follower);
}

#[test]
fn test_pre_vote_migration_with_free_stuck_pre_candidate() {
    let mut nt = new_pre_vote_migration_cluster();
    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::PreCandidate);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup)]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Leader);
    assert_eq!(nt.peers.get(&2).unwrap().state, StateType::Follower);
    assert_eq!(nt.peers.get(&3).unwrap().state, StateType::PreCandidate);

    let mut m = new_message(1, 3, MessageType::MsgHeartbeat);
    m.set_term(nt.peers.get(&1).unwrap().term);
    nt.send(vec![m]);

    assert_eq!(nt.peers.get(&1).unwrap().state, StateType::Follower);
    assert_eq!(
        nt.peers.get(&1).unwrap().term,
        nt.peers.get(&3).unwrap().term
    );
}
