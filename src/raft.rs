use std::collections::HashMap;

use storage::Storage;
use raft_log::RaftLog;
use progress::Progress;
use errors::Result;
use raftpb::{Message, HardState};
use read_only::{ReadOnlyOption, ReadOnly};

use rand::{self, Rng};

// A constant represents invalid id of raft.
pub const NONE: u64 = 0;

pub struct Config {
    /// id is the identity of the local raft. ID cannot be 0.
    pub id: u64,

    /// peers contains the IDs of all nodes (including self) in
    /// the raft cluster. It should only be set when starting a new
    /// raft cluster.
    /// Restarting raft from previous configuration will panic if
    /// peers is set.
    /// peer is private and only used for testing right now.
    pub peers: Vec<u64>,

    /// learners contains the IDs of all learner nodes (including self if the
	/// local node is a learner) in the raft cluster. learners only receives
	/// entries from the leader node. It does not vote or promote itself.
	pub learners: Vec<u64>,

    // election_tick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before election_tick has elapsed, it will become
	// candidate and start an election. election_tick must be greater than
	// HeartbeatTick. We suggest election_tick = 10 * heartbeat_tick to avoid
	// unnecessary leader switching.
	pub election_tick: u64,

	/// heartbeat_tick is the number of Node.Tick invocations that must pass between
	/// heartbeats. That is, a leader sends heartbeat messages to maintain its
	/// leadership every heartbeat_tick ticks.
	pub heartbeat_tick: u64,

    /// applied is the last applied index. It should only be set when restarting
	/// raft. raft will not return entries to the application smaller or equal to
	/// Applied. If Applied is unset when restarting, raft might return previous
	/// applied entries. This is a very application dependent configuration.
	pub applied: u64,

    /// max_size_per_msg limits the max size of each append message. Smaller value
	/// lowers the raft recovery cost(initial probing and message lost during normal
	/// operation). On the other side, it might affect the throughput during normal
	/// replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
	/// message.
	pub max_size_per_msg: u64,

    /// max_inflight_msgs limits the max number of in-flight append messages during
	/// optimistic replication phase. The application transportation layer usually
	/// has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
	/// overflowing that sending buffer. TODO (xiangli): feedback to application to
	/// limit the proposal rate?
	pub max_inflight_msgs: u64,

    /// check_quorum specifies if the leader should check quorum activity. Leader
	/// steps down when quorum is not active for an electionTimeout.
	pub check_quorum: bool,

    /// pre_vote enables the Pre-Vote algorithm described in raft thesis section
	/// 9.6. This prevents disruption when a node that has been partitioned away
	/// rejoins the cluster.
	pub pre_vote: bool,

    /// ReadOnlyOption specifies how the read only request is processed.
	///
	/// ReadOnlyOption::Safe guarantees the linearizability of the read only request by
	/// communicating with the quorum. It is the default and suggested option.
	///
	/// ReadOnlyOption::LeaseBased ensures linearizability of the read only request by
	/// relying on the leader lease. It can be affected by clock drift.
	/// If the clock drift is unbounded, leader might keep the lease longer than it
	/// should (clock can move backward/pause without any bound). ReadIndex is not safe
	/// in that case.
	/// CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyOption::LeaseBased.
	read_only_option: ReadOnlyOption,

    /// disable_proposal_forwarding set to true means that followers will drop
	/// proposals, rather than forwarding them to the leader. One use case for
	/// this feature would be in a situation where the Raft leader is used to
	/// compute the data of a proposal, for example, adding a timestamp from a
	/// hybrid logical clock to data in a monotonically increasing way. Forwarding
	/// should be disabled to prevent a follower with an innaccurate hybrid
	/// logical clock from assigning the timestamp and then forwarding the data
	/// to the leader.
	disable_proposal_forwarding: bool,

	/// tag used for logger.
	tag: String,
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.id == NONE {
            return Err(format_err!("invalid node id"));
        }

        if self.heartbeat_tick == 0 {
            return Err(format_err!("heartbeat tick must greater than 0"))
        }

        if self.max_size_per_msg <= 0 {
            return Err(format_err!("max inflight messages must be greater than 0"))
        }

        if self.read_only_option == ReadOnlyOption::LeaseBased && !self.check_quorum {
		    return Err(format_err!("check_quorum must be enabled when ReadOnlyOption is ReadOnlyOption::LeaseBased"))
	    }

        Ok(())
    }
}

pub enum StateType {
	Follower,
	Candidate,
	Leader,
	PreCandidate,
}

impl Default for StateType {
	fn default() -> StateType {
		StateType::Follower
	}
}

#[derive(Default)]
pub struct Raft<T: Storage> {
    pub id: u64,
    pub term: u64,
	pub vote: u64,
    pub raft_log: RaftLog<T>,
	pub max_inflight: u64,
	pub max_msg_size: u64,
	prs: HashMap<u64, Progress>,
	learner_prs: HashMap<u64, Progress>,
	pub state: StateType,
	pub is_learner: bool,
	pub votes: HashMap<u64, bool>,
	pub msgs: Vec<Message>,

	// the leader id
	pub lead: u64,

	// lead_transferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	pub lead_transferee: u64,

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pending_conf_index, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pub pending_conf_index: u64,

	pub read_only: ReadOnly,

	// number of ticks since it reached last electionTimeout when it is leader
	// or candidate.
	// number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	pub election_elapsed: u64,

	// number of ticks since it reached last heartbeat_timeout.
	// only leader keeps heartbeat_elapsed.
	pub heartbeat_elapsed: u64,

	pub check_quorum: bool,
	pub pre_vote:     bool,

	pub heartbeat_timeout: u64,
	pub election_timeout: u64,

	// randomized_election_timeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	pub randomized_election_timeout: u64,
	pub disable_proposal_forwarding: bool,

	/// tag only used for logger.
	tag: String,
}

impl<T: Storage> Raft<T> {
	/// Storage is the storage for raft. raft generates entries and states to be
	/// stored in storage. raft reads the persisted entries and states out of
	/// Storage when it needs. raft reads out the previous state and configuration
	/// out of storage when restarting.
    pub fn new(c: &Config, storage: T) -> Raft<T> {
		c.validate().expect("configuration is invalid");
		let (hard_state, conf_state) = storage.initial_state().unwrap();
		let raft_log = RaftLog::new(storage, c.tag.clone());

		let mut peers: &[u64] = &c.peers;
		let mut learners: &[u64] = &c.learners;

		if !conf_state.get_nodes().is_empty() || !conf_state.get_learners().is_empty() {
			if !peers.is_empty() || !learners.is_empty() {
				panic!("cannot specify both new(peers, learners) and ConfState.(Nodes, Learners)");
			}

			peers = &conf_state.get_nodes();
			learners = &conf_state.get_learners();
		}

		let mut r = Raft{
			id: c.id,
			term: Default::default(),
			vote: Default::default(),
			raft_log: raft_log,
			max_msg_size: c.max_size_per_msg,
			max_inflight: c.max_inflight_msgs,
			prs: HashMap::new(),
			learner_prs: HashMap::new(),
			state: Default::default(),
			is_learner: false,
			votes: HashMap::new(),
			msgs: Default::default(),
			lead: NONE,
			lead_transferee: Default::default(),
			pending_conf_index: Default::default(),
			read_only: ReadOnly::new(c.read_only_option.clone()),
			election_elapsed: Default::default(),
			heartbeat_elapsed: Default::default(),
			check_quorum: c.check_quorum,
			pre_vote: c.pre_vote,
			heartbeat_timeout: c.heartbeat_tick,
			election_timeout: c.election_tick,
			randomized_election_timeout: Default::default(),
			tag: c.tag.clone(),
			disable_proposal_forwarding: c.disable_proposal_forwarding,
		};

		for &p in peers {
			r.prs.insert(p, Progress::new(1, r.max_inflight as usize, false));
		}
		for &p in learners {
			if r.prs.contains_key(&p) {
				panic!("node {} in both learner and peer list", p);
			}
			r.learner_prs.insert(p, Progress::new(1, r.max_inflight as usize, true));
			if r.id == p {
				r.is_learner = true;
			}
		}
		if hard_state != HardState::new() {
			r.load_state(hard_state);
		}

		if c.applied > 0 {
			r.raft_log.applied_to(c.applied);
		}
		let term = r.term;
		r.become_follower(term, NONE);

        unimplemented!()
    }

	pub fn load_state(&mut self, state: HardState) {
		if state.commit < self.raft_log.committed || state.commit > self.raft_log.last_index() {
			panic!(
				"{} state.commit {} is out of range [{}, {}]", 
				self.id, 
				state.commit, 
				self.raft_log.committed,
				self.raft_log.last_index(),
			);
		}
		self.raft_log.committed = state.commit;
		self.term = state.term;
		self.vote = state.vote;
	}

	pub fn become_follower(&mut self, term: u64, lead: u64) {
		self.reset(term);
		self.state = StateType::Follower;
		self.lead = lead;
		info!("{} became follower at term {}", self.tag, self.term);
	}

	pub fn reset(&mut self, term: u64) {
		if self.term != term {
			self.term = term;
			self.vote = NONE;
		}
		self.lead = NONE;
		self.election_elapsed = 0;
		self.heartbeat_elapsed = 0;
		self.reset_randomized_election_timeout();
		self.abort_leader_transfer();
		self.votes = HashMap::new();

		let (last_index, max_inflight) = (self.raft_log.last_index(), self.max_inflight);
		let self_id = self.id;

		for (&id, pr) in self.prs.iter_mut() {
			*pr = Progress::new(last_index + 1, max_inflight as usize, false);
			if id == self_id {
				pr.matched = self.raft_log.last_index();
			}
		}
		for (&id, pr) in self.learner_prs.iter_mut() {
			*pr = Progress::new(last_index + 1, max_inflight as usize, true);
			if id == self_id {
				pr.matched = self.raft_log.last_index();
			}
		}

		self.read_only = ReadOnly::new(self.read_only.option);
		self.pending_conf_index = 0;
	} 

	fn reset_randomized_election_timeout(&mut self) {
		let prev_timeout = self.randomized_election_timeout;
		let timeout = self.election_timeout + rand::thread_rng().gen_range(0, self.election_timeout);
		debug!(
            "{} reset election timeout {} -> {} at {}",
            self.tag, prev_timeout, timeout, self.election_elapsed
        );

		self.randomized_election_timeout = timeout;
	}

	pub fn abort_leader_transfer(&mut self) {
		self.lead_transferee = NONE;
	}
}