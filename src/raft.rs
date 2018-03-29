use std::collections::HashMap;

use storage::Storage;
use raft_log::RaftLog;
use progress::Progress;
use errors::Result;
use raftpb::{Message};

#[derive(Debug, PartialEq)]
enum ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    Safe, 
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

impl Default for ReadOnlyOption {
    fn default() -> ReadOnlyOption {
        ReadOnlyOption::Safe
    }
}

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

    // ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	pub election_tick: usize,

	/// HeartbeatTick is the number of Node.Tick invocations that must pass between
	/// heartbeats. That is, a leader sends heartbeat messages to maintain its
	/// leadership every HeartbeatTick ticks.
	pub heartbeat_tick: usize,

    /// Applied is the last applied index. It should only be set when restarting
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
    pub raft_log: RaftLog<T>,
	pub max_inflight: usize,
	pub max_msg_size: usize,
	prs: HashMap<u64, Progress>,
	learner_prs: HashMap<u64, Progress>,
	pub state: StateType,
	pub is_learner: bool,
	pub votes: HashMap<u64, bool>,
	pub msgs: Vec<Message>,
	pub lead: u64,

	/// tag only used for logger.
	tag: String,
}

impl<T: Storage> Raft<T> {
	/// Storage is the storage for raft. raft generates entries and states to be
	/// stored in storage. raft reads the persisted entries and states out of
	/// Storage when it needs. raft reads out the previous state and configuration
	/// out of storage when restarting.
    fn new(c: &Config, storage: T) -> Raft<T> {
		c.validate().expect("configuration is invalid");
		let (ref hard_state, ref conf_state) = storage.initial_state().unwrap();
		let raftLog = RaftLog::new(storage, c.tag.clone());

		let mut peers: &[u64] = &c.peers;
		let mut learners: &[u64] = &c.learners;

		if !conf_state.get_nodes().is_empty() || !conf_state.get_learners().is_empty() {
			if !peers.is_empty() || !learners.is_empty() {
				panic!("cannot specify both new(peers, learners) and ConfState.(Nodes, Learners)");
			}

			peers = &conf_state.get_nodes();
			learners = &conf_state.get_learners();
		}

        unimplemented!()
    }
}