use storage::Storage;
use raft_log::raftLog;
use errors::Result;

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

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	pub heartbeat_tick: usize,
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.id == NONE {
            return Err(format_err!("invalid node id"));
        }

        if self.heartbeat_tick == 0 {
            return Err(format_err!("heartbeat tick must greater than 0"))
        }
        unimplemented!()
    }
}

#[derive(Default)]
pub struct Raft<T: Storage> {
    pub id: u64,
    pub term: u64,
    pub log: raftLog<T>,
}

impl<T: Storage> Raft<T> {
    fn new(c: &Config) -> Raft<T> {
        c.validate().expect("");
        unimplemented!()
    }
}