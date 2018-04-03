use std::collections::HashMap;

use storage::Storage;
use raft_log::RaftLog;
use progress::Progress;
use errors::Result;
use raftpb::{Message, HardState, MessageType};
use read_only::{ReadOnlyOption, ReadOnly};
use raw_node::SoftState;

use rand::{self, Rng};

// A constant represents invalid id of raft.
pub const NONE: u64 = 0;

// CAMPAIGN_PRE_ELECTION represents the first phase of a normal election when
// Config.pre_vote is true.
const CAMPAIGN_PRE_ELECTION: &[u8] = b"CampaignPreElection";
// CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase
// of the election when Config.pre_vote is true).
const CAMPAIGN_ELECTION: &[u8] = b"CampaignElection";
// CAMPAIGN_TRANSFER represents the type of leader transfer.
const CAMPAIGN_TRANSFER: &[u8] = b"CampaignTransfer";

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
	pub read_only_option: ReadOnlyOption,

    /// disable_proposal_forwarding set to true means that followers will drop
	/// proposals, rather than forwarding them to the leader. One use case for
	/// this feature would be in a situation where the Raft leader is used to
	/// compute the data of a proposal, for example, adding a timestamp from a
	/// hybrid logical clock to data in a monotonically increasing way. Forwarding
	/// should be disabled to prevent a follower with an innaccurate hybrid
	/// logical clock from assigning the timestamp and then forwarding the data
	/// to the leader.
	pub disable_proposal_forwarding: bool,

	/// tag used for logger.
	pub tag: String,
}

impl Config {
    fn validate(&mut self) -> Result<()> {
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
		if self.tag.is_empty() {
			self.tag = "raft_log: ".to_string();
		}

        Ok(())
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
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

#[derive(Debug, Default)]
pub struct Peer {
	pub id: u64,
	pub context: Vec<u8>,
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
    pub fn new(c: &mut Config, storage: T) -> Raft<T> {
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
		info!(
            "{} newRaft [peers: {:?}, term: {}, commit: {}, applied: {}, last_index: {}, \
             last_term: {}]",
            r.tag,
            r.nodes(),
            r.term,
            r.raft_log.committed,
            r.raft_log.get_applied(),
            r.raft_log.last_index(),
            r.raft_log.last_term()
        );
		r 
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

	fn nodes(&self) -> Vec<u64> {
		let mut nodes: Vec<u64> = self.prs.iter().map(|(&id, _)| id).collect();
		nodes.sort();
		nodes
	}

	pub fn add_node(&mut self, id: u64) {
		self.add_node_or_learner_node(id, false);
	}

	pub fn add_learner(&mut self, id: u64) {
		self.add_node_or_learner_node(id, true);
	}

	pub fn add_node_or_learner_node(&mut self, id: u64, is_learner: bool) {
		if self.prs.contains_key(&id) {
			if is_learner {
				info!(
                    "{} ignored add learner: do not support changing {} from voter to learner",
                    self.tag, id
                );
			}
			return
		} else if self.learner_prs.contains_key(&id) {
			if is_learner {
                // ignore redundant add learner.
                return;
            }
			self.promote_learner(id);
            if id == self.id {
                self.is_learner = false;
            }
		} else {
			let last_index = self.raft_log.last_index();
			self.set_progress(id, 0, last_index+1, is_learner);
		}
		self.get_progress(id).unwrap().recent_active = true;
	}

	pub fn promote_learner(&mut self, id: u64) {
        if let Some(mut pr) = self.learner_prs.remove(&id) {
            pr.is_learner = false;
            self.prs.insert(id, pr);
            return;
        }
        panic!("promote not exists learner: {}", id);
    }

	fn get_progress(&mut self, id: u64) -> Option<&mut Progress> {
		self.prs.get_mut(&id).or(self.learner_prs.get_mut(&id))
	}

	fn set_progress(&mut self, id: u64, matched: u64, next: u64, is_learner: bool) {
		if !is_learner {
			self.learner_prs.remove(&id);
			let mut pr = Progress::new(next, self.max_inflight as usize, is_learner);
			pr.matched = matched;
			self.prs.insert(id, pr);
			return
		}

		if self.prs.contains_key(&id) {
			panic!("{} unexpected changing from voter to learner for {}", self.id, id);
		}
		let mut pr = Progress::new(next, self.max_inflight as usize, is_learner);
		pr.matched = matched;
		self.learner_prs.insert(id, pr);
	}

	pub fn soft_state(&self) -> SoftState {
		SoftState{
			lead: self.lead,
			raft_state: self.state,
		}
	}

	pub fn hard_state(&self) -> HardState {
		let mut hs = HardState::new();
        hs.set_term(self.term);
        hs.set_vote(self.vote);
        hs.set_commit(self.raft_log.committed);
        hs
	}

	/// tick_election is run by followers and candidates after election_timeout.
	pub fn tick_election(&mut self) {
		self.election_elapsed += 1;
		if self.promotable() && self.past_election_timeout() {
			self.election_elapsed = 0;
			let mut msg = Message::new();
			msg.set_from(self.id);
			msg.set_msg_type(MessageType::MsgHup);
			self.step(msg).is_ok();
		}
	} 

	/// promotable indicates whether state machine can be promoted to leader,
	/// which is true when its own id is in progress list.
	pub fn promotable(&self) -> bool {
		self.prs.contains_key(&self.id)
	}

	/// past_election_timeout returns true iff r.electionElapsed is greater
	/// than or equal to the randomized election timeout in
	/// [electiontimeout, 2 * electiontimeout - 1].
	pub fn past_election_timeout(&self) -> bool {
		self.election_elapsed >= self.randomized_election_timeout
	}

	pub fn step(&mut self, msg: Message) -> Result<()> {
		// Handle the message term, which may result in our stepping down to a follower.
		if msg.get_term() == 0 {
			// local message
		} else if msg.get_term() > self.term {
			if msg.get_msg_type() == MessageType::MsgVote || msg.get_msg_type() == MessageType::MsgPreVote {
				let force = msg.get_context() == CAMPAIGN_TRANSFER;
				let in_lease = self.check_quorum && self.lead != NONE && self.election_elapsed < self.election_timeout;

				if !force && in_lease {
					// If a server receives a RequestVote request within the minimum election timeout
					// of hearing from a current leader, it does not update its term or grant its vote
					info!(
						"{} {} [logterm: {}, index: {}, vote: {}] ignored {:?} from {} [logterm: {}, index: {}] at term {}: lease is not expired (remaining ticks: {})",
						self.tag,
						self.id, 
						self.raft_log.last_term(), 
						self.raft_log.last_index(), 
						self.vote, 
						msg.get_msg_type(), 
						msg.get_from(), 
						msg.get_log_term(), 
						msg.get_index(), 
						self.term, 
						self.election_timeout-self.election_elapsed
					)
				}
			}

			if msg.get_msg_type() == MessageType::MsgPreVote {
				// Never change our term in response to a PreVote
			} else if msg.get_msg_type() == MessageType::MsgPreVoteResp && !msg.get_reject() {
				// We send pre-vote requests with a term in our future. If the
				// pre-vote is granted, we will increment our term when we get a
				// quorum. If it is not, the term comes from the node that
				// rejected our vote so we should become a follower at the new
				// term.
			} else {
				info!(
					"{} {} [term: {}] received a {:?} message with higher term from {} [term: {}]",
					self.tag,
					self.id, 
					self.term, 
					msg.get_msg_type(), 
					msg.get_from(), 
					msg.get_term(),
				);

				if msg.get_msg_type() == MessageType::MsgApp 
					|| msg.get_msg_type() == MessageType::MsgHeartbeat 
					|| msg.get_msg_type() == MessageType::MsgSnap {
					self.become_follower(msg.get_term(), msg.get_from());
				} else {
					self.become_follower(msg.get_term(), NONE);
				}
			}
		} else if msg.get_term() < self.term {
			if (self.check_quorum || self.pre_vote) 
				&& (msg.get_msg_type() == MessageType::MsgHeartbeat 
				|| msg.get_msg_type() == MessageType::MsgApp) {
				// We have received messages from a leader at a lower term. It is possible
				// that these messages were simply delayed in the network, but this could
				// also mean that this node has advanced its term number during a network
				// partition, and it is now unable to either win an election or to rejoin
				// the majority on the old term. If check_quorum is false, this will be
				// handled by incrementing term numbers in response to MsgVote with a
				// higher term, but if check_quorum is true we may not advance the term on
				// MsgVote and must generate other messages to advance the term. The net
				// result of these two features is to minimize the disruption caused by
				// nodes that have been removed from the cluster's configuration: a
				// removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
				// but it will not receive MsgApp or MsgHeartbeat, so it will not create
				// disruptive term increases
				// The above comments also true for Pre-Vote

				let mut m = Message::new();
				m.set_to(msg.get_from());
				m.set_msg_type(MessageType::MsgAppResp);
				self.send(m);
			} else if msg.get_msg_type() == MessageType::MsgPreVote {
				info!(
					"{} {} [logterm: {}, index: {}, vote: {}] rejected {:?} from {} [logterm: {}, index: {}] at term {}",
					self.tag,
					self.id, 
					self.raft_log.last_term(), 
					self.raft_log.last_index(), 
					self.vote, 
					msg.get_msg_type(), 
					msg.get_from(), 
					msg.get_log_term(), 
					msg.get_index(), 
					self.term,
				);
				let mut m = Message::new();
				m.set_to(msg.get_from());
				m.set_term(self.term);
				m.set_msg_type(MessageType::MsgPreVoteResp);
				m.set_reject(true);
				self.send(m);
			} else {
				info!(
					"{} {} [term: {}] ignored a {:?} message with lower term from {} [term: {}]",
					self.tag,
					self.id, 
					self.term,
					msg.get_msg_type(), 
					msg.get_from(), 
					msg.get_term(),
				)
			}
			return Ok(());
		}
		
		Ok(())
	}

	// send persists state to stable storage and then sends to its mailbox.
	fn send(&mut self, mut msg: Message) {
		msg.set_from(self.id);

		if msg.get_msg_type() == MessageType::MsgVote 
			|| msg.get_msg_type() == MessageType::MsgVoteResp 
			|| msg.get_msg_type() == MessageType::MsgPreVote 
			|| msg.get_msg_type() == MessageType::MsgPreVoteResp {
			if msg.get_term() == 0 {
				// All {pre-,}campaign messages need to have the term set when
				// sending.
				// - MsgVote: msg.term is the term the node is campaigning for,
				//   non-zero as we increment the term when campaigning.
				// - MsgVoteResp: msg.term is the new self.term if the MsgVote was
				//   granted, non-zero for the same reason MsgVote is
				// - MsgPreVote: msg.term is the term the node will campaign,
				//   non-zero as we use m.Term to indicate the next term we'll be
				//   campaigning for
				// - MsgPreVoteResp: msg.term is the term received in the original
				//   MsgPreVote if the pre-vote was granted, non-zero for the
				//   same reasons MsgPreVote is
				panic!("term should be set when sending {:?}", msg.get_msg_type());
			}
		} else {
			if msg.get_term() != 0 {
				panic!("term should not be set when sending {:?} (was {})", msg.get_msg_type(), msg.get_term());
			}

			// do not attach term to MsgProp, MsgReadIndex
			// proposals are a way to forward to the leader and
			// should be treated as local message.
			// MsgReadIndex is also forwarded to leader.
			if msg.get_msg_type() == MessageType::MsgProp || msg.get_msg_type() == MessageType::MsgReadIndex {
				msg.set_term(self.term);
			}
		}
		self.msgs.push(msg);
	} 
}