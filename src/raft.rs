use std::cmp;
use std::collections::HashMap;
use std::mem;

use errors::{Error, Result, StorageError};
use progress::{Progress, ProgressState};
use protobuf::RepeatedField;
use raft_log::RaftLog;
use raftpb::{Entry, EntryType, HardState, Message, MessageType, Snapshot};
use raw_node::SoftState;
use read_only::{ReadOnly, ReadOnlyOption, ReadState};
use storage::Storage;
use util::{num_of_pending_conf, vote_msg_resp_type, NO_LIMIT};

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

#[derive(Debug, Default)]
pub struct Status {
	pub id: u64,
	pub hard_state: HardState,
	pub soft_state: SoftState,
	pub applied: u64,
	pub progress: HashMap<u64, Progress>,
	pub lead_transferee: u64,
}

#[derive(Debug, Default)]
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

// Calculate the quorum of a Raft cluster with the specified total nodes.
pub fn quorum(total: usize) -> usize {
	total / 2 + 1
}

impl Config {
	fn validate(&mut self) -> Result<()> {
		if self.id == NONE {
			return Err(Error::ConfigInvalid("invalid node id".to_string()));
		}

		if self.heartbeat_tick == 0 {
			return Err(Error::ConfigInvalid(
				"heartbeat tick must greater than 0".to_string(),
			));
		}

		if self.max_size_per_msg == 0 {
			return Err(Error::ConfigInvalid(
				"max inflight messages must be greater than 0".to_string(),
			));
		}

		if self.read_only_option == ReadOnlyOption::LeaseBased && !self.check_quorum {
			return Err(Error::ConfigInvalid(
				"check_quorum must be enabled when ReadOnlyOption is ReadOnlyOption::LeaseBased"
					.to_string(),
			));
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
	pub read_states: Vec<ReadState>,
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
	pub pre_vote: bool,

	pub heartbeat_timeout: u64,
	pub election_timeout: u64,

	// randomized_election_timeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	pub randomized_election_timeout: u64,
	pub disable_proposal_forwarding: bool,

	/// tag only used for logger.
	tag: String,

	/// Will be called when step** is about to be called.
	/// return false will skip step**.
	pub before_step_state: Option<Box<FnMut(&Message) -> bool>>,
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

		let mut r = Raft {
			id: c.id,
			term: Default::default(),
			vote: Default::default(),
			read_states: Default::default(),
			raft_log,
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
			read_only: ReadOnly::new(c.read_only_option),
			election_elapsed: Default::default(),
			heartbeat_elapsed: Default::default(),
			check_quorum: c.check_quorum,
			pre_vote: c.pre_vote,
			heartbeat_timeout: c.heartbeat_tick,
			election_timeout: c.election_tick,
			randomized_election_timeout: Default::default(),
			tag: c.tag.clone(),
			disable_proposal_forwarding: c.disable_proposal_forwarding,
			before_step_state: None,
		};

		for &p in peers {
			r.prs
				.insert(p, Progress::new(1, r.max_inflight as usize, false));
		}
		for &p in learners {
			if r.prs.contains_key(&p) {
				panic!("node {} in both learner and peer list", p);
			}
			r.learner_prs
				.insert(p, Progress::new(1, r.max_inflight as usize, true));
			if r.id == p {
				r.is_learner = true;
			}
		}

		if hard_state != HardState::new() {
			r.load_state(&hard_state);
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

	pub fn get_status(&self) -> Status {
		let mut s = Status {
			id: self.id,
			lead_transferee: self.lead_transferee,
			..Default::default()
		};

		s.hard_state = self.hard_state();
		s.soft_state = self.soft_state();
		s.applied = self.raft_log.applied;

		if s.soft_state.raft_state == StateType::Leader {
			s.progress = HashMap::new();
			for (&id, p) in &self.prs {
				s.progress.insert(id, p.clone());
			}

			for (&id, p) in &self.learner_prs {
				s.progress.insert(id, p.clone());
			}
		}

		s
	}

	pub fn load_state(&mut self, state: &HardState) {
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

	fn append_entry(&mut self, ents: &mut [Entry]) {
		let mut li = self.raft_log.last_index();
		for (i, e) in ents.iter_mut().enumerate() {
			e.set_term(self.term);
			e.set_index(li + 1 + i as u64);
		}

		li = self.raft_log.append(ents);
		let id = self.id;

		// use latest "last" index after truncate/append
		self.get_mut_progress(id).unwrap().maybe_update(li);
		// Regardless of maybe_commit's return, our caller will call bcast_append.
		self.maybe_commit();
	}

	// maybe_commit attempts to advance the commit index. Returns true if
	// the commit index changed (in which case the caller should call
	// self.bcast_append).
	fn maybe_commit(&mut self) -> bool {
		let mut matched_indexs = Vec::with_capacity(self.prs.len());
		for p in self.prs.values() {
			matched_indexs.push(p.matched);
		}
		matched_indexs.sort_by(|a, b| b.cmp(a));
		let max_matched_index = matched_indexs[self.quorum() - 1];
		self.raft_log.maybe_commit(max_matched_index, self.term)
	}

	pub fn become_follower(&mut self, term: u64, lead: u64) {
		self.reset(term);
		self.state = StateType::Follower;
		self.lead = lead;
		info!("{} became follower at term {}", self.tag, self.term);
	}

	fn become_leader(&mut self) {
		if self.state == StateType::Follower {
			panic!("invalid transition [follower -> leader]");
		}

		let term = self.term;
		self.reset(term);
		self.lead = self.id;
		self.state = StateType::Leader;

		let ents = match self.raft_log.entries(self.raft_log.committed + 1, NO_LIMIT) {
			Ok(ents) => ents,
			Err(e) => panic!("unexpected error getting uncommitted entries ({:?})", e),
		};

		// Conservatively set the pending_conf_index to the last index in the
		// log. There may or may not be a pending config change, but it's
		// safe to delay any future proposals until we commit all our
		// pending log entries, and scanning the entire tail of the log
		// could be expensive.
		if !ents.is_empty() {
			self.pending_conf_index = ents[ents.len() - 1].get_index();
		}

		self.append_entry(&mut [Entry::new()]);
		info!(
			"{} {} became leader at term {}",
			self.tag, self.id, self.term
		);
	}

	pub fn become_candidate(&mut self) {
		if self.state == StateType::Leader {
			panic!("invalid transition [leader -> candidate]");
		}
		let term = self.term;
		self.reset(term + 1);
		self.vote = self.id;
		self.state = StateType::Candidate;
		info!(
			"{} {} became candidate at term {}",
			self.tag, self.id, self.term
		);
	}

	pub fn become_pre_candidate(&mut self) {
		if self.state == StateType::Leader {
			panic!("invalid transition [leader -> pre-candidate]")
		}
		self.votes = HashMap::new();
		self.state = StateType::PreCandidate;
		info!(
			"{} {} became pre-candidate at term {}",
			self.tag, self.id, self.term
		);
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

		for (&id, pr) in &mut self.prs {
			*pr = Progress::new(last_index + 1, max_inflight as usize, false);
			if id == self_id {
				pr.matched = last_index;
			}
		}
		for (&id, pr) in &mut self.learner_prs {
			*pr = Progress::new(last_index + 1, max_inflight as usize, true);
			if id == self_id {
				pr.matched = last_index;
			}
		}

		self.read_only = ReadOnly::new(self.read_only.option);
		self.pending_conf_index = 0;
	}

	fn reset_randomized_election_timeout(&mut self) {
		let prev_timeout = self.randomized_election_timeout;
		let timeout =
			self.election_timeout + rand::thread_rng().gen_range(0, self.election_timeout);
		debug!(
			"{} reset election timeout {} -> {} at {}",
			self.tag, prev_timeout, timeout, self.election_elapsed
		);

		self.randomized_election_timeout = timeout;
	}

	pub fn abort_leader_transfer(&mut self) {
		self.lead_transferee = NONE;
	}

	pub fn nodes(&self) -> Vec<u64> {
		let mut nodes: Vec<u64> = self.prs.iter().map(|(&id, _)| id).collect();
		nodes.sort();
		nodes
	}

	pub fn learner_nodes(&self) -> Vec<u64> {
		let mut nodes: Vec<u64> = self.learner_prs.iter().map(|(&id, _)| id).collect();
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
			return;
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
			self.set_progress(id, 0, last_index + 1, is_learner);
		}
		self.get_mut_progress(id).unwrap().recent_active = true;
	}

	pub fn remove_node(&mut self, id: u64) {
		self.del_progress(id);

		// do not try to commit or abort transferring if there is no nodes in the cluster.
		if self.prs.is_empty() && self.learner_prs.is_empty() {
			return;
		}

		// The quorum size is now smaller, so see if any pending entries can
		// be committed.
		if self.maybe_commit() {
			self.bcast_append();
		}

		// If the removed node is the leadTransferee, then abort the leadership transferring.
		if self.state == StateType::Leader && self.lead_transferee == id {
			self.abort_leader_transfer();
		}
	}

	fn del_progress(&mut self, id: u64) {
		self.prs.remove(&id);
		self.learner_prs.remove(&id);
	}

	fn promote_learner(&mut self, id: u64) {
		if let Some(mut pr) = self.learner_prs.remove(&id) {
			pr.is_learner = false;
			self.prs.insert(id, pr);
			return;
		}
		panic!("promote not exists learner: {}", id);
	}

	fn get_mut_progress(&mut self, id: u64) -> Option<&mut Progress> {
		self.prs.get_mut(&id).or(self.learner_prs.get_mut(&id))
	}

	pub fn get_progress(&self, id: u64) -> Option<&Progress> {
		self.prs.get(&id).or_else(|| self.learner_prs.get(&id))
	}

	fn set_progress(&mut self, id: u64, matched: u64, next: u64, is_learner: bool) {
		if !is_learner {
			self.learner_prs.remove(&id);
			let mut pr = Progress::new(next, self.max_inflight as usize, is_learner);
			pr.matched = matched;
			self.prs.insert(id, pr);
			return;
		}

		if self.prs.contains_key(&id) {
			panic!(
				"{} unexpected changing from voter to learner for {}",
				self.id, id
			);
		}
		let mut pr = Progress::new(next, self.max_inflight as usize, is_learner);
		pr.matched = matched;
		self.learner_prs.insert(id, pr);
	}

	pub fn soft_state(&self) -> SoftState {
		SoftState {
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

	pub fn tick(&mut self) {
		match self.state {
			StateType::Follower | StateType::PreCandidate | StateType::Candidate => {
				self.tick_election()
			}
			StateType::Leader => self.tick_heartbeat(),
		}
	}

	/// tick_election is run by followers and candidates after election_timeout.
	fn tick_election(&mut self) {
		self.election_elapsed += 1;
		if self.promotable() && self.past_election_timeout() {
			self.election_elapsed = 0;
			let mut msg = Message::new();
			msg.set_from(self.id);
			msg.set_msg_type(MessageType::MsgHup);
			self.step(msg).is_ok();
		}
	}

	fn tick_heartbeat(&mut self) {
		self.heartbeat_elapsed += 1;
		self.election_elapsed += 1;

		if self.election_elapsed >= self.election_timeout {
			self.election_elapsed = 0;

			if self.check_quorum {
				let mut m = Message::new();
				m.set_from(self.id);
				m.set_msg_type(MessageType::MsgCheckQuorum);
				self.step(m).is_ok();
			}

			// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
			if self.state == StateType::Leader && self.lead_transferee != NONE {
				self.abort_leader_transfer();
			}
		}

		if self.state != StateType::Leader {
			return;
		}

		if self.heartbeat_elapsed >= self.heartbeat_timeout {
			self.heartbeat_elapsed = 0;
			let mut m = Message::new();
			m.set_from(self.id);
			m.set_msg_type(MessageType::MsgBeat);
			self.step(m).is_ok();
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
			if msg.get_msg_type() == MessageType::MsgVote
				|| msg.get_msg_type() == MessageType::MsgPreVote
			{
				let force = msg.get_context() == CAMPAIGN_TRANSFER;
				let in_lease = self.check_quorum
					&& self.lead != NONE
					&& self.election_elapsed < self.election_timeout;

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
					);

					return Ok(());
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
					|| msg.get_msg_type() == MessageType::MsgSnap
				{
					self.become_follower(msg.get_term(), msg.get_from());
				} else {
					self.become_follower(msg.get_term(), NONE);
				}
			}
		} else if msg.get_term() < self.term {
			if (self.check_quorum || self.pre_vote)
				&& (msg.get_msg_type() == MessageType::MsgHeartbeat
					|| msg.get_msg_type() == MessageType::MsgApp)
			{
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
				// Before Pre-Vote enable, there may have candidate with higher term,
				// but less log. After update to Pre-Vote, the cluster may deadlock if
				// we drop messages with a lower term.

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
				// ignore other cases
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

		if msg.get_msg_type() == MessageType::MsgHup {
			if self.state != StateType::Leader {
				let ents = match self.raft_log.slice(
					self.raft_log.applied + 1,
					self.raft_log.committed + 1,
					NO_LIMIT,
				) {
					Ok(ents) => ents,
					Err(e) => panic!(e),
				};

				let n = num_of_pending_conf(&ents);
				if n > 0 && self.raft_log.committed > self.raft_log.applied {
					warn!(
						"{} {} cannot campaign at term {} since there are still {} pending configuration changes to apply", 
						self.tag,
						self.id,
						self.term,
						n,
					);
					return Ok(());
				}

				info!(
					"{} {} is starting a new election at term {}",
					self.tag, self.id, self.term
				);

				if self.pre_vote {
					self.campaign(CAMPAIGN_PRE_ELECTION);
				} else {
					self.campaign(CAMPAIGN_ELECTION);
				}
			} else {
				debug!(
					"{} {} ignoring MsgHup because already leader",
					self.tag, self.term
				);
			}
		} else if msg.get_msg_type() == MessageType::MsgPreVote
			|| msg.get_msg_type() == MessageType::MsgVote
		{
			if self.is_learner {
				info!(
					"{} {} [logterm: {}, index: {}, vote: {}] ignored {:?} from {} [logterm: {}, index: {}] at term {}: learner can not vote",
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
				return Ok(());
			}

			// We can vote if this is a repeat of a vote we've already cast...
			// ...we haven't voted and we don't think there's a leader yet in this term...
			// ...or this is a PreVote for a future term...
			let can_vote = self.vote == msg.get_from()
				|| (self.vote == NONE && self.lead == NONE)
				|| (msg.get_msg_type() == MessageType::MsgPreVote && msg.get_term() > self.term);

			// ...and we believe the candidate is up to date.
			if can_vote
				&& self
					.raft_log
					.is_up_to_date(msg.get_index(), msg.get_log_term())
			{
				info!(
					"{} {} [logterm: {}, index: {}, vote: {}] cast {:?} for {} [logterm: {}, index: {}] at term {}",
					self.tag,
					self.id, 
					self.raft_log.last_term(),
					self.raft_log.last_index(), 
					self.vote, 
					msg.get_msg_type(), 
					msg.get_from(), 
					msg.get_log_term(), 
					msg.get_index(), 
					msg.get_term(),
				);

				// When responding to Msg{Pre,}Vote messages we include the term
				// from the message, not the local term. To see why consider the
				// case where a single node was previously partitioned away and
				// it's local term is now of date. If we include the local term
				// (recall that for pre-votes we don't update the local term), the
				// (pre-)campaigning node on the other end will proceed to ignore
				// the message (it ignores all out of date messages).
				// The term in the original message and current local term are the
				// same in the case of regular votes, but different for pre-votes.
				let mut m = Message::new();
				m.set_to(msg.get_from());
				m.set_term(msg.get_term());
				m.set_msg_type(vote_msg_resp_type(msg.get_msg_type()));
				self.send(m);

				if msg.get_msg_type() == MessageType::MsgVote {
					self.election_elapsed = 0;
					self.vote = msg.get_from();
				}
			} else {
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
					msg.get_term(),
				);

				let mut m = Message::new();
				m.set_to(msg.get_from());
				m.set_term(self.term);
				m.set_msg_type(vote_msg_resp_type(msg.get_msg_type()));
				m.set_reject(true);
				self.send(m);
			}
		} else {
			match self.state {
				StateType::PreCandidate | StateType::Candidate => self.step_candidate(msg)?,
				StateType::Follower => self.step_follower(msg)?,
				StateType::Leader => self.step_leader(msg)?,
			}
		}

		Ok(())
	}

	fn step_follower(&mut self, mut msg: Message) -> Result<()> {
		match msg.get_msg_type() {
			MessageType::MsgProp => {
				if self.lead == NONE {
					info!(
						"{} {} no leader at term {}; dropping proposal",
						self.tag, self.id, self.term
					);
					return Err(Error::ProposalDropped);
				} else if self.disable_proposal_forwarding {
					info!(
						"{} {} not forwarding to leader {} at term {}; dropping proposal",
						self.tag, self.id, self.lead, self.term,
					);
					return Err(Error::ProposalDropped);
				}

				msg.set_to(self.lead);
				self.send(msg);
			}
			MessageType::MsgApp => {
				self.election_elapsed = 0;
				self.lead = msg.get_from();
				self.handle_append_entries(&msg);
			}
			MessageType::MsgHeartbeat => {
				self.election_elapsed = 0;
				self.lead = msg.get_from();
				self.handle_heartbeat(msg);
			}
			MessageType::MsgSnap => {
				self.election_elapsed = 0;
				self.lead = msg.get_from();
				self.handle_snapshot(msg);
			}
			MessageType::MsgTransferLeader => {
				if self.lead == NONE {
					info!(
						"{} {} no leader at term {}; dropping leader transfer msg",
						self.tag, self.id, self.term,
					);
					return Ok(());
				}
				msg.set_to(self.lead);
				self.send(msg);
			}
			MessageType::MsgTimeoutNow => {
				if self.promotable() {
					info!(
						"{} {} [term {}] received MsgTimeoutNow from {} and starts an election to get leadership.", 
						self.tag,
						self.id, 
						self.term, 
						msg.get_from(),
					);

					// Leadership transfers never use pre-vote even if r.preVote is true; we
					// know we are not recovering from a partition so there is no need for the
					// extra round trip.
					self.campaign(CAMPAIGN_TRANSFER);
				} else {
					info!(
						"{} {} received MsgTimeoutNow from {} but is not promotable",
						self.tag,
						self.id,
						msg.get_from(),
					);
				}
			}
			MessageType::MsgReadIndex => {
				if self.lead == NONE {
					info!(
						"{} {} no leader at term {}; dropping leader transfer msg",
						self.tag, self.id, self.term,
					);
					return Ok(());
				}
				msg.set_to(self.lead);
				self.send(msg);
			}
			MessageType::MsgReadIndexResp => {
				if msg.get_entries().len() != 1 {
					error!(
						"{} {} invalid format of MsgReadIndexResp from {}, entries count: {}",
						self.tag,
						self.id,
						msg.get_from(),
						msg.get_entries().len(),
					);
					return Ok(());
				}
				let rs = ReadState {
					index: msg.get_index(),
					request_ctx: msg.take_entries()[0].take_data(),
				};
				self.read_states.push(rs);
			}
			_ => return Ok(()),
		}
		Ok(())
	}

	fn step_leader(&mut self, mut msg: Message) -> Result<()> {
		match msg.get_msg_type() {
			MessageType::MsgBeat => {
				self.bcast_heartbeat();
				return Ok(());
			}
			MessageType::MsgCheckQuorum => {
				if !self.check_quorum_active() {
					warn!(
						"{} {} stepped down to follower since quorum is not active",
						self.tag, self.id
					);
					let term = self.term;
					self.become_follower(term, NONE);
				}
				return Ok(());
			}
			MessageType::MsgProp => {
				if msg.get_entries().is_empty() {
					panic!("{} stepped empty MsgProp", self.id);
				}
				if !self.prs.contains_key(&self.id) {
					// If we are not currently a member of the range (i.e. this node
					// was removed from the configuration while serving as leader),
					// drop any new proposals.
					return Err(Error::ProposalDropped);
				}
				if self.lead_transferee != NONE {
					debug!(
						"{} {} [term {}] transfer leadership to {} is in progress; dropping proposal", 
						self.tag,
						self.id,
						self.term,
						self.lead_transferee,
					);
					return Err(Error::ProposalDropped);
				}

				for (i, e) in msg.mut_entries().iter_mut().enumerate() {
					if e.get_entry_type() == EntryType::EntryConfChange {
						if self.pending_conf_index > self.raft_log.applied {
							info!(
								"{} propose conf {:?} ignored since pending unapplied configuration [index {}, applied {}]",
								self.tag,
								e,
								self.pending_conf_index,
								self.raft_log.applied,
							);
							*e = Entry::new();
							e.set_entry_type(EntryType::EntryNormal);
						} else {
							self.pending_conf_index = self.raft_log.last_index() + i as u64 + 1;
						}
					}
				}
				self.append_entry(msg.mut_entries());
				self.bcast_append();
				return Ok(());
			}
			MessageType::MsgReadIndex => {
				if self.quorum() > 1 {
					if self
						.raft_log
						.zero_term_on_err_compacted(self.raft_log.term(self.raft_log.committed))
						!= self.term
					{
						// Reject read only request when this leader has not committed any log entry at its term.
						return Ok(());
					}

					// thinking: use an interally defined context instead of the user given context.
					// We can express this in terms of the term and index instead of a user-supplied value.
					// This would allow multiple reads to piggyback on the same message.
					match self.read_only.option {
						ReadOnlyOption::Safe => {
							let ctx = msg.get_entries()[0].get_data().to_vec();
							self.read_only.add_request(self.raft_log.committed, msg);
							self.bcast_heartbeat_with_ctx(&Some(ctx));
						}
						ReadOnlyOption::LeaseBased => {
							let ri = self.raft_log.committed;
							if msg.get_from() == NONE || msg.get_from() == self.id {
								let rs = ReadState {
									index: ri,
									request_ctx: msg.take_entries()[0].take_data(),
								};
								self.read_states.push(rs);
							} else {
								let mut m = Message::new();
								m.set_to(msg.get_from());
								m.set_msg_type(MessageType::MsgReadIndexResp);
								m.set_index(ri);
								m.set_entries(msg.take_entries());
								self.send(m);
							}
						}
					}
				} else {
					let rs = ReadState {
						index: self.raft_log.committed,
						request_ctx: msg.take_entries()[0].take_data(),
					};
					self.read_states.push(rs);
				}
				Ok(())
			}
			_ => {
				// All other message types require a progress for msg.from (pr).
				if !self.prs.contains_key(&msg.get_from())
					&& !self.learner_prs.contains_key(&msg.get_from())
				{
					debug!(
						"{} {} no progress available for {}",
						self.tag,
						self.id,
						msg.get_from()
					);
					return Ok(());
				}

				let mut prs = self.take_prs();
				let mut learner_prs = self.take_learner_prs();

				let mut old_paused = false;
				let mut maybe_commit = false;
				let mut send_append = false;
				let mut more_to_send = None;
				let quorum = quorum(prs.len()) as u64;

				if let Some(pr) = prs
					.get_mut(&msg.get_from())
					.or_else(|| learner_prs.get_mut(&msg.get_from()))
				{
					match msg.get_msg_type() {
						MessageType::MsgAppResp => {
							self.handle_append_resp(
								pr,
								&msg,
								&mut old_paused,
								&mut maybe_commit,
								&mut send_append,
							);
						}
						MessageType::MsgHeartbeatResp => {
							self.handle_heartbeat_resp(
								pr,
								&msg,
								quorum,
								&mut send_append,
								&mut more_to_send,
							);
						}
						MessageType::MsgSnapStatus => {
							self.handle_snap_status(pr, &msg);
						}
						MessageType::MsgUnreachable => {
							self.handle_unreachable(pr, &msg);
						}
						MessageType::MsgTransferLeader => {
							self.handle_transfer_leader(pr, &msg);
						}
						_ => {}
					}
				}
				self.set_prs(prs);
				self.set_learner_prs(learner_prs);
				if maybe_commit {
					if self.maybe_commit() {
						self.bcast_append();
					} else if old_paused {
						// update() reset the wait state on this node. If we had delayed sending
						// an update before, send it now.
						send_append = true;
					}
				}

				if send_append {
					let from = msg.get_from();
					let mut prs = self.take_prs();
					let mut learner_prs = self.take_learner_prs();
					self.send_append(
						from,
						prs.get_mut(&from)
							.or_else(|| learner_prs.get_mut(&from))
							.unwrap(),
					);
					self.set_prs(prs);
					self.set_learner_prs(learner_prs);
				}

				if let Some(mut m) = more_to_send {
					self.send(m);
				}

				Ok(())
			}
		}
	}

	fn handle_transfer_leader(&mut self, pr: &mut Progress, msg: &Message) {
		if pr.is_learner {
			debug!(
				"{} {} is learner. Ignored transferring leadership",
				self.tag, self.id
			);
			return;
		}

		let lead_transferee = msg.get_from();
		let last_lead_transferee = self.lead_transferee;

		if last_lead_transferee != NONE {
			if last_lead_transferee == lead_transferee {
				info!(
					"{} {} [term {}] transfer leadership to {} is in progress, ignores request to same node {}",
					self.tag,
					self.id,
					self.term,
					lead_transferee,
					lead_transferee,
				);
				return;
			}

			self.abort_leader_transfer();
			info!(
				"{} {} [term {}] abort previous transferring leadership to {}",
				self.tag, self.id, self.term, last_lead_transferee,
			);
		}

		if lead_transferee == self.id {
			debug!(
				"{} {} is already leader. Ignored transferring leadership to self",
				self.tag, self.id
			);
			return;
		}
		// Transfer leadership to third party.
		info!(
			"{} {} [term {}] starts to transfer leadership to {}",
			self.tag, self.id, self.term, lead_transferee
		);
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		self.election_elapsed = 0;
		self.lead_transferee = lead_transferee;
		if pr.matched == self.raft_log.last_index() {
			self.send_timeout_now(lead_transferee);
			info!(
				"{} {} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log",
				self.tag, self.id, lead_transferee, lead_transferee,
			);
		} else {
			self.send_append(lead_transferee, pr);
		}
	}

	fn handle_unreachable(&mut self, pr: &mut Progress, msg: &Message) {
		// During optimistic replication, if the remote becomes unreachable,
		// there is huge probability that a MsgApp is lost.
		if pr.state == ProgressState::Replicate {
			pr.become_probe();
		}

		debug!(
			"{} failed to send message to {} because it is unreachable [{:?}]",
			self.tag,
			msg.get_from(),
			pr,
		);
	}

	fn handle_snap_status(&mut self, pr: &mut Progress, msg: &Message) {
		if pr.state != ProgressState::Snapshot {
			return;
		}
		if !msg.get_reject() {
			pr.become_probe();
			debug!(
				"{} {} snapshot succeeded, resumed sending replication messages to {} [{:?}]",
				self.tag,
				self.id,
				msg.get_from(),
				pr,
			);
		} else {
			pr.snapshot_failure();
			pr.become_probe();
			debug!(
				"{} {} snapshot failed, resumed sending replication messages to {} [{:?}]",
				self.tag,
				self.id,
				msg.get_from(),
				pr,
			);
		}

		// If snapshot finish, wait for the msgAppResp from the remote node before sending
		// out the next msgApp.
		// If snapshot failure, wait for a heartbeat interval before next try
		pr.pause();
	}

	fn handle_heartbeat_resp(
		&mut self,
		pr: &mut Progress,
		msg: &Message,
		quorum: u64,
		send_append: &mut bool,
		more_to_send: &mut Option<Message>,
	) {
		pr.recent_active = true;
		pr.resume();

		if pr.state == ProgressState::Replicate && pr.ins.full() {
			pr.ins.free_first_one();
		}
		if pr.matched < self.raft_log.last_index() {
			*send_append = true;
		}
		if self.read_only.option != ReadOnlyOption::Safe || msg.get_context().is_empty() {
			return;
		}

		let ack_count = self.read_only.recv_ack(&msg) as u64;
		if ack_count < quorum {
			return;
		}

		let rss = self.read_only.advance(msg);
		for rs in rss {
			let mut req = rs.req;
			if req.get_from() == NONE || req.get_from() == self.id {
				let s = ReadState {
					index: rs.index,
					request_ctx: req.take_entries()[0].take_data(),
				};
				self.read_states.push(s);
			} else {
				let mut m = Message::new();
				m.set_to(req.get_from());
				m.set_msg_type(MessageType::MsgReadIndexResp);
				m.set_index(rs.index);
				m.set_entries(req.take_entries());

				*more_to_send = Some(m);
			}
		}
	}

	fn handle_append_resp(
		&mut self,
		pr: &mut Progress,
		msg: &Message,
		old_paused: &mut bool,
		maybe_commit: &mut bool,
		send_append: &mut bool,
	) {
		pr.recent_active = true;
		if msg.get_reject() {
			debug!(
				"{} {} received msgApp rejection(lastindex: {}) from {} for index {}",
				self.tag,
				self.id,
				msg.get_reject_hint(),
				msg.get_from(),
				msg.get_index(),
			);

			if pr.maybe_decr_to(msg.get_index(), msg.get_reject_hint()) {
				debug!(
					"{} {} decreased progress of {} to [{:?}]",
					self.tag,
					self.id,
					msg.get_from(),
					pr,
				);
				if pr.state == ProgressState::Replicate {
					pr.become_probe();
				}
				*send_append = true;
			}
			return;
		}

		*old_paused = pr.is_paused();
		if !pr.maybe_update(msg.get_index()) {
			return;
		}

		*maybe_commit = true;

		if pr.state == ProgressState::Probe {
			pr.become_replicate();
		} else if pr.state == ProgressState::Snapshot && pr.need_snapshot_abort() {
			pr.become_probe();
		} else {
			pr.ins.free_to(msg.get_index());
		}

		// Transfer leadership is in progress.
		if msg.get_from() == self.lead_transferee {
			info!(
				"{} {} sent MsgTimeoutNow to {} after received MsgAppResp",
				self.tag,
				self.id,
				msg.get_from(),
			);
			self.send_timeout_now(msg.get_from());
		}
	}

	// step_candidate is shared by Candidate and PreCandidate; the difference is
	// whether they respond to MsgVoteResp or MsgPreVoteResp.
	fn step_candidate(&mut self, msg: Message) -> Result<()> {
		match msg.get_msg_type() {
			MessageType::MsgProp => {
				info!(
					"{} {} no leader at term {}; dropping proposal",
					self.tag, self.id, self.term
				);
				return Err(Error::ProposalDropped);
			}
			MessageType::MsgApp => {
				debug_assert_eq!(self.term, msg.get_term());
				self.become_follower(msg.get_term(), msg.get_from());
				self.handle_append_entries(&msg);
			}
			MessageType::MsgHeartbeat => {
				debug_assert_eq!(self.term, msg.get_term());
				self.become_follower(msg.get_term(), msg.get_from());
				self.handle_heartbeat(msg);
			}
			MessageType::MsgSnap => {
				debug_assert_eq!(self.term, msg.get_term());
				self.become_follower(msg.get_term(), msg.get_from());
				self.handle_snapshot(msg);
			}
			MessageType::MsgPreVoteResp | MessageType::MsgVoteResp => {
				if (self.state == StateType::PreCandidate
					&& msg.get_msg_type() != MessageType::MsgPreVoteResp)
					|| (self.state == StateType::Candidate
						&& msg.get_msg_type() != MessageType::MsgVoteResp)
				{
					return Ok(());
				}
				let granted = self.poll(msg.get_from(), msg.get_msg_type(), !msg.get_reject());
				info!(
					"{} {} [quorum:{}] has received {} {:?} votes and {} vote rejections",
					self.tag,
					self.id,
					self.quorum(),
					granted,
					msg.get_msg_type(),
					self.votes.len() - granted,
				);

				if self.quorum() == granted {
					if self.state == StateType::PreCandidate {
						self.campaign(CAMPAIGN_ELECTION);
					} else {
						self.become_leader();
						self.bcast_append();
					}
				} else if self.votes.len() - granted == self.quorum() {
					// MsgPreVoteResp contains future term of pre-candidate
					// msg.term > self.term; reuse self.term
					let term = self.term;
					self.become_follower(term, NONE);
				}
			}
			MessageType::MsgTimeoutNow => {
				info!(
					"{} {} [term {} state {:?}] ignored MsgTimeoutNow from {}",
					self.tag,
					self.id,
					self.term,
					self.state,
					msg.get_from(),
				);
			}
			_ => {}
		}

		Ok(())
	}

	// bcast_append sends RPC, with entries to all peers that are not up-to-date
	// according to the progress recorded in r.prs.
	fn bcast_append(&mut self) {
		let self_id = self.id;
		let mut prs = self.take_prs();
		prs.iter_mut()
			.filter(|&(id, _)| *id != self_id)
			.for_each(|(&id, mut pr)| {
				self.send_append(id, &mut pr);
			});
		self.set_prs(prs);

		let mut learner_prs = self.take_learner_prs();
		learner_prs
			.iter_mut()
			.filter(|&(id, _)| *id != self_id)
			.for_each(|(&id, mut pr)| {
				self.send_append(id, &mut pr);
			});
		self.set_learner_prs(learner_prs);
	}

	fn send_timeout_now(&mut self, to: u64) {
		let mut m = Message::new();
		m.set_to(to);
		m.set_msg_type(MessageType::MsgTimeoutNow);
		self.send(m);
	}

	fn check_quorum_active(&mut self) -> bool {
		let mut act = 0;
		let self_id = self.id;
		let prs = self.take_prs();
		prs.iter().for_each(|(&id, pr)| {
			if id == self_id {
				act += 1;
			}
			if pr.recent_active {
				act += 1;
			}
		});
		self.set_prs(prs);

		let learner_prs = self.take_learner_prs();
		learner_prs.iter().for_each(|(&id, pr)| {
			if id == self_id {
				act += 1;
			}
			if pr.recent_active {
				act += 1;
			}
		});
		self.set_learner_prs(learner_prs);
		act >= self.quorum()
	}

	// bcast_heartbeat sends RPC, without entries to all the peers.
	fn bcast_heartbeat(&mut self) {
		let last_ctx = self.read_only.last_pending_request_ctx();
		self.bcast_heartbeat_with_ctx(&last_ctx);
	}

	fn bcast_heartbeat_with_ctx(&mut self, ctx: &Option<Vec<u8>>) {
		let self_id = self.id;
		let prs = self.take_prs();
		prs.iter()
			.filter(|&(id, _)| *id != self_id)
			.for_each(|(&id, pr)| {
				self.send_heartbeat(id, ctx.clone(), pr);
			});
		self.set_prs(prs);

		let learner_prs = self.take_learner_prs();
		learner_prs
			.iter()
			.filter(|&(id, _)| *id != self_id)
			.for_each(|(&id, pr)| {
				self.send_heartbeat(id, ctx.clone(), pr);
			});
		self.set_learner_prs(learner_prs);
	}

	fn send_heartbeat(&mut self, to: u64, ctx: Option<Vec<u8>>, pr: &Progress) {
		// Attach the commit as min(to.matched, r.committed).
		// When the leader sends out heartbeat message,
		// the receiver(follower) might not be matched with the leader
		// or it might not have all the committed entries.
		// The leader MUST NOT forward the follower's commit to
		// an unmatched index.
		let commit = cmp::min(pr.matched, self.raft_log.committed);
		let mut m = Message::new();
		m.set_commit(commit);
		m.set_to(to);
		m.set_msg_type(MessageType::MsgHeartbeat);
		if let Some(ctx) = ctx {
			m.set_context(ctx);
		}

		self.send(m);
	}

	fn set_prs(&mut self, prs: HashMap<u64, Progress>) {
		mem::replace(&mut self.prs, prs);
	}

	fn take_prs(&mut self) -> HashMap<u64, Progress> {
		mem::replace(&mut self.prs, HashMap::new())
	}

	fn take_learner_prs(&mut self) -> HashMap<u64, Progress> {
		mem::replace(&mut self.learner_prs, HashMap::new())
	}

	fn set_learner_prs(&mut self, learner_prs: HashMap<u64, Progress>) {
		mem::replace(&mut self.learner_prs, learner_prs);
	}

	// send_append sends RPC, with entries to the given peer.
	fn send_append(&mut self, to: u64, pr: &mut Progress) {
		if pr.is_paused() {
			return;
		}

		let mut m = Message::new();
		m.set_to(to);
		let term = self.raft_log.term(pr.next - 1);
		let ents = self.raft_log.entries(pr.next, self.max_msg_size);

		// send snapshot if we failed to get term or entries
		if term.is_err() || ents.is_err() {
			if !pr.recent_active {
				debug!(
					"{} ignore sending snapshot to {} since it is not recently active",
					self.tag, to
				);
				return;
			}

			m.set_msg_type(MessageType::MsgSnap);
			match self.raft_log.snapshot() {
				Ok(s) => {
					if s.get_metadata().get_index() == 0 {
						panic!("need non-empty snapshot");
					}
					let (sindex, sterm) =
						(s.get_metadata().get_index(), s.get_metadata().get_term());

					m.set_snapshot(s);
					debug!(
						"{} {} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {} [{:?}]",
						self.tag,
						self.id, 
						self.raft_log.first_index(), 
						self.raft_log.committed, 
						sindex, 
						sterm, 
						to, 
						pr
					);

					pr.become_snapshot(sindex);
					debug!(
						"{} {} paused sending replication messages to {} [{:?}]",
						self.tag, self.id, to, pr,
					);
				}
				Err(e) => {
					if e == Error::Storage(StorageError::SnapshotTemporarilyUnavailable) {
						debug!(
							"{} {} failed to send snapshot to {} because snapshot is temporarily unavailable", 
							self.tag,
							self.id,
							to,
						);
						return;
					}
					panic!(e)
				}
			}
		} else {
			let term = term.unwrap();
			let ents = ents.unwrap();
			m.set_msg_type(MessageType::MsgApp);
			m.set_index(pr.next - 1);
			m.set_log_term(term);
			m.set_entries(RepeatedField::from_vec(ents));
			m.set_commit(self.raft_log.committed);

			let n = m.get_entries().len();
			if n != 0 {
				// optimistically increase the next when in ProgressState::Replicate
				if pr.state == ProgressState::Replicate {
					let last = m.get_entries()[n - 1].get_index();
					pr.optimistic_update(last);
					pr.ins.add(last);
				} else if pr.state == ProgressState::Probe {
					pr.pause();
				} else {
					panic!(
						"{} is sending append in unhandled state {:?}",
						self.id, pr.state
					);
				}
			}
		}

		self.send(m);
	}

	fn handle_snapshot(&mut self, mut msg: Message) {
		let (sindex, sterm) = (
			msg.get_snapshot().get_metadata().get_index(),
			msg.get_snapshot().get_metadata().get_term(),
		);

		if self.restore(msg.take_snapshot()) {
			info!(
				"{} {} [commit: {}] restore snapshot [index: {}, term: {}]",
				self.tag, self.id, self.raft_log.committed, sindex, sterm
			);

			let mut m = Message::new();
			m.set_to(msg.get_from());
			m.set_msg_type(MessageType::MsgAppResp);
			m.set_index(self.raft_log.last_index());
			self.send(m);
		} else {
			info!(
				"{} {} [commit: {}] ignored snapshot [index: {}, term: {}]",
				self.tag, self.id, self.raft_log.committed, sindex, sterm
			);

			let mut m = Message::new();
			m.set_to(msg.get_from());
			m.set_msg_type(MessageType::MsgAppResp);
			m.set_index(self.raft_log.committed);
			self.send(m);
		}
	}

	// restore recovers the state machine from a snapshot. It restores the log and the
	// configuration of state machine.
	fn restore(&mut self, s: Snapshot) -> bool {
		if s.get_metadata().get_index() < self.raft_log.committed {
			return false;
		}

		if self
			.raft_log
			.match_term(s.get_metadata().get_index(), s.get_metadata().get_term())
		{
			info!(
				"{} {} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
				self.tag,
				self.id, 
				self.raft_log.committed, 
				self.raft_log.last_index(), 
				self.raft_log.last_term(), 
				s.get_metadata().get_index(), 
				s.get_metadata().get_term()
			);
			self.raft_log.commit_to(s.get_metadata().get_index());
			return false;
		}

		// normal peer can not become learner.
		if !self.is_learner {
			if s.get_metadata()
				.get_conf_state()
				.get_learners()
				.contains(&self.id)
			{
				error!(
					"{} {} can't become learner when restores snapshot [index: {}, term: {}]",
					self.tag,
					self.id,
					s.get_metadata().get_index(),
					s.get_metadata().get_term(),
				);

				return false;
			}
		}

		info!(
			"{} {} [commit: {}, lastindex: {}, lastterm: {}] starts to restore snapshot [index: {}, term: {}]",
			self.tag,
			self.id, 
			self.raft_log.committed,
			self.raft_log.last_index(), 
			self.raft_log.last_term(), 
			s.get_metadata().get_index(), 
			s.get_metadata().get_term()
		);

		self.prs.clear();
		self.learner_prs.clear();
		self.restore_node(s.get_metadata().get_conf_state().get_nodes(), false);
		self.restore_node(s.get_metadata().get_conf_state().get_learners(), true);
		self.raft_log.restore(s);

		true
	}

	fn restore_node(&mut self, nodes: &[u64], is_learner: bool) {
		for &n in nodes {
			let (mut matched, mut next) = (0, self.raft_log.last_index() + 1);
			if n == self.id {
				matched = next - 1;
				self.is_learner = is_learner;
			}

			self.set_progress(n, matched, next, is_learner);
			info!(
				"{} {} restored progress of {} [matched: {}, next: {}]",
				self.tag, self.id, n, matched, next
			);
		}
	}

	fn handle_heartbeat(&mut self, mut msg: Message) {
		self.raft_log.commit_to(msg.get_commit());
		let mut m = Message::new();
		m.set_to(msg.get_from());
		m.set_msg_type(MessageType::MsgHeartbeatResp);
		m.set_context(msg.take_context());
		self.send(m);
	}

	fn handle_append_entries(&mut self, msg: &Message) {
		if msg.get_index() < self.raft_log.committed {
			let mut m = Message::new();
			m.set_to(msg.get_from());
			m.set_msg_type(MessageType::MsgAppResp);
			m.set_index(self.raft_log.committed);
			self.send(m);
			return;
		}

		if let Some(mlast_index) = self.raft_log.maybe_append(
			msg.get_index(),
			msg.get_log_term(),
			msg.get_commit(),
			msg.get_entries(),
		) {
			let mut m = Message::new();
			m.set_to(msg.get_from());
			m.set_msg_type(MessageType::MsgAppResp);
			m.set_index(mlast_index);
			self.send(m);
		} else {
			debug!(
				"{} {} [logterm: {}, index: {}] rejected msgApp [logterm: {}, index: {}] from {}",
				self.tag,
				self.id,
				self.raft_log
					.zero_term_on_err_compacted(self.raft_log.term(msg.get_index())),
				msg.get_index(),
				msg.get_log_term(),
				msg.get_index(),
				msg.get_from(),
			);

			let mut m = Message::new();
			m.set_to(msg.get_from());
			m.set_msg_type(MessageType::MsgAppResp);
			m.set_reject(true);
			m.set_index(msg.get_index());
			m.set_reject_hint(self.raft_log.last_index());
			self.send(m);
		}
	}

	pub fn campaign(&mut self, campaign_type: &[u8]) {
		let (term, vote_msg) = if campaign_type == CAMPAIGN_PRE_ELECTION {
			self.become_pre_candidate();
			(self.term + 1, MessageType::MsgPreVote)
		} else {
			self.become_candidate();
			(self.term, MessageType::MsgVote)
		};

		let id = self.id;
		if self.quorum() == self.poll(id, vote_msg_resp_type(vote_msg), true) {
			if campaign_type == CAMPAIGN_PRE_ELECTION {
				self.campaign(CAMPAIGN_ELECTION);
			} else {
				self.become_leader();
			}
			return;
		}

		let self_id = self.id;
		self.get_prs_ids()
			.iter()
			.filter(|&id| *id != self_id)
			.for_each(|&id| {
				info!(
					"{}: id: {}, [logterm: {}, index: {}] sent {:?} request to {} at term {}",
					self.tag,
					self.id,
					self.raft_log.last_term(),
					self.raft_log.last_index(),
					vote_msg,
					id,
					self.term,
				);

				let mut msg = Message::new();
				msg.set_term(term);
				msg.set_to(id);
				msg.set_msg_type(vote_msg);
				msg.set_index(self.raft_log.last_index());
				msg.set_log_term(self.raft_log.last_term());

				if campaign_type == CAMPAIGN_TRANSFER {
					msg.set_context(campaign_type.to_vec());
				}
				self.send(msg);
			});
	}

	fn get_prs_ids(&self) -> Vec<u64> {
		self.prs.keys().cloned().collect()
	}

	fn poll(&mut self, id: u64, t: MessageType, v: bool) -> usize {
		if v {
			info!(
				"{} {} received {:?} from {} at term {}",
				self.tag, self.id, t, id, self.term
			);
		} else {
			info!(
				"{} {} received {:?} rejection from {} at term {}",
				self.tag, self.id, t, id, self.term
			);
		}

		self.votes.entry(id).or_insert(v);
		let count = self.votes.values().filter(|v| **v).count();
		count
	}

	fn quorum(&self) -> usize {
		self.prs.len() / 2 + 1
	}

	// send persists state to stable storage and then sends to its mailbox.
	fn send(&mut self, mut msg: Message) {
		msg.set_from(self.id);

		if msg.get_msg_type() == MessageType::MsgVote
			|| msg.get_msg_type() == MessageType::MsgVoteResp
			|| msg.get_msg_type() == MessageType::MsgPreVote
			|| msg.get_msg_type() == MessageType::MsgPreVoteResp
		{
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
				panic!(
					"term should not be set when sending {:?} (was {})",
					msg.get_msg_type(),
					msg.get_term()
				);
			}

			// do not attach term to MsgProp, MsgReadIndex
			// proposals are a way to forward to the leader and
			// should be treated as local message.
			// MsgReadIndex is also forwarded to leader.
			if msg.get_msg_type() != MessageType::MsgProp
				&& msg.get_msg_type() != MessageType::MsgReadIndex
			{
				msg.set_term(self.term);
			}
		}
		self.msgs.push(msg);
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use std::ops::Deref;
	use std::ops::DerefMut;

	use log_unstable::Unstable;
	use progress::Inflights;
	use storage::MemStorage;

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
				Network::new_with_config(
					vec![None, NOP_STEPPER, NOP_STEPPER, None, None],
					pre_vote,
				),
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

	fn next_ents(sm: &mut StateMachine, s: &mut MemStorage) -> Vec<Entry> {
		let _ = s.append(&sm.raft_log.unstable_entries());
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

		use log_unstable::Unstable;
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

	fn new_test_config(id: u64, peers: Vec<u64>, election: u64, heartbeat: u64) -> Config {
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

	fn new_test_raft<T: Storage>(
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
	struct Connem {
		from: u64,
		to: u64,
	}

	#[derive(Default)]
	struct Network {
		peers: HashMap<u64, StateMachine>,
		storage: HashMap<u64, MemStorage>,
		dropm: HashMap<Connem, f64>,
		ignorem: HashMap<MessageType, bool>,
	}

	const NOP_STEPPER: Option<StateMachine> = Some(StateMachine { raft: None });

	#[derive(Default)]
	struct StateMachine {
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
}
