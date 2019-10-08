use errors::{Error, Result};
use progress::Progress;
use raft::{Config, Peer, Raft, StateType, Status, NONE};
use raftpb::{
    ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot,
};
use read_only::ReadState;
use storage::Storage;
use util::{is_empty_snap, is_local_msg, is_response_msg};

use protobuf::{self, RepeatedField};

#[derive(PartialEq, Debug)]
pub enum SnapshotStatus {
    Finish,
    Failure,
}

/// SoftState provides state that is useful for logging and debugging.
/// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq, Debug, Clone)]
pub struct SoftState {
    pub lead: u64,
    pub raft_state: StateType,
}

pub struct RawNode<T: Storage> {
    pub raft: Raft<T>,
    pub pre_soft_state: SoftState,
    pub pre_hard_state: HardState,
}

/// Ready encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed or sent to other peers.
/// All fields in Ready are read-only.
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Ready {
    /// The current volatile state of a Node.
    /// SoftState will be nil if there is no update.
    /// It is not required to consume or store SoftState.
    pub soft_state: Option<SoftState>,

    /// The current state of a Node to be saved to stable storage BEFORE
    /// Messages are sent.
    /// HardState will be equal to empty state if there is no update.
    pub hard_state: HardState,

    /// read_states can be used for node to serve linearizable read requests locally
    /// when its applied index is greater than the index in ReadState.
    /// Note that the readState will be returned when raft receives msgReadIndex.
    /// The returned is only valid for the request that requested to read.
    pub read_states: Vec<ReadState>,

    /// entries specifies entries to be saved to stable storage BEFORE
    /// Messages are sent.
    pub entries: Vec<Entry>,

    /// snapshot specifies the snapshot to be saved to stable storage.
    pub snapshot: Snapshot,

    /// committed_entries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    pub committed_entries: Vec<Entry>,

    /// messages specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    pub messages: Vec<Message>,

    /// must_sync indicates whether the HardState and Entries must be synchronously
    /// written to disk or if an asynchronous write is permissible.
    pub must_sync: bool,
}

impl Ready {
    fn new<T: Storage>(
        r: &Raft<T>,
        prev_soft_state: &SoftState,
        prev_hard_state: &HardState,
    ) -> Ready {
        let mut rd = Ready {
            entries: r.raft_log.unstable_entries(),
            committed_entries: r.raft_log.next_ents(),
            messages: r.msgs.clone(),
            ..Default::default()
        };
        let ss = r.soft_state();
        if &ss != prev_soft_state {
            rd.soft_state = Some(ss);
        }
        let hs = r.hard_state();
        if &hs != prev_hard_state {
            rd.hard_state = hs;
        }
        if let Some(ref s) = r.raft_log.unstable.snapshot {
            rd.snapshot = s.clone();
        }
        if !r.read_states.is_empty() {
            rd.read_states = r.read_states.clone();
        }

        rd.must_sync = !rd.entries.is_empty()
            || rd.hard_state.get_vote() != prev_hard_state.get_vote()
            || rd.hard_state.get_term() != prev_hard_state.get_term();
        rd
    }
}

impl<T: Storage> RawNode<T> {
    pub fn new(c: &mut Config, storage: T, mut peers: Vec<Peer>) -> Result<RawNode<T>> {
        if c.id == 0 {
            panic!("config id must not be zero");
        }
        let r = Raft::new(c, storage);
        let mut rn = RawNode {
            raft: r,
            pre_soft_state: Default::default(),
            pre_hard_state: Default::default(),
        };

        let last_index = rn.raft.raft_log.get_storage().last_index().unwrap();

        // If the log is empty, this is a new RawNode; otherwise it's
        // restoring an existing RawNode.
        if last_index == 0 {
            rn.raft.become_follower(1, NONE);
            let mut ents: Vec<Entry> = Vec::with_capacity(peers.len());
            for (i, peer) in peers.iter_mut().enumerate() {
                let mut cc = ConfChange::new();
                cc.set_change_type(ConfChangeType::ConfChangeAddNode);
                cc.set_node_id(peer.id);
                cc.set_context(peer.context.to_vec());
                let data =
                    protobuf::Message::write_to_bytes(&cc).expect("unexpected marshal error");
                let mut ent = Entry::new();
                ent.set_entry_type(EntryType::EntryConfChange);
                ent.set_term(1);
                ent.set_data(data);
                ent.set_index(i as u64 + 1);
                ents.push(ent);
            }
            rn.raft.raft_log.append(&ents);
            rn.raft.raft_log.committed = ents.len() as u64;

            for peer in peers {
                rn.raft.add_node(peer.id);
            }
        }
        rn.pre_soft_state = rn.raft.soft_state();
        if last_index == 0 {
            rn.pre_hard_state = HardState::new();
        } else {
            rn.pre_hard_state = rn.raft.hard_state();
        }
        Ok(rn)
    }

    // tick advances the internal logical clock by a single tick.
    pub fn tick(&mut self) {
        self.raft.tick();
    }

    // propose proposes data be appended to the raft log.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<()> {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgProp);
        m.set_from(self.raft.id);
        let mut e = Entry::new();
        e.set_data(data);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m)
    }

    // propose_conf_change proposes a config change.
    pub fn propose_conf_change(&mut self, cc: &ConfChange) -> Result<()> {
        let data = protobuf::Message::write_to_bytes(cc)?;
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgProp);
        let mut e = Entry::new();
        e.set_entry_type(EntryType::EntryConfChange);
        e.set_data(data);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m)
    }

    pub fn step(&mut self, msg: Message) -> Result<()> {
        if is_local_msg(msg.get_msg_type()) {
            return Err(Error::StepLocalMsg);
        }

        if is_response_msg(msg.get_msg_type()) && self.raft.get_progress(msg.get_from()).is_none() {
            return Err(Error::StepPeerNotFound);
        }

        self.raft.step(msg)
    }

    pub fn ready(&self) -> Ready {
        Ready::new(&self.raft, &self.pre_soft_state, &self.pre_hard_state)
    }

    pub fn advance(&mut self, rd: Ready) {
        self.commit_ready(rd);
    }

    fn commit_ready(&mut self, rd: Ready) {
        if let Some(ss) = rd.soft_state {
            self.pre_soft_state = ss;
        }
        if rd.hard_state != HardState::new() {
            self.pre_hard_state = rd.hard_state;
        }

        if self.pre_hard_state.get_commit() != 0 {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            self.raft.raft_log.applied_to(self.pre_hard_state.commit);
        }

        if !rd.entries.is_empty() {
            let e = &rd.entries[rd.entries.len() - 1];
            self.raft.raft_log.stable_to(e.get_index(), e.get_term());
        }
        if is_empty_snap(&rd.snapshot) {
            self.raft
                .raft_log
                .stable_snap_to(rd.snapshot.get_metadata().get_index());
        }
        if !rd.read_states.is_empty() {
            self.raft.read_states.clear();
        }
    }

    /// read_index requests a read state. The read state will be set in ready.
    /// Read State has a read index. Once the application advances further than the read
    /// index, any linearizable read requests issued before the read request can be
    /// processed safely. The read state will have the same rctx attached.
    pub fn read_index(&mut self, rctx: Vec<u8>) {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut e = Entry::new();
        e.set_data(rctx);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        let _ = self.raft.step(m);
    }

    // apply_conf_change applies a config change to the local node.
    pub fn apply_conf_change(&mut self, cc: &ConfChange) -> ConfState {
        if cc.get_node_id() == NONE {
            let mut cs = ConfState::new();
            cs.set_nodes(self.raft.nodes());
            cs.set_learners(self.raft.learner_nodes());
            return cs;
        }

        match cc.get_change_type() {
            ConfChangeType::ConfChangeAddNode => {
                self.raft.add_node(cc.get_node_id());
            }
            ConfChangeType::ConfChangeAddLearnerNode => {
                self.raft.add_learner(cc.get_node_id());
            }
            ConfChangeType::ConfChangeRemoveNode => {
                self.raft.remove_node(cc.get_node_id());
            }
            ConfChangeType::ConfChangeUpdateNode => {}
        }

        let mut cs = ConfState::new();
        cs.set_nodes(self.raft.nodes());
        cs.set_learners(self.raft.learner_nodes());
        cs
    }

    /// Campaign causes this RawNode to transition to candidate state.
    pub fn campaign(&mut self) -> Result<()> {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgHup);
        self.raft.step(m)
    }

    /// HasReady called when RawNode user need to check if any Ready pending.
    pub fn has_ready(&self) -> bool {
        if self.raft.soft_state() != self.pre_soft_state {
            return true;
        }

        if self.raft.hard_state() != HardState::new()
            && self.raft.hard_state() != self.pre_hard_state
        {
            return true;
        }

        if self.raft.raft_log.unstable.snapshot.as_ref().is_some()
            && self.raft.raft_log.unstable.snapshot.as_ref().unwrap() != &Snapshot::new()
        {
            return true;
        }

        if !self.raft.msgs.is_empty()
            || !self.raft.raft_log.unstable_entries().is_empty()
            || self.raft.raft_log.has_next_ents()
        {
            return true;
        }

        if !self.raft.read_states.is_empty() {
            return true;
        }

        false
    }

    /// report_unreachable reports the given node is not reachable for the last send.
    pub fn report_unreachable(&mut self, id: u64) {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgUnreachable);
        m.set_from(id);
        let _ = self.raft.step(m).is_ok();
    }

    /// report_snapshot reports the status of the sent snapshot.
    pub fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let rej = status == SnapshotStatus::Failure;

        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgSnapStatus);
        m.set_from(id);
        m.set_reject(rej);
        let _ = self.raft.step(m).is_ok();
    }

    /// transfer_leader tries to transfer leadership to the given transferee.
    pub fn transfer_leader(&mut self, transferee: u64) {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgTransferLeader);
        m.set_from(transferee);
        let _ = self.raft.step(m);
    }

    /// status returns the current status of the given group.
    pub fn status(&self) -> Status {
        self.raft.get_status()
    }
}
