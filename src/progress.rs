use std::cmp;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ProgressState {
    Probe,
    Replicate,
    Snapshot,
}

impl Default for ProgressState {
    fn default() -> ProgressState {
        ProgressState::Probe
    }
}

#[derive(Debug, Default)]
pub struct Progress {
    pub matched: u64,
    pub next: u64,
    pub state: ProgressState,
    pub paused: bool,
    pub pending_snapshot: u64,
    pub recent_active: bool,
    pub ins: Inflights,
    pub is_lenarner: bool,
}

impl Progress {
    pub fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.ins.reset();
    }

    pub fn become_probe(&mut self) {
        // If the original state is ProgressStateSnapshot, progress knows that
	    // the pending snapshot has been sent to this peer successfully, then
	    // probes from pendingSnapshot + 1.
        if self.state == ProgressState::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next = cmp::max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.reset_state(ProgressState::Probe);
            self.next = self.matched + 1;
        }
    }

    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next = self.matched + 1;
    }

    pub fn become_snapshot(&mut self, index: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = index;
    }
}

#[derive(Debug, Default)]
pub struct Inflights {
    pub start: usize,
    pub count: usize,
    pub size: usize,
    pub buffer: Vec<u64>,
}

impl Inflights {
    fn reset(&mut self) {
        self.start = 0;
        self.count = 0;
    }
}