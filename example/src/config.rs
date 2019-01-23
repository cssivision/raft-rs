pub struct Config {
    pub id: u64,
    pub peers: Vec<u64>,
	pub learners: Vec<u64>,
	pub election_tick: u64,
	pub heartbeat_tick: u64,
}