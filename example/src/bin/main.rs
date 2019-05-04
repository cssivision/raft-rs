use example::api;
use crossbeam::channel::unbounded;
use libraft::raft::Config;

fn main() {
    let cfg = Config{
        id: 1,
        ..Default::default()
    };

    let (propc_tx, propc_rx) = unbounded::<Vec<u8>>();
}
