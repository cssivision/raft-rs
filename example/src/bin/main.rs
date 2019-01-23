use clap::{App, Arg};
use example::api;
use std::sync::mpsc::channel;

fn main() {
    let (proposal_c_tx, proposal_c_rx) = channel::<Vec<u8>>();
    println!("Hello, world!");
}
