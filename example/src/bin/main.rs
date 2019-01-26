use clap::{App, Arg};
use example::api;
use std::sync::mpsc::channel;

fn main() {
    let (propc_tx, propc_rx) = channel::<Vec<u8>>();
    println!("Hello, world!");
}
