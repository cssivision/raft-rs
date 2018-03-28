#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

#[macro_use]
extern crate log;

extern crate protobuf;
#[macro_use]
extern crate failure;
#[macro_use] 
extern crate failure_derive;

mod raftpb;
mod storage;
mod errors;
mod raft;
mod raft_log;
mod log_unstable;
mod progress;