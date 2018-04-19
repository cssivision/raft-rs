use std::u64;

use raftpb::{Entry, EntryType, MessageType};

use protobuf::Message;

pub const NO_LIMIT: u64 = u64::MAX;

pub fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: u64) {
    if max == NO_LIMIT || entries.len() <= 1 {
        return;
    }

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += u64::from(Message::compute_size(e));
                true
            } else {
                size += u64::from(Message::compute_size(e));
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}

pub fn num_of_pending_conf(ents: &[Entry]) -> u64 {
    ents.into_iter()
        .filter(|e| e.get_entry_type() == EntryType::EntryConfChange)
        .count() as u64
}

pub fn vote_msg_resp_type(t: MessageType) -> MessageType {
    match t {
        MessageType::MsgVote => MessageType::MsgVoteResp,
        MessageType::MsgPreVote => MessageType::MsgPreVoteResp,
        _ => panic!("not a vote message: {:?}", t),
    }
}

pub fn is_local_msg(msgt: MessageType) -> bool {
    msgt == MessageType::MsgHup || msgt == MessageType::MsgBeat
        || msgt == MessageType::MsgUnreachable || msgt == MessageType::MsgSnapStatus
        || msgt == MessageType::MsgCheckQuorum
}

pub fn is_response_msg(msgt: MessageType) -> bool {
    msgt == MessageType::MsgAppResp || msgt == MessageType::MsgVoteResp
        || msgt == MessageType::MsgHeartbeatResp || msgt == MessageType::MsgPreVoteResp
        || msgt == MessageType::MsgUnreachable
}
