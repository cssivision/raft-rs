use std::u64;

use raftpb::{Entry, EntryType, MessageType, Snapshot};

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
    msgt == MessageType::MsgHup
        || msgt == MessageType::MsgBeat
        || msgt == MessageType::MsgUnreachable
        || msgt == MessageType::MsgSnapStatus
        || msgt == MessageType::MsgCheckQuorum
}

pub fn is_response_msg(msgt: MessageType) -> bool {
    msgt == MessageType::MsgAppResp
        || msgt == MessageType::MsgVoteResp
        || msgt == MessageType::MsgHeartbeatResp
        || msgt == MessageType::MsgPreVoteResp
        || msgt == MessageType::MsgUnreachable
}

pub fn is_empty_snap(snap: Snapshot) -> bool {
    snap.get_metadata().get_index() == 0 
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_local_msg() {
        let tests = vec![
            (MessageType::MsgHup, true),
            (MessageType::MsgBeat, true),
            (MessageType::MsgUnreachable, true),
            (MessageType::MsgSnapStatus, true),
            (MessageType::MsgCheckQuorum, true),
            (MessageType::MsgTransferLeader, false),
            (MessageType::MsgProp, false),
            (MessageType::MsgApp, false),
            (MessageType::MsgAppResp, false),
            (MessageType::MsgVote, false),
            (MessageType::MsgVoteResp, false),
            (MessageType::MsgSnap, false),
            (MessageType::MsgHeartbeat, false),
            (MessageType::MsgHeartbeatResp, false),
            (MessageType::MsgTimeoutNow, false),
            (MessageType::MsgReadIndex, false),
            (MessageType::MsgReadIndexResp, false),
            (MessageType::MsgPreVote, false),
            (MessageType::MsgPreVoteResp, false),
        ];

        for (msgt, is_local) in tests {
            assert_eq!(is_local_msg(msgt), is_local);
        }
    }

    #[test]
    fn test_is_response_msg() {
        let tests = vec![
            (MessageType::MsgHeartbeatResp, true),
            (MessageType::MsgUnreachable, true),
            (MessageType::MsgVoteResp, true),
            (MessageType::MsgPreVoteResp, true),
            (MessageType::MsgAppResp, true),
            (MessageType::MsgHup, false),
            (MessageType::MsgBeat, false),
            (MessageType::MsgSnapStatus, false),
            (MessageType::MsgCheckQuorum, false),
            (MessageType::MsgTransferLeader, false),
            (MessageType::MsgProp, false),
            (MessageType::MsgApp, false),
            (MessageType::MsgVote, false),
            (MessageType::MsgSnap, false),
            (MessageType::MsgHeartbeat, false),
            (MessageType::MsgTimeoutNow, false),
            (MessageType::MsgReadIndex, false),
            (MessageType::MsgReadIndexResp, false),
            (MessageType::MsgPreVote, false),
        ];

        for (msgt, is_local) in tests {
            assert_eq!(is_response_msg(msgt), is_local);
        }
    }

    #[test]
    fn test_vote_msg_resp_type() {
        assert_eq!(
            vote_msg_resp_type(MessageType::MsgVote),
            MessageType::MsgVoteResp
        );

        assert_eq!(
            vote_msg_resp_type(MessageType::MsgPreVote),
            MessageType::MsgPreVoteResp
        );
    }

    #[test]
    fn test_limit_size() {
        let ents = vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        let tests = vec![
            (
                u64::MAX,
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)],
            ),
            (0, vec![new_entry(4, 4)]),
            (
                Message::compute_size(&ents[0]) as u64 + Message::compute_size(&ents[1]) as u64,
                vec![new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                Message::compute_size(&ents[0]) as u64
                    + Message::compute_size(&ents[1]) as u64
                    + Message::compute_size(&ents[2]) as u64 - 1,
                vec![new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                Message::compute_size(&ents[0]) as u64 + Message::compute_size(&ents[1]) as u64 - 1,
                vec![new_entry(4, 4)],
            ),
        ];

        for (max_size, wents) in tests {
            let mut ents = vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
            limit_size(&mut ents, max_size);
            assert_eq!(ents, wents);
        }
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        e
    }
}
