use protobuf::Message;
use std::u64;

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