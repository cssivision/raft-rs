use storage::Storage;

#[derive(Default)]
pub struct raftLog<T: Storage> {
    storage: T,
}