use failure::Error;
use std::result;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum StorageError {
    #[fail(display = "snapshot out of date")]
    SnapshotOutOfDate,

    #[fail(display = "requested index is unavailable due to compaction")]
    ErrCompacted,

    #[fail(display = "requested entry at index is unavailable")]
    ErrUnavailable
}
