use std::error;
use std::{cmp, io, result};

use protobuf::ProtobufError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Storage(err: StorageError) {
            from()
            cause(err)
            description(err.description())
        }
        StepLocalMsg {
            description("raft: cannot step raft local message")
        }
        StepPeerNotFound {
            description("raft: cannot step as peer not found")
        }
        ProposalDropped {
            description("raft: proposal dropped")
        }
        ConfigInvalid(desc: String) {
            description(desc)
        }
        Codec(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("protobuf error {:?}", err)
        }

    }
}

impl cmp::PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::StepPeerNotFound, &Error::StepPeerNotFound) => true,
            (&Error::ProposalDropped, &Error::ProposalDropped) => true,
            (&Error::Storage(ref e1), &Error::Storage(ref e2)) => e1 == e2,
            (&Error::Io(ref e1), &Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (&Error::StepLocalMsg, &Error::StepLocalMsg) => true,
            (&Error::ConfigInvalid(ref e1), &Error::ConfigInvalid(ref e2)) => e1 == e2,
            _ => false,
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum StorageError {
        Compacted {
            description("log compacted")
        }
        Unavailable {
            description("log unavailable")
        }
        SnapshotOutOfDate {
            description("snapshot out of date")
        }
        SnapshotTemporarilyUnavailable {
            description("snapshot is temporarily unavailable")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl cmp::PartialEq for StorageError {
    fn eq(&self, other: &StorageError) -> bool {
        match (self, other) {
            (&StorageError::Compacted, &StorageError::Compacted) => true,
            (&StorageError::Unavailable, &StorageError::Unavailable) => true,
            (&StorageError::SnapshotOutOfDate, &StorageError::SnapshotOutOfDate) => true,
            (
                &StorageError::SnapshotTemporarilyUnavailable,
                &StorageError::SnapshotTemporarilyUnavailable,
            ) => true,
            _ => false,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
