use thiserror::Error;

pub type Result<T> = std::result::Result<T, VgerError>;

#[derive(Debug, Error)]
pub enum VgerError {
    #[error("storage I/O error: {0}")]
    Storage(#[source] Box<opendal::Error>),

    #[error("repository not found at '{0}'")]
    RepoNotFound(String),

    #[error("repository already exists at '{0}'")]
    RepoAlreadyExists(String),

    #[error("decryption failed: wrong passphrase or corrupted data")]
    DecryptionFailed,

    #[error("key derivation error: {0}")]
    KeyDerivation(String),

    #[error("snapshot not found: '{0}'")]
    SnapshotNotFound(String),

    #[error("snapshot already exists: '{0}'")]
    SnapshotAlreadyExists(String),

    #[error("invalid repository format: {0}")]
    InvalidFormat(String),

    #[error("unknown object type tag: {0}")]
    UnknownObjectType(u8),

    #[error("unknown compression tag: {0}")]
    UnknownCompressionTag(u8),

    #[error("unsupported repository version: {0}")]
    UnsupportedVersion(u32),

    #[error("serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    #[error("deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("unsupported backend: '{0}'")]
    UnsupportedBackend(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("repository is locked by another process (lock: {0})")]
    Locked(String),

    #[error("chunk not found in index: {0}")]
    ChunkNotInIndex(crate::crypto::chunk_id::ChunkId),

    #[error("decompression error: {0}")]
    Decompression(String),

    #[error("hook error: {0}")]
    Hook(String),

    #[error("{0}")]
    Other(String),
}

impl From<opendal::Error> for VgerError {
    fn from(value: opendal::Error) -> Self {
        VgerError::Storage(Box::new(value))
    }
}
