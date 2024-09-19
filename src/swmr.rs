/// The ordered write-ahead log only supports bytes.
pub mod wal;
pub use wal::{Builder, OrderWal};

/// The generic implementation of the ordered write-ahead log.
pub mod generic;
pub use generic::{GenericBuilder, GenericOrderWal};
