// /// The ordered write-ahead log only supports bytes.
// pub mod wal;
// pub use wal::{Builder, OrderWal};

// /// The generic implementation of the ordered write-ahead log.
// pub mod generic;
// pub use generic::{GenericBuilder, GenericOrderWal};

mod c;
mod wal;


/// An ordered write-ahead log implementation for single thread environments.
pub mod base {
  use crate::pointer::Pointer;

  use super::wal;

  pub use crate::base::{Writer, Reader};

  /// An ordered write-ahead log implementation for single thread environments.
  ///
  /// ```text
  /// +----------------------+-------------------------+--------------------+
  /// | magic text (6 bytes) | magic version (2 bytes) |  header (8 bytes)  |
  /// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
  /// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
  /// +----------------------+-------------------------+--------------------+---------------------+-----------------|--------------------+
  /// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
  /// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
  /// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
  /// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
  /// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
  /// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
  /// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
  /// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
  /// ```
  pub type OrderWal<C, S> =  wal::OrderWal<Pointer<C>, C, S>;
}


/// A multiple version ordered write-ahead log implementation for single thread environments.
pub mod mvcc {
  use crate::pointer::MvccPointer;

  use super::wal;

  pub use crate::mvcc::{Writer, Reader};

  /// A multiple versioned ordered write-ahead log implementation for single thread environments.
  ///
  /// ```text
  /// +----------------------+-------------------------+--------------------+
  /// | magic text (6 bytes) | magic version (2 bytes) |  header (8 bytes)  |
  /// +----------------------+-------------------------+--------------------+---------------------+---------------------+-----------------+--------------------+
  /// |     flag (1 byte)    |    version (8 bytes)    |  key len (4 bytes) |    key (n bytes)    | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
  /// +----------------------+-------------------------+--------------------+---------------------+---------------------+-----------------+--------------------+
  /// |     flag (1 byte)    |    version (8 bytes)    |  key len (4 bytes) |    key (n bytes)    | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
  /// +----------------------+-------------------------+--------------------+---------------------+---------------------+-----------------+--------------------+
  /// |     flag (1 byte)    |    version (8 bytes)    |  key len (4 bytes) |    key (n bytes)    | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
  /// +----------------------+-------------------------+--------------------+---------------------+---------------------+-----------------+--------------------+
  /// |         ...          |            ...          |         ...        |          ...        |        ...          |         ...     |        ,,,         |
  /// +----------------------+-------------------------+--------------------+---------------------+---------------------+-----------------+--------------------+
  /// ```
  pub type OrderWal<C, S> =  wal::OrderWal<MvccPointer<C>, C, S>;
}

