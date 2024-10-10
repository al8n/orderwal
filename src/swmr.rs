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
  use core::ops::Bound;

  use dbutils::checksum::Crc32;

  use crate::pointer::Pointer;

  use super::wal;

  pub use crate::base::{Reader, Writer};

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
  pub type OrderWal<C, S = Crc32> = wal::OrderWal<Pointer<C>, C, S>;

  #[test]
  fn test_() {
    let wal: OrderWal<dbutils::Ascend> = todo!();
    let start: &[u8] = &[1, 2, 3];
    let end: &[u8] = &[4, 5, 6];

    wal.range::<[u8], _>(3, (Bound::Included(start), Bound::Excluded(end)));
  }
}

/// A multiple version ordered write-ahead log implementation for single thread environments.
pub mod mvcc {
  use core::ops::Bound;

  use dbutils::checksum::Crc32;

  use crate::pointer::MvccPointer;

  use super::wal;

  pub use crate::mvcc::{Reader, Writer};

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
  pub type OrderWal<C, S = Crc32> = wal::OrderWal<MvccPointer<C>, C, S>;

  #[test]
  fn test_() {
    let wal: OrderWal<dbutils::Ascend> = todo!();
    let start: &[u8] = &[1, 2, 3];
    let end: &[u8] = &[4, 5, 6];

    wal.range::<[u8], _>(3, (Bound::Included(start), Bound::Excluded(end)));
  }
}
