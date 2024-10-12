mod reader;
mod wal;
mod writer;

/// An ordered write-ahead log implementation for multiple threads environments.
pub mod base {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};

  use crate::memtable::linked::LinkedTable as BaseLinkedTable;

  pub use crate::wal::bytes::{
    base::{Reader, Writer},
    pointer::Pointer,
  };

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`crossbeam_skiplist::SkipSet`].
  pub type LinkedTable<C> = BaseLinkedTable<Pointer<C>>;

  /// An ordered write-ahead log implementation for multiple threads environments.
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
  pub type OrderWal<M, C, S = Crc32> = writer::OrderWal<M, C, S>;

  /// Immutable reader for the ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<M, C, S = Crc32> = reader::OrderWalReader<M, C, S>;
}

/// A multiple version ordered write-ahead log implementation for multiple threads environments.
pub mod multiple_version {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};

  use crate::memtable::linked::LinkedTable as BaseLinkedTable;

  pub use crate::wal::bytes::{
    mvcc::{Reader, Writer},
    pointer::VersionPointer,
  };

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`crossbeam_skiplist::SkipSet`].
  pub type LinkedTable<C> = BaseLinkedTable<VersionPointer<C>>;

  /// A multiple versioned ordered write-ahead log implementation for multiple threads environments.
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
  pub type OrderWal<M, C, S = Crc32> = writer::OrderWal<M, C, S>;

  /// Immutable reader for the multiple versioned ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<M, C, S = Crc32> = reader::OrderWalReader<M, C, S>;
}

/// The ordered write-ahead log only supports generic.
pub mod generic {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};
  use crate::memtable::linked::LinkedTable as BaseLinkedTable;

  pub use crate::wal::generic::{
    base::{Reader, Writer},
    GenericPointer,
  };

  /// An memory table for [`GenericOrderWal`] or [`GenericOrderWalReader`] based on [`crossbeam_skiplist::SkipSet`].
  pub type LinkedTable<K, V> = BaseLinkedTable<GenericPointer<K, V>>;

  /// A generic ordered write-ahead log implementation for multiple threads environments.
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
  pub type GenericOrderWal<K, V, M, S = Crc32> = writer::GenericOrderWal<K, V, M, S>;

  /// Immutable reader for the generic ordered write-ahead log [`GenericOrderWal`].
  pub type GenericOrderWalReader<K, V, M, S = Crc32> = reader::GenericOrderWalReader<K, V, M, S>;
}

/// A multiple version ordered write-ahead log implementation for multiple threads environments.
pub mod generic_multiple_version {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};
  use crate::memtable::linked::LinkedTable as BaseLinkedTable;

  pub use crate::wal::generic::{
    mvcc::{Reader, Writer},
    GenericVersionPointer,
  };

  /// An memory table for multiple version [`GenericOrderWal`] or [`GenericOrderWalReader`] based on [`crossbeam_skiplist::SkipSet`].
  pub type LinkedTable<K, V> = BaseLinkedTable<GenericVersionPointer<K, V>>;

  /// A multiple versioned generic ordered write-ahead log implementation for multiple threads environments.
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
  pub type GenericOrderWal<K, V, M, S = Crc32> = writer::GenericOrderWal<K, V, M, S>;

  /// Immutable reader for the multiple versioned generic ordered write-ahead log [`GenericOrderWal`].
  pub type GenericOrderWalReader<K, V, M, S = Crc32> = reader::GenericOrderWalReader<K, V, M, S>;
}
