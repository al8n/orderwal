mod reader;
mod wal;
mod writer;

/// The ordered write-ahead log only supports generic.
pub mod generic {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};
  use crate::memtable::{
    arena::ArenaTable as BaseArenaTable, linked::LinkedTable as BaseLinkedTable,
  };

  pub use crate::wal::{
    base::{Reader, Writer},
    GenericPointer,
  };

  /// An memory table for [`GenericOrderWal`] or [`GenericOrderWalReader`] based on [`LinkedTable`](BaseLinkedTable).
  pub type LinkedTable<K, V> = BaseLinkedTable<GenericPointer<K, V>>;

  /// An memory table for [`GenericOrderWal`] or [`GenericOrderWalReader`] based on [`ArenaTable`](BaseArenaTable).
  pub type ArenaTable<K, V> = BaseArenaTable<GenericPointer<K, V>>;

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
  use crate::memtable::{
    arena::VersionedArenaTable as BaseArenaTable, linked::LinkedTable as BaseLinkedTable,
  };

  pub use crate::wal::{
    multiple_version::{Reader, Writer},
    GenericVersionPointer,
  };

  /// An memory table for multiple version [`GenericOrderWal`] or [`GenericOrderWalReader`] based on [`LinkedTable`](BaseLinkedTable).
  pub type LinkedTable<K, V> = BaseLinkedTable<GenericVersionPointer<K, V>>;

  /// An memory table for multiple version [`GenericOrderWal`] or [`GenericOrderWalReader`] based on [`VersionedArenaTable`](BaseArenaTable).
  pub type ArenaTable<K, V> = BaseArenaTable<GenericVersionPointer<K, V>>;

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
