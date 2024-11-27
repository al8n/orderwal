mod reader;
mod wal;
mod writer;

#[cfg(all(
  test,
  any(
    all_orderwal_tests,
    test_swmr_constructor,
    test_swmr_insert,
    test_swmr_get,
    test_swmr_iters,
  )
))]
mod tests;

/// The ordered write-ahead log without multiple version support.
pub mod base {
  #[cfg(feature = "std")]
  use crate::generic::memtable::linked::Table as BaseLinkedTable;

  use {
    super::{reader, writer},
    crate::generic::memtable::{
      alternative::Table as BaseAlternativeTable, arena::Table as BaseArenaTable,
    },
    dbutils::checksum::Crc32,
  };

  pub use crate::generic::{
    memtable::arena::TableOptions as ArenaTableOptions,
    types::base::{Entry, Key, Value},
    wal::base::{Iter, Keys, RangeKeys, RangeValues, Reader, Writer},
  };

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`linked::Table`](BaseLinkedTable).
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  pub type LinkedTable<K, V> = BaseLinkedTable<K, V>;

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`arena::Table`](BaseArenaTable).
  pub type ArenaTable<K, V> = BaseArenaTable<K, V>;

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`alternative::Table`](BaseAlternativeTable).
  pub type AlternativeTable<K, V> = BaseAlternativeTable<K, V>;

  /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  #[cfg(feature = "std")]
  pub type DefaultTable<K, V> = LinkedTable<K, V>;

  /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  #[cfg(not(feature = "std"))]
  pub type DefaultTable<K, V> = ArenaTable<K, V>;

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
  pub type OrderWal<K, V, M = DefaultTable<K, V>, S = Crc32> = writer::OrderWal<K, V, M, S>;

  /// Immutable reader for the generic ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<K, V, M = DefaultTable<K, V>, S = Crc32> =
    reader::OrderWalReader<K, V, M, S>;
}

/// A multiple version ordered write-ahead log implementation for multiple threads environments.
pub mod multiple_version {
  #[cfg(feature = "std")]
  use crate::generic::memtable::linked::MultipleVersionTable as BaseLinkedTable;

  use {
    super::{reader, writer},
    crate::generic::memtable::{
      alternative::MultipleVersionTable as BaseAlternativeTable,
      arena::MultipleVersionTable as BaseArenaTable,
    },
    dbutils::checksum::Crc32,
  };

  pub use crate::generic::{
    memtable::arena::TableOptions as ArenaTableOptions,
    types::multiple_version::{Entry, Key, Value, VersionedEntry},
    wal::multiple_version::{
      Iter, IterAll, Keys, MultipleVersionRange, RangeKeys, RangeValues, Reader, Writer,
    },
  };

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`linked::MultipleVersionTable`](BaseLinkedTable).
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  pub type LinkedTable<K, V> = BaseLinkedTable<K, V>;

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`arena::MultipleVersionTable`](BaseArenaTable).
  pub type ArenaTable<K, V> = BaseArenaTable<K, V>;

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`alternative::MultipleVersionTable`](BaseAlternativeTable).
  pub type AlternativeTable<K, V> = BaseAlternativeTable<K, V>;

  /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  #[cfg(feature = "std")]
  pub type DefaultTable<K, V> = LinkedTable<K, V>;

  /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  #[cfg(not(feature = "std"))]
  pub type DefaultTable<K, V> = ArenaTable<K, V>;

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
  pub type OrderWal<K, V, M = DefaultTable<K, V>, S = Crc32> = writer::OrderWal<K, V, M, S>;

  /// Immutable reader for the multiple versioned generic ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<K, V, M = DefaultTable<K, V>, S = Crc32> =
    reader::OrderWalReader<K, V, M, S>;
}
