mod reader;
mod wal;
mod writer;

// #[cfg(all(
//   test,
//   any(
//     all_orderwal_tests,
//     test_swmr_constructor,
//     test_swmr_insert,
//     test_swmr_get,
//     test_swmr_iters,
//   )
// ))]
#[cfg(test)]
mod tests;

/// The ordered write-ahead log without multiple version support.
pub mod base {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};
  use crate::memtable::{
    alternative::Table as BaseAlternativeTable, arena::Table as BaseArenaTable,
    linked::Table as BaseLinkedTable,
  };

  pub use crate::{
    memtable::arena::TableOptions as ArenaTableOptions,
    types::base::{Entry, Key, Value},
    wal::base::{Iter, Keys, RangeKeys, RangeValues, Reader, Writer},
  };

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`linked::Table`](BaseLinkedTable).
  pub type LinkedTable<K, V> = BaseLinkedTable<K, V>;

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`arena::Table`](BaseArenaTable).
  pub type ArenaTable<K, V> = BaseArenaTable<K, V>;

  /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`alternative::Table`](BaseAlternativeTable).
  pub type AlternativeTable<K, V> = BaseAlternativeTable<K, V>;

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
  pub type OrderWal<K, V, M = LinkedTable<K, V>, S = Crc32> = writer::OrderWal<K, V, M, S>;

  /// Immutable reader for the generic ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<K, V, M = LinkedTable<K, V>, S = Crc32> =
    reader::OrderWalReader<K, V, M, S>;
}

/// A multiple version ordered write-ahead log implementation for multiple threads environments.
pub mod multiple_version {
  use dbutils::checksum::Crc32;

  use super::{reader, writer};
  use crate::memtable::{
    alternative::MultipleVersionTable as BaseAlternativeTable,
    arena::MultipleVersionTable as BaseArenaTable, linked::MultipleVersionTable as BaseLinkedTable,
  };

  pub use crate::{
    memtable::arena::TableOptions as ArenaTableOptions,
    types::multiple_version::{Entry, Key, MultipleVersionEntry, Value},
    wal::multiple_version::{
      Iter, Keys, MultipleVersionIter, MultipleVersionRange, RangeKeys, RangeValues, Reader, Writer,
    },
  };

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`linked::MultipleVersionTable`](BaseLinkedTable).
  pub type LinkedTable<K, V> = BaseLinkedTable<K, V>;

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`arena::MultipleVersionTable`](BaseArenaTable).
  pub type ArenaTable<K, V> = BaseArenaTable<K, V>;

  /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`alternative::MultipleVersionTable`](BaseAlternativeTable).
  pub type AlternativeTable<K, V> = BaseAlternativeTable<K, V>;

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
  pub type OrderWal<K, V, M = LinkedTable<K, V>, S = Crc32> = writer::OrderWal<K, V, M, S>;

  /// Immutable reader for the multiple versioned generic ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<K, V, M = LinkedTable<K, V>, S = Crc32> =
    reader::OrderWalReader<K, V, M, S>;
}
