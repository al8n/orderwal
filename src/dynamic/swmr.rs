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
// mod tests;

/// The ordered write-ahead log without multiple version support.
pub mod unique {
  // #[cfg(feature = "std")]
  // use crate::dynamic::memtable::linked::Table as BaseLinkedTable;

  pub use skl::dynamic::{Ascend, Descend};

  use {
    super::{reader, writer},
    crate::memtable::{
      // alternative::Table as BaseAlternativeTable,
      dynamic::unique,
    },
    dbutils::checksum::Crc32,
  };

  pub use crate::dynamic::{
    // memtable::bounded::TableOptions as BoundedTableOptions,
    wal::unique::{Reader, Writer},
  };

  // /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`linked::Table`](BaseLinkedTable).
  // #[cfg(feature = "std")]
  // #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  // pub type LinkedTable<K, V> = BaseLinkedTable<K, V>;

  // /// An memory table for [`OrderWal`] or [`OrderWalReader`] based on [`alternative::Table`](BaseAlternativeTable).
  // pub type AlternativeTable<K, V> = BaseAlternativeTable<K, V>;

  // /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  // #[cfg(feature = "std")]
  // pub type DefaultMemtable<K, V> = LinkedTable<K, V>;

  /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  // #[cfg(not(feature = "std"))]
  pub type DefaultMemtable = unique::bounded::Table<Ascend>;

  /// A dynamic ordered write-ahead log implementation for multiple threads environments.
  pub type OrderWal<M = DefaultMemtable, S = Crc32> = writer::OrderWal<M, S>;

  /// Immutable reader for the dynamic ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<M = DefaultMemtable, S = Crc32> = reader::OrderWalReader<M, S>;
}

/// A multiple version ordered write-ahead log implementation for multiple threads environments.
pub mod multiple_version {
  // #[cfg(feature = "std")]
  // use crate::dynamic::memtable::linked::MultipleVersionTable as BaseLinkedTable;

  pub use skl::dynamic::{Ascend, Descend};

  use {
    super::{reader, writer},
    crate::memtable::dynamic::{
      // alternative::MultipleVersionTable as BaseAlternativeTable,
      multiple_version,
    },
    dbutils::checksum::Crc32,
  };

  pub use crate::dynamic::{
    // types::multiple_version::{Entry, Key, Value, VersionedEntry},
    wal::multiple_version::{Reader, Writer},
  };

  // /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`linked::MultipleVersionTable`](BaseLinkedTable).
  // #[cfg(feature = "std")]
  // #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  // pub type LinkedTable<K, V> = BaseLinkedTable<K, V>;

  // /// An memory table for multiple version [`OrderWal`] or [`OrderWalReader`] based on [`alternative::MultipleVersionTable`](BaseAlternativeTable).
  // pub type AlternativeTable<K, V> = BaseAlternativeTable<K, V>;

  // / The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  // #[cfg(feature = "std")]
  // pub type DefaultMemtable = BaseBoundedTable;

  /// The default memory table used by [`OrderWal`] or [`OrderWalReader`].
  // #[cfg(not(feature = "std"))]
  pub type DefaultMemtable = multiple_version::bounded::Table<Ascend>;

  /// A multiple versioned dynamic ordered write-ahead log implementation for multiple threads environments.
  pub type OrderWal<M = DefaultMemtable, S = Crc32> = writer::OrderWal<M, S>;

  /// Immutable reader for the multiple versioned dynamic ordered write-ahead log [`OrderWal`].
  pub type OrderWalReader<M = DefaultMemtable, S = Crc32> = reader::OrderWalReader<M, S>;
}
