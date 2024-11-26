use {
  super::wal::{KeyPointer, ValuePointer},
  crate::{types::Kind, WithVersion, WithoutVersion},
  core::{ops::{Bound, RangeBounds}, borrow::Borrow},
};

// /// Memtable implementation based on linked based [`SkipMap`][`crossbeam_skiplist`].
// #[cfg(feature = "std")]
// #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
// pub mod linked;

// /// Memtable implementation based on ARNEA based [`SkipMap`](skl).
// pub mod arena;

// /// Sum type for different memtable implementations.
// pub mod alternative;

/// An entry which is stored in the memory table.
pub trait BaseEntry<'a>: Sized {
  /// Returns the key in the entry.
  fn key(&self) -> KeyPointer;

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>;
}

/// An entry which is stored in the memory table.
pub trait MemtableEntry<'a>: BaseEntry<'a> + WithoutVersion {
  /// Returns the value in the entry.
  fn value(&self) -> ValuePointer;
}

/// An entry which is stored in the multiple versioned memory table.
pub trait VersionedMemtableEntry<'a>: BaseEntry<'a> + WithVersion {
  /// Returns the value in the entry.
  fn value(&self) -> Option<ValuePointer>;

  /// Returns the version of the entry if it is versioned.
  fn version(&self) -> u64;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait BaseTable {
  /// The comparator used to compare keys.
  type Comparator; 

  /// The configuration options for the memtable.
  type Options;

  /// The error type may be returned when constructing the memtable.
  type Error;

  /// The item returned by the iterator or query methods.
  type Item<'a>: BaseEntry<'a> + Clone
  where
    Self: 'a;

  /// The iterator type.
  type Iterator<'a>: DoubleEndedIterator<Item = Self::Item<'a>>
  where
    Self: 'a;

  /// The range iterator type.
  type Range<'a, Q, R>: DoubleEndedIterator<Item = Self::Item<'a>>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Creates a new memtable with the specified options.
  fn new(opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized;

  /// Inserts a pointer into the memtable.
  fn insert(
    &self,
    version: Option<u64>,
    kp: KeyPointer,
    vp: ValuePointer,
  ) -> Result<(), Self::Error>;

  /// Removes the pointer associated with the key.
  fn remove(&self, version: Option<u64>, key: KeyPointer) -> Result<(), Self::Error>;

  /// Returns the kind of the memtable.
  fn kind() -> Kind;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait Memtable: BaseTable
where
  for<'a> Self::Item<'a>: MemtableEntry<'a>,
{
  /// Returns the number of entries in the memtable.
  fn len(&self) -> usize;

  /// Returns `true` if the memtable is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Returns the upper bound of the memtable.
  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the lower bound of the memtable.
  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the first pointer in the memtable.
  fn first(&self) -> Option<Self::Item<'_>>;

  /// Returns the last pointer in the memtable.
  fn last(&self) -> Option<Self::Item<'_>>;

  /// Returns the pointer associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over the memtable.
  fn iter(&self) -> Self::Iterator<'_>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait MultipleVersionMemtable: BaseTable
where
  for<'a> Self::Item<'a>: VersionedMemtableEntry<'a>,
{
  /// The item returned by the iterator or query methods.
  type VersionedItem<'a>: VersionedMemtableEntry<'a> + Clone
  where
    Self: 'a;

  /// The iterator type which can yields all the entries in the memtable.
  type IterAll<'a>: DoubleEndedIterator<Item = Self::VersionedItem<'a>>
  where
    Self: 'a;

  /// The range iterator type which can yields all the entries in the memtable.
  type RangeAll<'a, Q, R>: DoubleEndedIterator<Item = Self::VersionedItem<'a>>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the maximum version of the memtable.
  fn maximum_version(&self) -> u64;

  /// Returns the minimum version of the memtable.
  fn minimum_version(&self) -> u64;

  /// Returns `true` if the memtable may contain an entry whose version is less than or equal to the specified version.
  fn may_contain_version(&self, version: u64) -> bool;

  /// Returns the upper bound of the memtable.
  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the upper bound of the memtable.
  fn upper_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the lower bound of the memtable.
  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the lower bound of the memtable.
  fn lower_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the first pointer in the memtable.
  fn first(&self, version: u64) -> Option<Self::Item<'_>>;

  /// Returns the first pointer in the memtable.
  fn first_versioned(&self, version: u64) -> Option<Self::VersionedItem<'_>>;

  /// Returns the last pointer in the memtable.
  fn last(&self, version: u64) -> Option<Self::Item<'_>>;

  /// Returns the last pointer in the memtable.
  fn last_versioned(&self, version: u64) -> Option<Self::VersionedItem<'_>>;

  /// Returns the pointer associated with the key.
  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the pointer associated with the key.
  fn get_versioned<Q>(&self, version: u64, key: &Q) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains_versioned<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over the memtable.
  fn iter(&self, version: u64) -> Self::Iterator<'_>;

  /// Returns an iterator over all the entries in the memtable.
  fn iter_all_versions(&self, version: u64) -> Self::IterAll<'_>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all the entries in a subset of the memtable.
  fn range_all_versions<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangeAll<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;
}
