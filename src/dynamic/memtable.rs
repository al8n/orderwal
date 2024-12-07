use super::types::{Active, MaybeTombstone, State};
use crate::types::{Kind, RecordPointer};
use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

// /// Memtable implementation based on linked based [`SkipMap`][`crossbeam_skiplist`].
// #[cfg(feature = "std")]
// #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
// pub mod linked;

// /// Memtable implementation based on ARNEA based [`SkipMap`](skl).
// pub mod arena;

// /// Sum type for different memtable implementations.
// pub mod alternative;

/// Memtable implementation based on ARNEA based [`SkipMap`](skl).
pub mod bounded;

/// An entry which is stored in the memory table.
pub trait MemtableEntry<'a>
where
  Self: Sized,
{
  /// The value type.
  type Value;

  /// Returns the key in the entry.
  fn key(&self) -> &'a [u8];

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value;

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>;
}

/// An range entry which is stored in the memory table.
pub trait RangeEntry<'a>: Sized {
  /// Returns the start bound of the range entry.
  fn start_bound(&self) -> Bound<&'a [u8]>;

  /// Returns the end bound of the range entry.
  fn end_bound(&self) -> Bound<&'a [u8]>;

  /// Returns the range of the entry.
  fn range(&self) -> impl RangeBounds<[u8]> + 'a {
    (self.start_bound(), self.end_bound())
  }

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>;
}

/// An entry which is stored in the memory table.
pub trait RangeDeletionEntry<'a>: RangeEntry<'a> {}

/// An entry which is stored in the memory table.
pub trait RangeUpdateEntry<'a>
where
  Self: RangeEntry<'a>,
{
  /// The value type.
  type Value;

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait BaseTable {
  /// The configuration options for the memtable.
  type Options;

  /// The error type may be returned when constructing the memtable.
  type Error;

  /// The item returned by the iterator or query methods.
  type Entry<'a>
  where
    Self: 'a;

  /// The item returned by the point iterators
  type PointEntry<'a, S>
  where
    Self: 'a,
    S: State<'a>;

  /// The item returned by the bulk deletions iterators
  type RangeDeletionEntry<'a, S>
  where
    Self: 'a,
    S: State<'a>;

  /// The item returned by the bulk updates iterators
  type RangeUpdateEntry<'a, S>
  where
    Self: 'a,
    S: State<'a>;

  /// The iterator type.
  type Iterator<'a>: DoubleEndedIterator<Item = Self::Entry<'a>>
  where
    Self: 'a;

  /// The range iterator type.
  type Range<'a, Q, R>: DoubleEndedIterator<Item = Self::Entry<'a>>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// The iterator over point entries.
  type PointsIterator<'a, S>: DoubleEndedIterator<Item = Self::PointEntry<'a, S>>
  where
    Self: 'a,
    S: State<'a>;

  /// The range iterator over point entries.
  type RangePoints<'a, S, Q, R>: DoubleEndedIterator<Item = Self::PointEntry<'a, S>>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// The iterator over range deletions entries.
  type BulkDeletionsIterator<'a, S>: DoubleEndedIterator<Item = Self::RangeDeletionEntry<'a, S>>
  where
    Self: 'a,
    S: State<'a>;

  /// The range iterator over range deletions entries.
  type BulkDeletionsRange<'a, S, Q, R>: DoubleEndedIterator<Item = Self::RangeDeletionEntry<'a, S>>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// The iterator over range updates entries.
  type BulkUpdatesIterator<'a, S>: DoubleEndedIterator<Item = Self::RangeUpdateEntry<'a, S>>
  where
    Self: 'a,
    S: State<'a>;

  /// The range iterator over range updates entries.
  type BulkUpdatesRange<'a, S, Q, R>: DoubleEndedIterator<Item = Self::RangeUpdateEntry<'a, S>>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Creates a new memtable with the specified options.
  fn new<A>(arena: A, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
    A: rarena_allocator::Allocator;

  /// Inserts a pointer into the memtable.
  fn insert(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Removes the pointer associated with the key.
  fn remove(&self, version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts a range deletion pointer into the memtable, a range deletion is a deletion of a range of keys,
  /// which means that keys in the range are marked as deleted.
  ///
  /// This is not a contra operation to [`range_set`](MultipleVersionMemtable::range_set).
  /// See also [`range_set`](MultipleVersionMemtable::range_set) and [`range_set`](MultipleVersionMemtable::range_unset).
  fn range_remove(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts an range update pointer into the memtable.
  fn range_set(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Unset a range from the memtable, this is a contra operation to [`range_set`](MultipleVersionMemtable::range_set).
  fn range_unset(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Returns the kind of the memtable.
  fn kind() -> Kind;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait Memtable: BaseTable {
  /// Returns the number of entries in the memtable.
  fn len(&self) -> usize;

  /// Returns `true` if the memtable is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Returns the upper bound of the memtable.
  fn upper_bound<'a, Q>(&'a self, bound: Bound<&'a Q>) -> Option<Self::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range::<Q, _>((Bound::Unbounded, bound))
      .next_back()
  }

  /// Returns the lower bound of the memtable.
  fn lower_bound<'a, Q>(&'a self, bound: Bound<&'a Q>) -> Option<Self::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range::<Q, _>((bound, Bound::Unbounded))
      .next()
  }

  /// Returns the first pointer in the memtable.
  fn first(&self) -> Option<Self::Entry<'_>> {
    self.iter().next()
  }

  /// Returns the last pointer in the memtable.
  fn last(&self) -> Option<Self::Entry<'_>> {
    self.iter().next_back()
  }

  /// Returns the pointer associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<Self::Entry<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>
  {
    self.get(key).is_some()
  }

  /// Returns an iterator over the memtable.
  fn iter(&self) -> Self::Iterator<'_>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;
  
  /// Returns an iterator over point entries in the memtable.
  fn point_iter(&self) -> Self::PointsIterator<'_, Active>;

  /// Returns an iterator over a subset of point entries in the memtable.
  fn point_range<'a, Q, R>(&'a self, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over range deletions entries in the memtable.
  fn bulk_deletions_iter(&self) -> Self::BulkDeletionsIterator<'_, Active>;

  /// Returns an iterator over a subset of range deletions entries in the memtable.
  fn bulk_deletions_range<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over range updates entries in the memtable.
  fn bulk_updates_iter(&self) -> Self::BulkUpdatesIterator<'_, Active>;

  /// Returns an iterator over a subset of range updates entries in the memtable.
  fn bulk_updates_range<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait MultipleVersionMemtable: BaseTable {
  /// Returns the maximum version of the memtable.
  fn maximum_version(&self) -> u64;

  /// Returns the minimum version of the memtable.
  fn minimum_version(&self) -> u64;

  /// Returns `true` if the memtable may contain an entry whose version is less than or equal to the specified version.
  fn may_contain_version(&self, version: u64) -> bool;

  /// Returns the upper bound of the memtable.
  fn upper_bound<'a, Q>(&'a self, version: u64, bound: Bound<&'a Q>) -> Option<Self::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range::<Q, _>(version, (Bound::Unbounded, bound))
      .next_back()
  }

  /// Returns the lower bound of the memtable.
  fn lower_bound<'a, Q>(&'a self, version: u64, bound: Bound<&'a Q>) -> Option<Self::Entry<'a>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range::<Q, _>(version, (bound, Bound::Unbounded))
      .next()
  }

  /// Returns the first pointer in the memtable.
  fn first(&self, version: u64) -> Option<Self::Entry<'_>> {
    self.iter(version).next()
  }

  /// Returns the last pointer in the memtable.
  fn last(&self, version: u64) -> Option<Self::Entry<'_>> {
    self.iter(version).next_back()
  }

  /// Returns the pointer associated with the key.
  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Entry<'_>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.get(version, key).is_some()
  }

  /// Returns an iterator over the memtable.
  fn iter(&self, version: u64) -> Self::Iterator<'_>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over point entries in the memtable.
  fn point_iter(&self, version: u64) -> Self::PointsIterator<'_, Active>;

  /// Returns an iterator over all the point entries in the memtable.
  fn point_iter_with_tombstone(&self, version: u64) -> Self::PointsIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of point entries in the memtable.
  fn point_range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all the point entries in a subset of the memtable.
  fn point_range_with_tombstone<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over range deletions entries in the memtable.
  fn bulk_deletions_iter(&self, version: u64) -> Self::BulkDeletionsIterator<'_, Active>;

  /// Returns an iterator over all the range deletions entries in the memtable.
  fn bulk_deletions_iter_with_tombstone(
    &self,
    version: u64,
  ) -> Self::BulkDeletionsIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of range deletions entries in the memtable.
  fn bulk_deletions_range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all the range deletions entries in a subset of the memtable.
  fn bulk_deletions_range_with_tombstone<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over range updates entries in the memtable.
  fn bulk_updates_iter(&self, version: u64) -> Self::BulkUpdatesIterator<'_, Active>;

  /// Returns an iterator over all the range updates entries in the memtable.
  fn bulk_updates_iter_with_tombstone(
    &self,
    version: u64,
  ) -> Self::BulkUpdatesIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of range updates entries in the memtable.
  fn bulk_updates_range<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all the range updates entries in a subset of the memtable.
  fn bulk_updates_range_with_tombstone<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;
}
