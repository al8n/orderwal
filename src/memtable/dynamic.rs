use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
};

use skl::{Active, MaybeTombstone};

use crate::{memtable::Memtable, State};

/// Bounded memtable implementation based on ARNEA based [`SkipMap`](skl::generic::multiple_version::sync::SkipMap)s.
pub mod bounded;

mod comparator;
mod range_comparator;

pub(crate) use comparator::MemtableComparator;
pub(crate) use range_comparator::MemtableRangeComparator;

/// A memory table which is used to store pointers to the underlying entries.
pub trait DynamicMemtable: Memtable {
  /// The item returned by the iterator or query methods.
  type Entry<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The item returned by the point iterators
  type PointEntry<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The item returned by the bulk deletions iterators
  type RangeDeletionEntry<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The item returned by the bulk updates iterators
  type RangeUpdateEntry<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The iterator type.
  type Iterator<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The range iterator type.
  type Range<'a, S, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
    S: State + 'a;

  /// The iterator over point entries.
  type PointsIterator<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The range iterator over point entries.
  type RangePoints<'a, S, Q, R>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// The iterator over range deletions entries.
  type BulkDeletionsIterator<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The range iterator over range deletions entries.
  type BulkDeletionsRange<'a, S, Q, R>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// The iterator over range updates entries.
  type BulkUpdatesIterator<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The range iterator over range updates entries.
  type BulkUpdatesRange<'a, S, Q, R>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the maximum version of the memtable.
  fn maximum_version(&self) -> u64;

  /// Returns the minimum version of the memtable.
  fn minimum_version(&self) -> u64;

  /// Returns `true` if the memtable may contain an entry whose version is less than or equal to the specified version.
  fn may_contain_version(&self, version: u64) -> bool;

  /// Returns the upper bound of the memtable.
  fn upper_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the lower bound of the memtable.
  fn lower_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the upper bound of the memtable.
  fn upper_bound_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the lower bound of the memtable.
  fn lower_bound_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns the first pointer in the memtable.
  fn first(&self, version: u64) -> Option<Self::Entry<'_, Active>>;

  /// Returns the last pointer in the memtable.
  fn last(&self, version: u64) -> Option<Self::Entry<'_, Active>>;

  /// Returns the first pointer in the memtable.
  fn first_with_tombstone(&self, version: u64) -> Option<Self::Entry<'_, MaybeTombstone>>;

  /// Returns the last pointer in the memtable.
  fn last_with_tombstone(&self, version: u64) -> Option<Self::Entry<'_, MaybeTombstone>>;

  /// Returns the pointer associated with the key.
  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Entry<'_, Active>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.get(version, key).is_some()
  }

  /// Returns the pointer associated with the key.
  fn get_with_tombstone<Q>(&self, version: u64, key: &Q) -> Option<Self::Entry<'_, MaybeTombstone>>
  where
    Q: ?Sized + Borrow<[u8]>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains_with_tombsone<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.get_with_tombstone(version, key).is_some()
  }

  /// Returns an iterator over the memtable.
  fn iter(&self, version: u64) -> Self::Iterator<'_, Active>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over the memtable.
  fn iter_all(&self, version: u64) -> Self::Iterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of the memtable.
  fn range_all<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over point entries in the memtable.
  fn iter_points(&self, version: u64) -> Self::PointsIterator<'_, Active>;

  /// Returns an iterator over all(including all versions and tombstones) the point entries in the memtable.
  fn iter_all_points(&self, version: u64) -> Self::PointsIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of point entries in the memtable.
  fn range_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all(including all versions and tombstones) the point entries in a subset of the memtable.
  fn range_all_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over range deletions entries in the memtable.
  fn iter_bulk_deletions(&self, version: u64) -> Self::BulkDeletionsIterator<'_, Active>;

  /// Returns an iterator over all(including all versions and tombstones) the range deletions entries in the memtable.
  fn iter_all_bulk_deletions(
    &self,
    version: u64,
  ) -> Self::BulkDeletionsIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of range deletions entries in the memtable.
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all(including all versions and tombstones) the range deletions entries in a subset of the memtable.
  fn range_all_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over range updates entries in the memtable.
  fn iter_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, Active>;

  /// Returns an iterator over all(including all versions and tombstones) the range updates entries in the memtable.
  fn iter_all_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of range updates entries in the memtable.
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  /// Returns an iterator over all(including all versions and tombstones) the range updates entries in a subset of the memtable.
  fn range_all_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;
}
