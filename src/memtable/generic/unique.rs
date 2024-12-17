use core::ops::{Bound, RangeBounds};

use skl::{
  generic::{Type, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone,
};

use crate::{memtable::Memtable, State};

/// Bounded memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub mod bounded;

/// A memory table which is used to store pointers to the underlying entries.
pub trait GenericMemtable<K, V>
where
  Self: Memtable,
  K: Type + ?Sized,
  V: Type + ?Sized,
{
  /// The comparator used for key comparison.
  type Comparator: TypeRefComparator<K>;

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
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
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
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State + 'a,

    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  /// The iterator over range deletions entries.
  type BulkDeletionsIterator<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The range iterator over range deletions entries.
  type BulkDeletionsRange<'a, S, Q, R>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State + 'a,

    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  /// The iterator over range updates entries.
  type BulkUpdatesIterator<'a, S>
  where
    Self: 'a,
    S: State + 'a;

  /// The range iterator over range updates entries.
  type BulkUpdatesRange<'a, S, Q, R>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  /// Returns the upper bound of the memtable.
  fn upper_bound<'a, Q>(&'a self, bound: Bound<&'a Q>) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns the lower bound of the memtable.
  fn lower_bound<'a, Q>(&'a self, bound: Bound<&'a Q>) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns the first pointer in the memtable.
  fn first(&self) -> Option<Self::Entry<'_, Active>>;

  /// Returns the last pointer in the memtable.
  fn last(&self) -> Option<Self::Entry<'_, Active>>;

  /// Returns the pointer associated with the key.
  fn get<'a, Q>(&'a self, key: &Q) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.get(key).is_some()
  }

  /// Returns an iterator over the memtable.
  fn iter(&self) -> Self::Iterator<'_, Active>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns an iterator over point entries in the memtable.
  fn iter_points(&self) -> Self::PointsIterator<'_, Active>;

  /// Returns an iterator over all the point entries in the memtable.
  fn iter_points_with_tombstone(&self) -> Self::PointsIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of point entries in the memtable.
  fn range_points<'a, Q, R>(&'a self, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns an iterator over all the point entries in a subset of the memtable.
  fn range_points_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns an iterator over range deletions entries in the memtable.
  fn iter_bulk_deletions(&self) -> Self::BulkDeletionsIterator<'_, Active>;

  /// Returns an iterator over all the range deletions entries in the memtable.
  fn iter_bulk_deletions_with_tombstone(&self) -> Self::BulkDeletionsIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of range deletions entries in the memtable.
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns an iterator over all the range deletions entries in a subset of the memtable.
  fn range_bulk_deletions_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns an iterator over range updates entries in the memtable.
  fn iter_bulk_updates(&self) -> Self::BulkUpdatesIterator<'_, Active>;

  /// Returns an iterator over all the range updates entries in the memtable.
  fn iter_bulk_updates_with_tombstone(&self) -> Self::BulkUpdatesIterator<'_, MaybeTombstone>;

  /// Returns an iterator over a subset of range updates entries in the memtable.
  fn range_bulk_updates<'a, Q, R>(&'a self, range: R) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;

  /// Returns an iterator over all the range updates entries in a subset of the memtable.
  fn range_bulk_updates_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>;
}
