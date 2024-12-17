use core::ops::{Bound, ControlFlow, RangeBounds};

use ref_cast::RefCast as _;
use skl::{
  generic::{unique::Map as _, Type, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone,
};

use crate::{
  memtable::bounded::unique,
  types::{Generic, Query},
  State,
};

use super::GenericMemtable;

/// Generic unique version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<K, V, C> = unique::Table<C, Generic<K, V>>;

/// Entry of the [`Table`].
pub type Entry<'a, K, V, S, C> = unique::Entry<'a, S, C, Generic<K, V>>;

/// Point entry of the [`Table`].
pub type PointEntry<'a, K, V, S, C> = unique::PointEntry<'a, S, C, Generic<K, V>>;

/// Range deletion entry of the [`Table`].
pub type RangeDeletionEntry<'a, K, V, S, C> = unique::RangeDeletionEntry<'a, S, C, Generic<K, V>>;

/// Range update entry of the [`Table`].
pub type RangeUpdateEntry<'a, K, V, S, C> = unique::RangeUpdateEntry<'a, S, C, Generic<K, V>>;

/// Iterator of the [`Table`].
pub type Iter<'a, K, V, S, C> = unique::Iter<'a, S, C, Generic<K, V>>;

/// Range iterator of the [`Table`].
pub type Range<'a, K, V, S, Q, R, C> = unique::Range<'a, S, Q, R, C, Generic<K, V>>;

/// Point iterator of the [`Table`].
pub type IterPoints<'a, K, V, S, C> = unique::IterPoints<'a, S, C, Generic<K, V>>;

/// Range point iterator of the [`Table`].
pub type RangePoints<'a, K, V, S, Q, R, C> = unique::RangePoints<'a, S, Q, R, C, Generic<K, V>>;

/// Bulk deletions iterator of the [`Table`].
pub type IterBulkDeletions<'a, K, V, S, C> = unique::IterBulkDeletions<'a, S, C, Generic<K, V>>;

/// Bulk deletions range iterator of the [`Table`].
pub type RangeBulkDeletions<'a, K, V, S, Q, R, C> =
  unique::RangeBulkDeletions<'a, S, Q, R, C, Generic<K, V>>;

/// Bulk updates iterator of the [`Table`].
pub type IterBulkUpdates<'a, K, V, S, C> = unique::IterBulkUpdates<'a, S, C, Generic<K, V>>;

/// Bulk updates range iterator of the [`Table`].
pub type RangeBulkUpdates<'a, K, V, S, Q, R, C> =
  unique::RangeBulkUpdates<'a, S, Q, R, C, Generic<K, V>>;

impl<K, V, C> GenericMemtable<K, V> for Table<K, V, C>
where
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
  C: TypeRefComparator<K> + 'static,
{
  type Comparator = C;

  type Entry<'a, S>
    = Entry<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type PointEntry<'a, S>
    = PointEntry<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangeDeletionEntry<'a, S>
    = RangeDeletionEntry<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangeUpdateEntry<'a, S>
    = RangeUpdateEntry<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type Iterator<'a, S>
    = Iter<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type Range<'a, S, Q, R>
    = Range<'a, K, V, S, Q, R, C>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    S: State + 'a;

  type PointsIterator<'a, S>
    = IterPoints<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangePoints<'a, S, Q, R>
    = RangePoints<'a, K, V, S, Q, R, C>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  type BulkDeletionsIterator<'a, S>
    = IterBulkDeletions<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type BulkDeletionsRange<'a, S, Q, R>
    = RangeBulkDeletions<'a, K, V, S, Q, R, C>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  type BulkUpdatesIterator<'a, S>
    = IterBulkUpdates<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type BulkUpdatesRange<'a, S, Q, R>
    = RangeBulkUpdates<'a, K, V, S, Q, R, C>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State + 'a,

    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  #[inline]
  fn upper_bound<'a, Q>(&'a self, bound: core::ops::Bound<&'a Q>) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.range::<Q, _>((Bound::Unbounded, bound)).next_back()
  }

  #[inline]
  fn lower_bound<'a, Q>(&'a self, bound: core::ops::Bound<&'a Q>) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self.range::<Q, _>((bound, Bound::Unbounded)).next()
  }

  #[inline]
  fn first(&self) -> Option<Self::Entry<'_, Active>> {
    self.iter().next()
  }

  #[inline]
  fn last(&self) -> Option<Self::Entry<'_, Active>> {
    self.iter().next_back()
  }

  #[inline]
  fn get<'a, Q>(&'a self, key: &Q) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    let ent = self.skl.get(Query::ref_cast(key))?;
    match self.validate(PointEntry::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn iter(&self) -> Self::Iterator<'_, Active> {
    Iter::new(self)
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    Range::new(self, range)
  }

  #[inline]
  fn iter_points(&self) -> Self::PointsIterator<'_, Active> {
    IterPoints::new(self.skl.iter())
  }

  #[inline]
  fn iter_points_with_tombstone(&self) -> Self::PointsIterator<'_, MaybeTombstone> {
    IterPoints::new(self.skl.iter_with_tombstone())
  }

  #[inline]
  fn range_points<'a, Q, R>(&'a self, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangePoints::new(self.skl.range(range.into()))
  }

  #[inline]
  fn range_points_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangePoints::new(self.skl.range_with_tombstone(range.into()))
  }

  #[inline]
  fn iter_bulk_deletions(&self) -> Self::BulkDeletionsIterator<'_, Active> {
    IterBulkDeletions::new(self.range_deletions_skl.iter())
  }

  #[inline]
  fn iter_bulk_deletions_with_tombstone(&self) -> Self::BulkDeletionsIterator<'_, MaybeTombstone> {
    IterBulkDeletions::new(self.range_deletions_skl.iter_with_tombstone())
  }

  #[inline]
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(range.into()))
  }

  #[inline]
  fn range_bulk_deletions_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range_with_tombstone(range.into()))
  }

  #[inline]
  fn iter_bulk_updates(&self) -> Self::BulkUpdatesIterator<'_, Active> {
    IterBulkUpdates::new(self.range_updates_skl.iter())
  }

  #[inline]
  fn iter_bulk_updates_with_tombstone(&self) -> Self::BulkUpdatesIterator<'_, MaybeTombstone> {
    IterBulkUpdates::new(self.range_updates_skl.iter_with_tombstone())
  }

  #[inline]
  fn range_bulk_updates<'a, Q, R>(&'a self, range: R) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(range.into()))
  }

  #[inline]
  fn range_bulk_updates_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_with_tombstone(range.into()))
  }
}
