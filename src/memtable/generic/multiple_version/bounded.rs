use core::ops::{Bound, ControlFlow, RangeBounds};

use ref_cast::RefCast;
use skl::{
  generic::{multiple_version::Map as _, Type, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone,
};

use crate::{
  memtable::bounded::multiple_version,
  types::{Generic, Query},
  State,
};

use super::GenericMemtable;

/// Generic multiple version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<K, V, C> = multiple_version::Table<C, Generic<K, V>>;

/// Entry of the [`Table`].
pub type Entry<'a, K, V, S, C> = multiple_version::Entry<'a, S, C, Generic<K, V>>;

/// Point entry of the [`Table`].
pub type PointEntry<'a, K, V, S, C> = multiple_version::PointEntry<'a, S, C, Generic<K, V>>;

/// Range deletion entry of the [`Table`].
pub type RangeDeletionEntry<'a, K, V, S, C> =
  multiple_version::RangeDeletionEntry<'a, S, C, Generic<K, V>>;

/// Range update entry of the [`Table`].
pub type RangeUpdateEntry<'a, K, V, S, C> =
  multiple_version::RangeUpdateEntry<'a, S, C, Generic<K, V>>;

/// Iterator of the [`Table`].
pub type Iter<'a, K, V, S, C> = multiple_version::Iter<'a, S, C, Generic<K, V>>;

/// Range iterator of the [`Table`].
pub type Range<'a, K, V, S, Q, R, C> = multiple_version::Range<'a, S, Q, R, C, Generic<K, V>>;

/// Point iterator of the [`Table`].
pub type IterPoints<'a, K, V, S, C> = multiple_version::IterPoints<'a, S, C, Generic<K, V>>;

/// Range point iterator of the [`Table`].
pub type RangePoints<'a, K, V, S, Q, R, C> =
  multiple_version::RangePoints<'a, S, Q, R, C, Generic<K, V>>;

/// Bulk deletions iterator of the [`Table`].
pub type IterBulkDeletions<'a, K, V, S, C> =
  multiple_version::IterBulkDeletions<'a, S, C, Generic<K, V>>;

/// Bulk deletions range iterator of the [`Table`].
pub type RangeBulkDeletions<'a, K, V, S, Q, R, C> =
  multiple_version::RangeBulkDeletions<'a, S, Q, R, C, Generic<K, V>>;

/// Bulk updates iterator of the [`Table`].
pub type IterBulkUpdates<'a, K, V, S, C> =
  multiple_version::IterBulkUpdates<'a, S, C, Generic<K, V>>;

/// Bulk updates range iterator of the [`Table`].
pub type RangeBulkUpdates<'a, K, V, S, Q, R, C> =
  multiple_version::RangeBulkUpdates<'a, S, Q, R, C, Generic<K, V>>;

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
  fn maximum_version(&self) -> u64 {
    self
      .skl
      .maximum_version()
      .max(self.range_deletions_skl.maximum_version())
      .max(self.range_updates_skl.maximum_version())
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    self
      .skl
      .minimum_version()
      .min(self.range_deletions_skl.minimum_version())
      .min(self.range_updates_skl.minimum_version())
  }

  #[inline]
  fn may_contain_version(&self, version: u64) -> bool {
    self.skl.may_contain_version(version)
      || self.range_deletions_skl.may_contain_version(version)
      || self.range_updates_skl.may_contain_version(version)
  }

  #[inline]
  fn upper_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: core::ops::Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self
      .range::<Q, _>(version, (Bound::Unbounded, bound))
      .next_back()
  }

  #[inline]
  fn lower_bound<'a, Q>(
    &'a self,
    version: u64,
    bound: core::ops::Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    self
      .range::<Q, _>(version, (bound, Bound::Unbounded))
      .next()
  }

  #[inline]
  fn first(&self, version: u64) -> Option<Self::Entry<'_, Active>> {
    self.iter(version).next()
  }

  #[inline]
  fn last(&self, version: u64) -> Option<Self::Entry<'_, Active>> {
    self.iter(version).next_back()
  }

  #[inline]
  fn get<'a, Q>(&'a self, version: u64, key: &Q) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    let ent = self.skl.get(version, Query::ref_cast(key))?;
    match self.validate(version, PointEntry::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn iter(&self, version: u64) -> Self::Iterator<'_, Active> {
    Iter::new(version, self)
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    Range::new(version, self, range)
  }

  #[inline]
  fn iter_points(&self, version: u64) -> Self::PointsIterator<'_, Active> {
    IterPoints::new(self.skl.iter(version))
  }

  #[inline]
  fn iter_all_points(&self, version: u64) -> Self::PointsIterator<'_, MaybeTombstone> {
    IterPoints::new(self.skl.iter_all(version))
  }

  #[inline]
  fn range_points<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangePoints::new(self.skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangePoints::new(self.skl.range_all(version, range.into()))
  }

  #[inline]
  fn iter_bulk_deletions(&self, version: u64) -> Self::BulkDeletionsIterator<'_, Active> {
    IterBulkDeletions::new(self.range_deletions_skl.iter(version))
  }

  #[inline]
  fn iter_all_bulk_deletions(
    &self,
    version: u64,
  ) -> Self::BulkDeletionsIterator<'_, MaybeTombstone> {
    IterBulkDeletions::new(self.range_deletions_skl.iter_all(version))
  }

  #[inline]
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range_all(version, range.into()))
  }

  #[inline]
  fn iter_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, Active> {
    IterBulkUpdates::new(self.range_updates_skl.iter(version))
  }

  #[inline]
  fn iter_all_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, MaybeTombstone> {
    IterBulkUpdates::new(self.range_updates_skl.iter_all(version))
  }

  #[inline]
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_all(version, range.into()))
  }
}
