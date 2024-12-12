use core::ops::{ControlFlow, RangeBounds};

use ref_cast::RefCast;
use skl::{
  generic::{multiple_version::Map as _, Type, TypeRefComparator, TypeRefQueryComparator},
  Active,
};

use crate::{
  memtable::bounded::multiple_version::{self, *},
  types::{Generic, Query},
  State,
};

use super::GenericMemtable;

/// Generic multiple version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<K, V, C> = multiple_version::Table<C, Generic<K, V>>;

impl<K, V, C> GenericMemtable<K, V> for Table<K, V, C>
where
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
  C: TypeRefComparator<K> + 'static,
{
  type Comparator = C;

  type Entry<'a>
    = Entry<'a, Active, C, Generic<K, V>>
  where
    Self: 'a;

  type PointEntry<'a, S>
    = PointEntry<'a, S, C, Generic<K, V>>
  where
    Self: 'a,
    S: State<'a>;

  type RangeDeletionEntry<'a, S>
    = RangeDeletionEntry<'a, S, C, Generic<K, V>>
  where
    Self: 'a,
    S: State<'a>;

  type RangeUpdateEntry<'a, S>
    = RangeUpdateEntry<'a, S, C, Generic<K, V>>
  where
    Self: 'a,
    S: State<'a>;

  type Iterator<'a>
    = Iter<'a, C, Generic<K, V>>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Q, R, C, Generic<K, V>>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  type PointsIterator<'a, S>
    = IterPoints<'a, S, C, Generic<K, V>>
  where
    Self: 'a,
    S: State<'a>;

  type RangePoints<'a, S, Q, R>
    = RangePoints<'a, S, Q, R, C, Generic<K, V>>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  type BulkDeletionsIterator<'a, S>
    = IterBulkDeletions<'a, S, C, Generic<K, V>>
  where
    Self: 'a,
    S: State<'a>;

  type BulkDeletionsRange<'a, S, Q, R>
    = RangeBulkDeletions<'a, S, Q, R, C, Generic<K, V>>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  type BulkUpdatesIterator<'a, S>
    = IterBulkUpdates<'a, S, C, Generic<K, V>>
  where
    Self: 'a,
    S: State<'a>;

  type BulkUpdatesRange<'a, S, Q, R>
    = RangeBulkUpdates<'a, S, Q, R, C, Generic<K, V>>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<K, Q>,
    S: State<'a>,
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
  fn get<'a, Q>(&'a self, version: u64, key: &Q) -> Option<Self::Entry<'a>>
  where
    Q: ?Sized,
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    let ent = self.skl.get(version, Query::ref_cast(key))?;
    match self.validate(version, PointEntry::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    Iter::new(version, self)
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    Range::new(version, self, range)
  }

  #[inline]
  fn iter_points(&self, version: u64) -> Self::PointsIterator<'_, Active> {
    IterPoints::new(self.skl.iter(version))
  }

  #[inline]
  fn iter_all_points(&self, version: u64) -> Self::PointsIterator<'_, skl::MaybeTombstone> {
    IterPoints::new(self.skl.iter_all(version))
  }

  #[inline]
  fn range_points<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    RangePoints::new(self.skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
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
  ) -> Self::BulkDeletionsIterator<'_, skl::MaybeTombstone> {
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
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range_all(version, range.into()))
  }

  #[inline]
  fn iter_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, Active> {
    IterBulkUpdates::new(self.range_updates_skl.iter(version))
  }

  #[inline]
  fn iter_all_bulk_updates(
    &self,
    version: u64,
  ) -> Self::BulkUpdatesIterator<'_, skl::MaybeTombstone> {
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
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: skl::generic::TypeRefQueryComparator<K, Q>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_all(version, range.into()))
  }
}
