use core::ops::{Bound, ControlFlow, RangeBounds};

use ref_cast::RefCast;
use skl::{
  generic::{multiple_version::Map as _, Type, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone,
};

use crate::{
  memtable::bounded,
  state::State,
  types::{BulkOperation, Generic, Query, Remove, Update},
};

use super::GenericMemtable;

/// Generic multiple version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<K, V, C> = bounded::Table<C, Generic<K, V>>;

/// Entry of the [`Table`].
pub type EntryRef<'a, K, V, S, C> = bounded::EntryRef<'a, S, C, Generic<K, V>>;

/// Point entry of the [`Table`].
pub type PointEntryRef<'a, K, V, S, C> = bounded::PointEntryRef<'a, S, C, Generic<K, V>>;

/// Range entry of the [`Table`].
pub type RangeEntry<'a, K, V, S, O, C> = bounded::RangeEntryRef<'a, S, O, C, Generic<K, V>>;

/// Iterator of the [`Table`].
pub type Iter<'a, K, V, S, C> = bounded::Iter<'a, S, C, Generic<K, V>>;

/// Range iterator of the [`Table`].
pub type Range<'a, K, V, S, Q, R, C> = bounded::Range<'a, S, Q, R, C, Generic<K, V>>;

/// Point iterator of the [`Table`].
pub type IterPoints<'a, K, V, S, C> = bounded::IterPoints<'a, S, C, Generic<K, V>>;

/// Range point iterator of the [`Table`].
pub type RangePoints<'a, K, V, S, Q, R, C> = bounded::RangePoints<'a, S, Q, R, C, Generic<K, V>>;

/// Bulk operations iterator of the [`Table`].
pub type IterBulkOperations<'a, K, V, S, O, C> =
  bounded::IterBulkOperations<'a, S, O, C, Generic<K, V>>;

/// Bulk operations range iterator of the [`Table`].
pub type RangeBulkOperations<'a, K, V, S, O, Q, R, C> =
  bounded::RangeBulkOperations<'a, S, O, Q, R, C, Generic<K, V>>;

impl<K, V, C> GenericMemtable<K, V> for Table<K, V, C>
where
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
  C: 'static,
{
  type Comparator = C;

  type Entry<'a, S>
    = EntryRef<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type PointEntry<'a, S>
    = PointEntryRef<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangeEntry<'a, S, O>
    = RangeEntry<'a, K, V, S, O, C>
  where
    Self: 'a,
    S: State + 'a,
    O: BulkOperation;

  type Iterator<'a, S>
    = Iter<'a, K, V, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type Range<'a, S, Q, R>
    = Range<'a, K, V, S, Q, R, C>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
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
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  type BulkOperationsIterator<'a, S, O>
    = IterBulkOperations<'a, K, V, S, O, C>
  where
    Self: 'a,
    S: State + 'a,
    O: BulkOperation;

  type BulkOperationsRange<'a, S, O, Q, R>
    = RangeBulkOperations<'a, K, V, S, O, Q, R, C>
  where
    Self: 'a,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
    S: State + 'a,
    O: BulkOperation,
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
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
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
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self
      .range::<Q, _>(version, (bound, Bound::Unbounded))
      .next()
  }

  #[inline]
  fn upper_bound_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self
      .range_all::<Q, _>(version, (Bound::Unbounded, bound))
      .next_back()
  }

  #[inline]
  fn lower_bound_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    bound: Bound<&'a Q>,
  ) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    self
      .range_all::<Q, _>(version, (bound, Bound::Unbounded))
      .next()
  }

  #[inline]
  fn first<'a>(&'a self, version: u64) -> Option<Self::Entry<'a, Active>>
  where
    Self::Comparator: TypeRefComparator<'a, K>,
  {
    self.iter(version).next()
  }

  #[inline]
  fn last<'a>(&'a self, version: u64) -> Option<Self::Entry<'a, Active>>
  where
    Self::Comparator: TypeRefComparator<'a, K>,
  {
    self.iter(version).next_back()
  }

  #[inline]
  fn first_with_tombstone<'a>(&'a self, version: u64) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Self::Comparator: TypeRefComparator<'a, K>,
  {
    self.iter_all(version).next()
  }

  #[inline]
  fn last_with_tombstone<'a>(&'a self, version: u64) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Self::Comparator: TypeRefComparator<'a, K>,
  {
    self.iter_all(version).next_back()
  }

  #[inline]
  fn get<'a, Q>(&'a self, version: u64, key: &Q) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    let ent = self.skl.get(version, Query::ref_cast(key))?;
    match self.validate(version, PointEntryRef::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn get_with_tombstone<'a, Q>(
    &'a self,
    version: u64,
    key: &Q,
  ) -> Option<Self::Entry<'a, MaybeTombstone>>
  where
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    let ent = self.skl.get_with_tombstone(version, Query::ref_cast(key))?;
    match self.validate(version, PointEntryRef::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn iter(&self, version: u64) -> Self::Iterator<'_, Active> {
    Iter::new(version, self)
  }

  #[inline]
  fn iter_all(&self, version: u64) -> Self::Iterator<'_, MaybeTombstone> {
    Iter::with_tombstone(version, self)
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    Range::new(version, self, range)
  }

  #[inline]
  fn range_all<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    Range::with_tombstone(version, self, range)
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
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
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
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    RangePoints::new(self.skl.range_all(version, range.into()))
  }

  #[inline]
  fn iter_bulk_removes(&self, version: u64) -> Self::BulkOperationsIterator<'_, Active, Remove> {
    IterBulkOperations::new(self.range_deletions_skl.iter(version))
  }

  #[inline]
  fn iter_all_bulk_removes(
    &self,
    version: u64,
  ) -> Self::BulkOperationsIterator<'_, MaybeTombstone, Remove> {
    IterBulkOperations::new(self.range_deletions_skl.iter_all(version))
  }

  #[inline]
  fn range_bulk_removes<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkOperationsRange<'a, Active, Remove, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    RangeBulkOperations::new(self.range_deletions_skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_bulk_removes<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkOperationsRange<'a, MaybeTombstone, Remove, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    RangeBulkOperations::new(self.range_deletions_skl.range_all(version, range.into()))
  }

  #[inline]
  fn iter_bulk_updates(&self, version: u64) -> Self::BulkOperationsIterator<'_, Active, Update> {
    IterBulkOperations::new(self.range_updates_skl.iter(version))
  }

  #[inline]
  fn iter_all_bulk_updates(
    &self,
    version: u64,
  ) -> Self::BulkOperationsIterator<'_, MaybeTombstone, Update> {
    IterBulkOperations::new(self.range_updates_skl.iter_all(version))
  }

  #[inline]
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkOperationsRange<'a, Active, Update, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    RangeBulkOperations::new(self.range_updates_skl.range(version, range.into()))
  }

  #[inline]
  fn range_all_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkOperationsRange<'a, MaybeTombstone, Update, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
  {
    RangeBulkOperations::new(self.range_updates_skl.range_all(version, range.into()))
  }
}
