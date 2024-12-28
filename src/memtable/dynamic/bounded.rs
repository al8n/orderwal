use core::{
  borrow::Borrow,
  ops::{Bound, ControlFlow, RangeBounds},
};

use ref_cast::RefCast as _;
use skl::{dynamic::BytesComparator, generic::multiple_version::Map as _, Active, MaybeTombstone};

use crate::{
  memtable::bounded,
  state::State,
  types::{Dynamic, Query, Remove, Update},
};

use super::DynamicMemtable;

/// Dynamic multiple version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::multiple_version::sync::SkipMap)s.
pub type Table<C> = bounded::Table<C, Dynamic>;

/// Entry of the [`Table`].
pub type EntryRef<'a, S, C> = bounded::EntryRef<'a, S, C, Dynamic>;

/// Point entry of the [`Table`].
pub type PointEntryRef<'a, S, C> = bounded::PointEntryRef<'a, S, C, Dynamic>;

/// Range entry of the [`Table`].
pub type RangeEntryRef<'a, S, O, C> = bounded::RangeEntryRef<'a, S, O, C, Dynamic>;

/// Iterator of the [`Table`].
pub type Iter<'a, S, C> = bounded::Iter<'a, S, C, Dynamic>;

/// Range iterator of the [`Table`].
pub type Range<'a, S, Q, R, C> = bounded::Range<'a, S, Q, R, C, Dynamic>;

/// Point iterator of the [`Table`].
pub type IterPoints<'a, S, C> = bounded::IterPoints<'a, S, C, Dynamic>;

/// Range point iterator of the [`Table`].
pub type RangePoints<'a, S, Q, R, C> = bounded::RangePoints<'a, S, Q, R, C, Dynamic>;

/// Bulk operations iterator of the [`Table`].
pub type IterBulkOperations<'a, S, O, C> = bounded::IterBulkOperations<'a, S, O, C, Dynamic>;

/// Bulk operations range iterator of the [`Table`].
pub type RangeBulkOperations<'a, S, O, Q, R, C> =
  bounded::RangeBulkOperations<'a, S, O, Q, R, C, Dynamic>;

impl<C> DynamicMemtable for Table<C>
where
  C: BytesComparator + 'static,
{
  type Entry<'a, S>
    = EntryRef<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type PointEntry<'a, S>
    = PointEntryRef<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangeEntry<'a, S, O>
    = RangeEntryRef<'a, S, O, C>
  where
    Self: 'a,
    S: State + 'a,
    O: crate::types::BulkOperation;

  type Iterator<'a, S>
    = Iter<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type Range<'a, S, Q, R>
    = Range<'a, S, Q, R, C>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
    S: State + 'a;

  type PointsIterator<'a, S>
    = IterPoints<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangePoints<'a, S, Q, R>
    = RangePoints<'a, S, Q, R, C>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkOperationsIterator<'a, S, O>
    = IterBulkOperations<'a, S, O, C>
  where
    Self: 'a,
    S: State + 'a,
    O: crate::types::BulkOperation;

  type BulkOperationsRange<'a, S, O, Q, R>
    = RangeBulkOperations<'a, S, O, Q, R, C>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
    O: crate::types::BulkOperation;

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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range_all::<Q, _>(version, (bound, Bound::Unbounded))
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
  fn first_with_tombstone(&self, version: u64) -> Option<Self::Entry<'_, MaybeTombstone>> {
    self.iter_all(version).next()
  }

  #[inline]
  fn last_with_tombstone(&self, version: u64) -> Option<Self::Entry<'_, MaybeTombstone>> {
    self.iter_all(version).next_back()
  }

  #[inline]
  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Entry<'_, Active>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    let ent = self.skl.get(version, Query::ref_cast(key))?;
    match self.validate(version, PointEntryRef::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn get_with_tombstone<Q>(&self, version: u64, key: &Q) -> Option<Self::Entry<'_, MaybeTombstone>>
  where
    Q: ?Sized + Borrow<[u8]>,
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
  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    Range::new(version, self, range)
  }

  #[inline]
  fn iter_all(&self, version: u64) -> Self::Iterator<'_, MaybeTombstone> {
    Iter::with_tombstone(version, self)
  }

  #[inline]
  fn range_all<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkOperations::new(self.range_updates_skl.range_all(version, range.into()))
  }
}
