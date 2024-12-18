use core::{
  borrow::Borrow,
  ops::{Bound, ControlFlow, RangeBounds},
};

use ref_cast::RefCast as _;
use skl::{dynamic::BytesComparator, generic::unique::Map as _, Active, MaybeTombstone};

use crate::{
  memtable::bounded::unique,
  types::{Dynamic, Query},
  State,
};

use super::DynamicMemtable;

/// Dynamic unique version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<C> = unique::Table<C, Dynamic>;

/// Entry of the [`Table`].
pub type Entry<'a, S, C> = unique::Entry<'a, S, C, Dynamic>;

/// Point entry of the [`Table`].
pub type PointEntry<'a, S, C> = unique::PointEntry<'a, S, C, Dynamic>;

/// Range deletion entry of the [`Table`].
pub type RangeDeletionEntry<'a, S, C> = unique::RangeDeletionEntry<'a, S, C, Dynamic>;

/// Range update entry of the [`Table`].
pub type RangeUpdateEntry<'a, S, C> = unique::RangeUpdateEntry<'a, S, C, Dynamic>;

/// Iterator of the [`Table`].
pub type Iter<'a, S, C> = unique::Iter<'a, S, C, Dynamic>;

/// Range iterator of the [`Table`].
pub type Range<'a, S, Q, R, C> = unique::Range<'a, S, Q, R, C, Dynamic>;

/// Point iterator of the [`Table`].
pub type IterPoints<'a, S, C> = unique::IterPoints<'a, S, C, Dynamic>;

/// Range point iterator of the [`Table`].
pub type RangePoints<'a, S, Q, R, C> = unique::RangePoints<'a, S, Q, R, C, Dynamic>;

/// Bulk deletions iterator of the [`Table`].
pub type IterBulkDeletions<'a, S, C> = unique::IterBulkDeletions<'a, S, C, Dynamic>;

/// Bulk deletions range iterator of the [`Table`].
pub type RangeBulkDeletions<'a, S, Q, R, C> = unique::RangeBulkDeletions<'a, S, Q, R, C, Dynamic>;

/// Bulk updates iterator of the [`Table`].
pub type IterBulkUpdates<'a, S, C> = unique::IterBulkUpdates<'a, S, C, Dynamic>;

/// Bulk updates range iterator of the [`Table`].
pub type RangeBulkUpdates<'a, S, Q, R, C> = unique::RangeBulkUpdates<'a, S, Q, R, C, Dynamic>;

impl<C> DynamicMemtable for Table<C>
where
  C: BytesComparator + 'static,
{
  type Entry<'a, S>
    = Entry<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type PointEntry<'a, S>
    = PointEntry<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangeDeletionEntry<'a, S>
    = RangeDeletionEntry<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type RangeUpdateEntry<'a, S>
    = RangeUpdateEntry<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

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

  type BulkDeletionsIterator<'a, S>
    = IterBulkDeletions<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type BulkDeletionsRange<'a, S, Q, R>
    = RangeBulkDeletions<'a, S, Q, R, C>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkUpdatesIterator<'a, S>
    = IterBulkUpdates<'a, S, C>
  where
    Self: 'a,
    S: State + 'a;

  type BulkUpdatesRange<'a, S, Q, R>
    = RangeBulkUpdates<'a, S, Q, R, C>
  where
    Self: 'a,
    S: State + 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  #[inline]
  fn upper_bound<'a, Q>(&'a self, bound: core::ops::Bound<&'a Q>) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range::<Q, _>((Bound::Unbounded, bound))
      .next_back()
  }

  #[inline]
  fn lower_bound<'a, Q>(&'a self, bound: core::ops::Bound<&'a Q>) -> Option<Self::Entry<'a, Active>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self
      .range::<Q, _>((bound, Bound::Unbounded))
      .next()
  }

  #[inline]
  fn first(&self) -> Option<Self::Entry<'_, Active>> {
    self.iter().next()
  }

  #[inline]
  fn last(&self) -> Option<Self::Entry<'_, Active>> {
    self.iter().next_back()
  }

  fn get<Q>(&self, key: &Q) -> Option<Self::Entry<'_, Active>>
  where
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
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
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_with_tombstone(range.into()))
  }
}
