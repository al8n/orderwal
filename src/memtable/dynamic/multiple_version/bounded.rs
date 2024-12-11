use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesComparator, generic::multiple_version::Map as _, Active};

use crate::{
  memtable::bounded::multiple_version::{self, *},
  types::Dynamic,
  State, WithVersion,
};

use super::DynamicMemtable;

/// Dynamic multiple version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<C> = multiple_version::Table<C, Dynamic>;

impl<C> DynamicMemtable for Table<C>
where
  C: BytesComparator + 'static,
  for<'a> PointEntry<'a, Active, C, Dynamic>: WithVersion,
{
  type Entry<'a>
    = Entry<'a, Active, C, Dynamic>
  where
    Self: 'a;

  type PointEntry<'a, S>
    = PointEntry<'a, S, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>;

  type RangeDeletionEntry<'a, S>
    = RangeDeletionEntry<'a, S, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>;

  type RangeUpdateEntry<'a, S>
    = RangeUpdateEntry<'a, S, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>;

  type Iterator<'a>
    = Iter<'a, C, Dynamic>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Q, R, C, Dynamic>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type PointsIterator<'a, S>
    = IterPoints<'a, S, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>;

  type RangePoints<'a, S, Q, R>
    = RangePoints<'a, S, Q, R, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkDeletionsIterator<'a, S>
    = IterBulkDeletions<'a, S, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>;

  type BulkDeletionsRange<'a, S, Q, R>
    = RangeBulkDeletions<'a, S, Q, R, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkUpdatesIterator<'a, S>
    = IterBulkUpdates<'a, S, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>;

  type BulkUpdatesRange<'a, S, Q, R>
    = RangeBulkUpdates<'a, S, Q, R, C, Dynamic>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

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

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Entry<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    let ent = self.skl.get(version, key)?;
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
    Q: ?Sized + Borrow<[u8]>,
  {
    Range::new(version, self, range)
  }

  #[inline]
  fn iter_points(&self, version: u64) -> Self::PointsIterator<'_, skl::Active> {
    IterPoints::new(self.skl.iter(version))
  }

  #[inline]
  fn iter_all_points(&self, version: u64) -> Self::PointsIterator<'_, skl::MaybeTombstone> {
    IterPoints::new(self.skl.iter_all(version))
  }

  #[inline]
  fn range_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, skl::Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangePoints::new(self.skl.range(version, range))
  }

  #[inline]
  fn range_all_points<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangePoints::new(self.skl.range_all(version, range))
  }

  #[inline]
  fn iter_bulk_deletions(&self, version: u64) -> Self::BulkDeletionsIterator<'_, skl::Active> {
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
  ) -> Self::BulkDeletionsRange<'a, skl::Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(version, range))
  }

  #[inline]
  fn range_all_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range_all(version, range))
  }

  #[inline]
  fn iter_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, skl::Active> {
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
  ) -> Self::BulkUpdatesRange<'a, skl::Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(version, range))
  }

  #[inline]
  fn range_all_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_all(version, range))
  }
}
