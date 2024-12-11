use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use ref_cast::RefCast as _;
use skl::{
  dynamic::BytesComparator,
  generic::{unique::Map as _, Comparable, Type, TypeRefComparator, TypeRefQueryComparator},
  Active,
};

use crate::{
  memtable::bounded::unique::{self, *},
  types::{Generic, Query},
  State,
};

use super::GenericMemtable;

/// Dynamic unique version memtable implementation based on ARNEA based [`SkipMap`](skl::generic::unique::sync::SkipMap)s.
pub type Table<K: ?Sized, V: ?Sized, C> = unique::Table<C, Generic<K, V>>;

impl<K, V, C> GenericMemtable<K, V> for Table<K, V, C>
where
  C: 'static,
  K: Type + ?Sized + 'static,
  V: Type + ?Sized + 'static,
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
    Self::Comparator: TypeRefQueryComparator<'a, K, Q>,
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
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized;

  fn get<'a, Q>(&'a self, key: &Q) -> Option<Self::Entry<'a>>
  where
    Q: ?Sized,
  {
    let ent = self.skl.get(Query::ref_cast(key))?;
    match self.validate(PointEntry::new(ent)) {
      ControlFlow::Break(entry) => entry,
      ControlFlow::Continue(_) => None,
    }
  }

  #[inline]
  fn iter(&self) -> Self::Iterator<'_> {
    Iter::new(self)
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,

    Q: ?Sized,
  {
    Range::new(self, range)
  }

  #[inline]
  fn iter_points(&self) -> Self::PointsIterator<'_, skl::Active> {
    IterPoints::new(self.skl.iter())
  }

  #[inline]
  fn iter_points_with_tombstone(&self) -> Self::PointsIterator<'_, skl::MaybeTombstone> {
    IterPoints::new(self.skl.iter_with_tombstone())
  }

  #[inline]
  fn range_points<'a, Q, R>(&'a self, range: R) -> Self::RangePoints<'a, skl::Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,

    Q: ?Sized,
  {
    RangePoints::new(self.skl.range(range))
  }

  #[inline]
  fn range_points_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::RangePoints<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
  {
    RangePoints::new(self.skl.range_with_tombstone(range))
  }

  #[inline]
  fn iter_bulk_deletions(&self) -> Self::BulkDeletionsIterator<'_, skl::Active> {
    IterBulkDeletions::new(self.range_deletions_skl.iter())
  }

  #[inline]
  fn iter_bulk_deletions_with_tombstone(
    &self,
  ) -> Self::BulkDeletionsIterator<'_, skl::MaybeTombstone> {
    IterBulkDeletions::new(self.range_deletions_skl.iter_with_tombstone())
  }

  #[inline]
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, skl::Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(range))
  }

  #[inline]
  fn range_bulk_deletions_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range_with_tombstone(range))
  }

  #[inline]
  fn iter_bulk_updates(&self) -> Self::BulkUpdatesIterator<'_, skl::Active> {
    IterBulkUpdates::new(self.range_updates_skl.iter())
  }

  #[inline]
  fn iter_bulk_updates_with_tombstone(&self) -> Self::BulkUpdatesIterator<'_, skl::MaybeTombstone> {
    IterBulkUpdates::new(self.range_updates_skl.iter_with_tombstone())
  }

  #[inline]
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, skl::Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(range))
  }

  #[inline]
  fn range_bulk_updates_with_tombstone<'a, Q, R>(
    &'a self,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, skl::MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_with_tombstone(range))
  }
}
