use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesRangeComparator, Active};

use crate::{
  memtable::dynamic::{MemtableEntry as _, RangeEntry as _, RangeUpdateEntry as _},
  State, WithVersion as _,
};

use super::DynamicMemtable;

pub use entry::*;
pub use iter::*;
pub use point::*;
pub use range_deletion::*;
pub use range_update::*;

mod entry;
mod iter;
mod point;
mod range_deletion;
mod range_update;

dynamic_memtable!(multiple_version(version));

impl<C> DynamicMemtable for Table<C>
where
  C: BytesComparator + 'static,
{
  type Entry<'a>
    = Entry<'a, Active, C>
  where
    Self: 'a;

  type PointEntry<'a, S>
    = PointEntry<'a, S, C>
  where
    Self: 'a,
    S: State<'a>;

  type RangeDeletionEntry<'a, S>
    = RangeDeletionEntry<'a, S, C>
  where
    Self: 'a,
    S: State<'a>;

  type RangeUpdateEntry<'a, S>
    = RangeUpdateEntry<'a, S, C>
  where
    Self: 'a,
    S: State<'a>;

  type Iterator<'a>
    = Iter<'a, C>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Q, R, C>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type PointsIterator<'a, S>
    = IterPoints<'a, S, C>
  where
    Self: 'a,
    S: State<'a>;

  type RangePoints<'a, S, Q, R>
    = RangePoints<'a, S, Q, R, C>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkDeletionsIterator<'a, S>
    = IterBulkDeletions<'a, S, C>
  where
    Self: 'a,
    S: State<'a>;

  type BulkDeletionsRange<'a, S, Q, R>
    = RangeBulkDeletions<'a, S, Q, R, C>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkUpdatesIterator<'a, S>
    = IterBulkUpdates<'a, S, C>
  where
    Self: 'a,
    S: State<'a>;

  type BulkUpdatesRange<'a, S, Q, R>
    = RangeBulkUpdates<'a, S, Q, R, C>
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

impl<C> Table<C>
where
  C: BytesComparator + 'static,
{
  fn validate<'a>(
    &'a self,
    query_version: u64,
    ent: PointEntry<'a, Active, C>,
  ) -> ControlFlow<Option<Entry<'a, Active, C>>, PointEntry<'a, Active, C>> {
    let key = ent.key();
    let version = ent.version();

    // check if the next entry is visible.
    // As the range_del_skl is sorted by the end key, we can use the lower_bound to find the first
    // deletion range that may cover the next entry.

    let shadow = self
      .range_deletions_skl
      .range(query_version, ..=key)
      .any(|ent| {
        let del_ent_version = ent.version();
        if !(version <= del_ent_version && del_ent_version <= query_version) {
          return false;
        }

        let ent = RangeDeletionEntry::new(ent);
        BytesRangeComparator::compare_contains(&self.cmp, &ent.range(), key)
      });

    if shadow {
      return ControlFlow::Continue(ent);
    }

    // find the range key entry with maximum version that shadow the next entry.
    let range_ent = self
      .range_updates_skl
      .range_all(query_version, ..=key)
      .filter_map(|ent| {
        let range_ent_version = ent.version();
        if !(version <= range_ent_version && range_ent_version <= query_version) {
          return None;
        }

        let ent = RangeUpdateEntry::new(ent);
        if BytesRangeComparator::compare_contains(&self.cmp, &ent.range(), key) {
          Some(ent)
        } else {
          None
        }
      })
      .max_by_key(|e| e.version());

    // check if the next entry's value should be shadowed by the range key entries.
    if let Some(range_ent) = range_ent {
      if let Some(val) = range_ent.value() {
        return ControlFlow::Break(Some(Entry::new(
          self,
          query_version,
          ent,
          key,
          val,
          range_ent.version(),
        )));
      }

      // if value is None, the such range is unset, so we should return the value of the point entry.
    }

    let val = ent.value();
    let version = ent.version();
    ControlFlow::Break(Some(Entry::new(
      self,
      query_version,
      ent,
      key,
      val,
      version,
    )))
  }
}
