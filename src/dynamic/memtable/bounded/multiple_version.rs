use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use super::{MemtableComparator, MemtableRangeComparator, TableOptions};

use crate::{
  dynamic::{
    memtable::{
      BaseTable, MemtableEntry, MultipleVersionMemtable, RangeEntry, RangeUpdateEntry as _,
    },
    types::State,
  },
  types::{Kind, RecordPointer},
  WithVersion,
};
use among::Among;
use triomphe::Arc;

pub use entry::*;
pub use iter::*;
pub use point::*;
pub use range_deletion::*;
pub use range_update::*;

use skl::{
  dynamic::{Ascend, BytesComparator, BytesRangeComparator},
  either::Either,
  generic::{
    multiple_version::{sync::SkipMap as GenericSkipMap, Map as GenericMap},
    Builder,
  },
  Active, Arena as _, MaybeTombstone, Options,
};

macro_rules! range_entry_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident => $raw:ident.$fetch:ident) $(::$version:ident)?
  ) => {
    $(#[$meta])*
    pub struct $ent<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::MemtableRangeComparator<C>>,
      data: core::cell::OnceCell<$crate::types::$raw<'a>>,
    }

    impl<'a, S, C> Clone for $ent<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      #[inline]
      fn clone(&self) -> Self {
        Self {
          ent: self.ent.clone(),
          data: self.data.clone(),
        }
      }
    }

    impl<'a, S, C> $ent<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::MemtableRangeComparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, S, C> $crate::dynamic::memtable::RangeEntry<'a> for $ent<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,
    {
      #[inline]
      fn start_bound(&self) -> core::ops::Bound<&'a [u8]> {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.start_bound()
      }

      #[inline]
      fn end_bound(&self) -> core::ops::Bound<&'a [u8]> {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.end_bound()
      }

      #[inline]
      fn next(&mut self) -> Option<Self> {
        self.ent.next().map(Self::new)
      }

      #[inline]
      fn prev(&mut self) -> Option<Self> {
        self.ent.prev().map(Self::new)
      }
    }

    $(
      impl<'a, S, C> $crate::WithVersion for $ent<'a, S, C>
      where
        C: dbutils::equivalentor::BytesComparator,
        S: $crate::dynamic::types::State<'a>,
      {
        #[inline]
        fn $version(&self) -> u64 {
          self.ent.$version()
        }
      }
    )?
  };
}

macro_rules! range_deletion_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    range_entry_wrapper! {
      $(#[$meta])*
      $ent($inner => RawRangeDeletionRef.fetch_range_deletion) $(::$version)?
    }

    impl<'a, S, C> crate::dynamic::memtable::RangeDeletionEntry<'a>
      for $ent<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,
    {
    }
  };
}

macro_rules! range_update_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    range_entry_wrapper! {
      $(#[$meta])*
      $ent($inner => RawRangeUpdateRef.fetch_range_update) $(::$version)?
    }

    impl<'a, C> crate::dynamic::memtable::RangeUpdateEntry<'a>
      for $ent<'a, $crate::dynamic::types::MaybeTombstone, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Value = Option<&'a [u8]>;

      #[inline]
      fn value(&self) -> Self::Value {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
        ent.value()
      }
    }

    impl<'a, C> crate::dynamic::memtable::RangeUpdateEntry<'a>
      for $ent<'a, $crate::dynamic::types::Active, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Value = &'a [u8];

      #[inline]
      fn value(&self) -> Self::Value {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
        ent.value().expect("entry in Active state must have a value")
      }
    }
  };
}

macro_rules! iter_wrapper {
  (
    $(#[$meta:meta])*
    $iter:ident($inner:ident) yield $ent:ident by $cmp:ident
  ) => {
    $(#[$meta])*
    pub struct $iter<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      iter: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::$cmp<C>>,
    }

    impl<'a, S, C> $iter<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      #[inline]
      pub(super) const fn new(iter: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::$cmp<C>>) -> Self {
        Self { iter }
      }
    }

    impl<'a, S, C> Iterator for $iter<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,

    {
      type Item = $ent<'a, S, C>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map($ent::new)
      }
    }

    impl<'a, S, C> DoubleEndedIterator for $iter<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,

    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map($ent::new)
      }
    }
  };
}

macro_rules! range_wrapper {
  (
    $(#[$meta:meta])*
    $iter:ident($inner:ident) yield $ent:ident by $cmp:ident
  ) => {
    $(#[$meta])*
    pub struct $iter<'a, S, Q, R, C>
    where
      S: $crate::dynamic::types::State<'a>,
      Q: ?Sized,
    {
      range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, $crate::dynamic::memtable::bounded::$cmp<C>>,
    }

    impl<'a, S, Q, R, C> $iter<'a, S, Q, R, C>
    where
      S: $crate::dynamic::types::State<'a>,
      Q: ?Sized,
    {
      #[inline]
      pub(super) const fn new(range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, $crate::dynamic::memtable::bounded::$cmp<C>>) -> Self {
        Self { range }
      }
    }

    impl<'a, S, Q, R, C> Iterator for $iter<'a, S, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,
      R: core::ops::RangeBounds<Q>,
      Q: ?Sized + core::borrow::Borrow<[u8]>,
    {
      type Item = $ent<'a, S, C>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map($ent::new)
      }
    }

    impl<'a, S, Q, R, C> DoubleEndedIterator for $iter<'a, S, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,

      R: core::ops::RangeBounds<Q>,
      Q: ?Sized + core::borrow::Borrow<[u8]>,
    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map($ent::new)
      }
    }
  };
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct Table<C = Ascend> {
  cmp: Arc<C>,
  skl: GenericSkipMap<RecordPointer, (), MemtableComparator<C>>,
  range_deletions_skl: GenericSkipMap<RecordPointer, (), MemtableRangeComparator<C>>,
  range_updates_skl: GenericSkipMap<RecordPointer, (), MemtableRangeComparator<C>>,
}

impl<C> BaseTable for Table<C>
where
  C: BytesComparator + 'static,
{
  type Options = TableOptions<C>;

  type Error = skl::error::Error;

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
  fn new<A>(arena: A, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
    A: rarena_allocator::Allocator,
  {
    memmap_or_not!(opts(arena))
  }

  #[inline]
  fn insert(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .skl
      .insert(version.unwrap(), &pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }

  #[inline]
  fn remove(&self, version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
    self
      .skl
      .get_or_remove(version.unwrap(), &key)
      .map(|_| ())
      .map_err(Either::unwrap_right)
  }

  #[inline]
  fn range_remove(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_deletions_skl
      .insert(version.unwrap(), &pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }

  #[inline]
  fn range_set(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .insert(version.unwrap(), &pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }

  #[inline]
  fn range_unset(&self, version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .get_or_remove(version.unwrap(), &key)
      .map(|_| ())
      .map_err(Either::unwrap_right)
  }

  #[inline]
  fn mode() -> Kind {
    Kind::MultipleVersion
  }
}

impl<C> MultipleVersionMemtable for Table<C>
where
  C: BytesComparator + 'static,
{
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
  fn iter_points(&self, version: u64) -> Self::PointsIterator<'_, Active> {
    IterPoints::new(self.skl.iter(version))
  }

  #[inline]
  fn iter_points_with_tombstone(&self, version: u64) -> Self::PointsIterator<'_, MaybeTombstone> {
    IterPoints::new(self.skl.iter_with_tombstone(version))
  }

  #[inline]
  fn range_points<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangePoints::new(self.skl.range(version, range))
  }

  #[inline]
  fn range_points_with_tombstone<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::RangePoints<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangePoints::new(self.skl.range_with_tombstone(version, range))
  }

  #[inline]
  fn iter_bulk_deletions(&self, version: u64) -> Self::BulkDeletionsIterator<'_, Active> {
    IterBulkDeletions::new(self.range_deletions_skl.iter(version))
  }

  #[inline]
  fn iter_bulk_deletions_with_tombstone(
    &self,
    version: u64,
  ) -> Self::BulkDeletionsIterator<'_, MaybeTombstone> {
    IterBulkDeletions::new(self.range_deletions_skl.iter_with_tombstone(version))
  }

  #[inline]
  fn range_bulk_deletions<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(version, range))
  }

  #[inline]
  fn range_bulk_deletions_with_tombstone<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkDeletionsRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkDeletions::new(
      self
        .range_deletions_skl
        .range_with_tombstone(version, range),
    )
  }

  #[inline]
  fn iter_bulk_updates(&self, version: u64) -> Self::BulkUpdatesIterator<'_, Active> {
    IterBulkUpdates::new(self.range_updates_skl.iter(version))
  }

  #[inline]
  fn iter_bulk_updates_with_tombstone(
    &self,
    version: u64,
  ) -> Self::BulkUpdatesIterator<'_, MaybeTombstone> {
    IterBulkUpdates::new(self.range_updates_skl.iter_with_tombstone(version))
  }

  #[inline]
  fn range_bulk_updates<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(version, range))
  }

  #[inline]
  fn range_bulk_updates_with_tombstone<'a, Q, R>(
    &'a self,
    version: u64,
    range: R,
  ) -> Self::BulkUpdatesRange<'a, MaybeTombstone, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range_with_tombstone(version, range))
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
      .range_with_tombstone(query_version, ..=key)
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

mod entry;
mod iter;
mod point;
mod range_deletion;
mod range_update;
