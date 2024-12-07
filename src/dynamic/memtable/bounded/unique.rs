use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use super::{MemtableComparator, MemtableRangeComparator, TableOptions};

use crate::{
  dynamic::{
    memtable::{BaseTable, Memtable, MemtableEntry, RangeEntry, RangeUpdateEntry as _},
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
    unique::{sync::SkipMap as GenericSkipMap, Map as GenericMap},
    Builder,
  },
  Active, Arena as _, Options,
};

macro_rules! iter_wrapper {
  (
    $(#[$meta:meta])*
    $iter:ident($inner:ident) yield $ent:ident by $cmp:ident
  ) => {
    $(#[$meta])*
    pub struct $iter<'a, C>
    {
      iter: $inner<'a, $crate::types::RecordPointer, (), $crate::dynamic::memtable::bounded::$cmp<C>>,
    }

    impl<'a, C> $iter<'a, C>
    {
      #[inline]
      pub(super) const fn new(iter: $inner<'a, $crate::types::RecordPointer, (), $crate::dynamic::memtable::bounded::$cmp<C>>) -> Self {
        Self { iter }
      }
    }

    impl<'a, C> Iterator for $iter<'a, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Item = $ent<'a, C>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map($ent::new)
      }
    }

    impl<C> DoubleEndedIterator for $iter<'_, C>
    where
      C: dbutils::equivalentor::BytesComparator,
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
    pub struct $iter<'a, Q, R, C>
    where
      Q: ?Sized,
    {
      range: $inner<'a, $crate::types::RecordPointer, (), Q, R, $crate::dynamic::memtable::bounded::$cmp<C>>,
    }

    impl<'a, Q, R, C> $iter<'a, Q, R, C>
    where
      Q: ?Sized,
    {
      #[inline]
      pub(super) const fn new(range: $inner<'a, $crate::types::RecordPointer, (), Q, R, $crate::dynamic::memtable::bounded::$cmp<C>>) -> Self {
        Self { range }
      }
    }

    impl<'a, Q, R, C> Iterator for $iter<'a, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      R: core::ops::RangeBounds<Q>,
      Q: ?Sized + core::borrow::Borrow<[u8]>,
    {
      type Item = $ent<'a, C>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map($ent::new)
      }
    }

    impl<Q, R, C> DoubleEndedIterator for $iter<'_, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
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

mod entry;
mod iter;
mod point;
mod range_deletion;
mod range_update;

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
    = Entry<'a, C>
  where
    Self: 'a;

  type PointEntry<'a, S>
    = PointEntry<'a, C>
  where
    Self: 'a,
    S: State<'a>;

  type RangeDeletionEntry<'a, S>
    = RangeDeletionEntry<'a, C>
  where
    Self: 'a,
    S: State<'a>;

  type RangeUpdateEntry<'a, S>
    = RangeUpdateEntry<'a, C>
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
    = IterPoints<'a, C>
  where
    Self: 'a,
    S: State<'a>;

  type RangePoints<'a, S, Q, R>
    = RangePoints<'a, Q, R, C>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkDeletionsIterator<'a, S>
    = IterBulkDeletions<'a, C>
  where
    Self: 'a,
    S: State<'a>;

  type BulkDeletionsRange<'a, S, Q, R>
    = RangeBulkDeletions<'a, Q, R, C>
  where
    Self: 'a,
    S: State<'a>,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  type BulkUpdatesIterator<'a, S>
    = IterBulkUpdates<'a, C>
  where
    Self: 'a,
    S: State<'a>;

  type BulkUpdatesRange<'a, S, Q, R>
    = RangeBulkUpdates<'a, Q, R, C>
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
  fn insert(&self, _: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .skl
      .insert(&pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }

  #[inline]
  fn remove(&self, _: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
    self
      .skl
      .get_or_remove(&key)
      .map(|_| ())
      .map_err(Either::unwrap_right)
  }

  #[inline]
  fn range_remove(&self, _: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_deletions_skl
      .insert(&pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }

  #[inline]
  fn range_set(&self, _: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .insert(&pointer, &())
      .map(|_| ())
      .map_err(Among::unwrap_right)
  }

  #[inline]
  fn range_unset(&self, _: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
    self
      .range_updates_skl
      .get_or_remove(&key)
      .map(|_| ())
      .map_err(Either::unwrap_right)
  }

  #[inline]
  fn kind() -> Kind {
    Kind::Unique
  }
}

impl<C> Memtable for Table<C>
where
  C: BytesComparator + 'static,
{
  fn len(&self) -> usize {
        todo!()
    }

  fn get<Q>(&self, key: &Q) -> Option<Self::Entry<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    let ent = self.skl.get(key)?;
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
    Q: ?Sized + Borrow<[u8]>,
  {
    Range::new(self, range)
  }

  #[inline]
  fn point_iter(&self) -> Self::PointsIterator<'_, Active> {
    IterPoints::new(self.skl.iter())
  }

  #[inline]
  fn point_range<'a, Q, R>(&'a self, range: R) -> Self::RangePoints<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangePoints::new(self.skl.range(range))
  }

  #[inline]
  fn bulk_deletions_iter(&self) -> Self::BulkDeletionsIterator<'_, Active> {
    IterBulkDeletions::new(self.range_deletions_skl.iter())
  }

  #[inline]
  fn bulk_deletions_range<'a, Q, R>(
    &'a self,

    range: R,
  ) -> Self::BulkDeletionsRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkDeletions::new(self.range_deletions_skl.range(range))
  }
  
  #[inline]
  fn bulk_updates_iter(&self) -> Self::BulkUpdatesIterator<'_, Active> {
    IterBulkUpdates::new(self.range_updates_skl.iter())
  }
  
  #[inline]
  fn bulk_updates_range<'a, Q, R>(&'a self, range: R) -> Self::BulkUpdatesRange<'a, Active, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    RangeBulkUpdates::new(self.range_updates_skl.range(range))
  }
}

impl<C> Table<C>
where
  C: BytesComparator + 'static,
{
  fn validate<'a>(
    &'a self,
    ent: PointEntry<'a, C>,
  ) -> ControlFlow<Option<Entry<'a, C>>, PointEntry<'a, C>> {
    let key = ent.key();

    // check if the next entry is visible.
    // As the range_del_skl is sorted by the end key, we can use the lower_bound to find the first
    // deletion range that may cover the next entry.

    let shadow = self.range_deletions_skl.range(..=key).any(|ent| {
      let ent = RangeDeletionEntry::new(ent);
      BytesRangeComparator::compare_contains(&self.cmp, &ent.range(), key)
    });

    if shadow {
      return ControlFlow::Continue(ent);
    }

    // find the range key entry with maximum version that shadow the next entry.
    let range_ent = self
      .range_updates_skl
      .range(..=key)
      .find_map(|ent| {
        let ent = RangeUpdateEntry::new(ent);
        if BytesRangeComparator::compare_contains(&self.cmp, &ent.range(), key) {
          Some(ent)
        } else {
          None
        }
      });

    // check if the next entry's value should be shadowed by the range key entries.
    if let Some(range_ent) = range_ent {
      if let Some(val) = range_ent.value() {
        return ControlFlow::Break(Some(Entry::new(self, ent, key, val)));
      }

      // if value is None, the such range is unset, so we should return the value of the point entry.
    }

    let val = ent.value();
    ControlFlow::Break(Some(Entry::new(self, ent, key, val)))
  }
}
