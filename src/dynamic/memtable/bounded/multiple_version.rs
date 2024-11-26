use core::borrow::Borrow;

use crate::MemtableComparator;

use {
  super::TableOptions,
  crate::{
    dynamic::{
      memtable::{BaseEntry, BaseTable, MultipleVersionMemtable, VersionedMemtableEntry},
      wal::{KeyPointer, ValuePointer},
    },
    types::Kind,
    WithVersion,
  },
  among::Among,
  core::ops::{Bound, RangeBounds},
  dbutils::{
    equivalentor::Comparator,
    types::{KeyRef, Type},
  },
  skl::{
    either::Either,
    dynamic::multiple_version::{sync::SkipMap, Map as _},
    Options,
  },
};

pub use skl::dynamic::multiple_version::sync::{Entry, Iter, IterAll, Range, RangeAll, VersionedEntry};
use skl::dynamic::Builder;

impl<'a, C> BaseEntry<'a> for Entry<'a, MemtableComparator<C>>
where
  C: Comparator,
{
  #[inline]
  fn next(&mut self) -> Option<Self> {
    Entry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    Entry::prev(self)
  }

  #[inline]
  fn key(&self) -> KeyPointer {
    *Entry::key(self)
  }
}

impl<'a, C> VersionedMemtableEntry<'a> for Entry<'a, MemtableComparator<C>>
where
  C: Comparator,
{
  #[inline]
  fn value(&self) -> Option<ValuePointer> {
    Some(*Entry::value(self))
  }

  #[inline]
  fn version(&self) -> u64 {
    Entry::version(self)
  }
}

impl<C> WithVersion for Entry<'_, MemtableComparator<C>> {
}

impl<'a, C> BaseEntry<'a> for VersionedEntry<'a, MemtableComparator<C>>
where
  C: Comparator,
{
  #[inline]
  fn next(&mut self) -> Option<Self> {
    VersionedEntry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    VersionedEntry::prev(self)
  }

  #[inline]
  fn key(&self) -> KeyPointer {
    *VersionedEntry::key(self)
  }
}

impl<'a, C> VersionedMemtableEntry<'a> for VersionedEntry<'a, MemtableComparator<C>>
where
  C: Comparator,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version()
  }

  #[inline]
  fn value(&self) -> Option<ValuePointer> {
    VersionedEntry::value(self)
  }
}

impl<C> WithVersion for VersionedEntry<'_, MemtableComparator<C>>
{
}

/// A memory table implementation based on ARENA [`SkipMap`](skl).
pub struct MultipleVersionTable<C> {
  skl: SkipMap<MemtableComparator<C>>,
  range_deletions_skl: SkipMap<MemtableRangeComparator<C>>,
  range_updates_skl: SkipMap<MemtableRangeComparator<C>>,
}

impl<C> BaseTable for MultipleVersionTable<C>
where
  C: Comparator,
{
  type Comparator = C;

  type Options = TableOptions;

  type Error = skl::error::Error;

  type Item<'a>
    = Entry<'a, MemtableComparator<C>>
  where
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, MemtableComparator<C>>
  where
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, MemtableComparator<C>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  #[inline]
  fn new(opts: Self::Options, cmp: MemtableComparator<Self::Comparator>) -> Result<Self, Self::Error> {
    let arena_opts = Options::new()
      .with_capacity(opts.capacity())
      .with_freelist(skl::options::Freelist::None)
      .with_unify(false)
      .with_max_height(opts.max_height());

    let b = Builder::new()
      .with_options(arena_opts)
      .with_comparator(cmp);
    let points: SkipMap<MemtableComparator<C>> = memmap_or_not!(opts(b))?;

  }

  fn insert(
    &self,
    version: Option<u64>,
    kp: KeyPointer,
    vp: ValuePointer,
  ) -> Result<(), Self::Error>
  {
    self
      .map
      .insert(version.unwrap_or(0), &kp, &vp)
      .map(|_| ())
      .map_err(|e| match e {
        Among::Right(e) => e,
        _ => unreachable!(),
      })
  }

  fn remove(&self, version: Option<u64>, key: KeyPointer) -> Result<(), Self::Error>
  {
    match self.map.get_or_remove(version.unwrap_or(0), &key) {
      Err(Either::Right(e)) => Err(e),
      Err(Either::Left(_)) => unreachable!(),
      _ => Ok(()),
    }
  }
  
  fn remove_range(&self, version: Option<u64>, rp: KeyPointer) -> Result<(), Self::Error> {
        todo!()
  }
  
  fn update_range(
        &self,
        version: Option<u64>,
        rp: KeyPointer,
        vp: ValuePointer,
      ) -> Result<(), Self::Error> {
        todo!()
  }
  
  #[inline]
  fn kind() -> Kind {
    Kind::MultipleVersion
  }
}

impl<C> MultipleVersionMemtable for MultipleVersionTable<C>
where
  C: Comparator + 'static,
{
  type VersionedItem<'a>
    = VersionedEntry<'a, MemtableComparator<C>>
  where
    Self: 'a;

  type IterAll<'a>
    = IterAll<'a, MemtableComparator<C>>
  where
    Self: 'a;

  type RangeAll<'a, Q, R>
    = RangeAll<'a, MemtableComparator<C>, Q, R>
  where
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>;

  #[inline]
  fn maximum_version(&self) -> u64 {
    self.points.maximum_version()
  }

  #[inline]
  fn minimum_version(&self) -> u64 {
    self.points.minimum_version()
  }

  #[inline]
  fn may_contain_version(&self, version: u64) -> bool {
    self.points.may_contain_version(version)
  }

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.upper_bound(version, bound)
  }

  fn upper_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.upper_bound_versioned(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.lower_bound(version, bound)
  }

  fn lower_bound_versioned<Q>(
    &self,
    version: u64,
    bound: Bound<&Q>,
  ) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.lower_bound_versioned(version, bound)
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  {
    self.points.first(version)
  }

  fn first_versioned(&self, version: u64) -> Option<Self::VersionedItem<'_>>
  {
    self.points.first_versioned(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  {
    self.points.last(version)
  }

  fn last_versioned(&self, version: u64) -> Option<Self::VersionedItem<'_>>
  {
    self.points.last_versioned(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.get(version, key)
  }

  fn get_versioned<Q>(&self, version: u64, key: &Q) -> Option<Self::VersionedItem<'_>>
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.get_versioned(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.contains_key(version, key)
  }

  fn contains_versioned<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.contains_key_versioned(version, key)
  }

  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    self.points.iter(version)
  }

  fn iter_all_versions(&self, version: u64) -> Self::IterAll<'_> {
    self.points.iter_all_versions(version)
  }

  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.range(version, range)
  }

  fn range_all_versions<'a, Q, R>(&'a self, version: u64, range: R) -> Self::RangeAll<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Borrow<[u8]>,
  {
    self.points.range_all_versions(version, range)
  }
}
