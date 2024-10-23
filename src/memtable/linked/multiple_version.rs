use core::{
  convert::Infallible,
  ops::{Bound, RangeBounds},
};

use crossbeam_skiplist_mvcc::nested::{
  AllVersionsIter, AllVersionsRange, Entry, Iter, Range, SkipMap, VersionedEntry,
};
use dbutils::equivalent::Comparable;

use crate::{
  memtable,
  sealed::{Pointer, WithVersion},
};

/// An memory table implementation based on [`crossbeam_skiplist::SkipSet`].
pub struct MultipleVersionTable<P>(SkipMap<P, ()>);

impl<P> Default for MultipleVersionTable<P> {
  #[inline]
  fn default() -> Self {
    Self(SkipMap::new())
  }
}

impl<'a, P> memtable::MemtableEntry<'a> for Entry<'a, P, ()>
where
  P: Ord,
{
  type Pointer = P;

  #[inline]
  fn pointer(&self) -> &Self::Pointer {
    self.key()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    Entry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    Entry::prev(self)
  }
}

impl<'a, P> memtable::MemtableEntry<'a> for VersionedEntry<'a, P, ()>
where
  P: Ord,
{
  type Pointer = P;

  #[inline]
  fn pointer(&self) -> &Self::Pointer {
    self.key()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    VersionedEntry::next(self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    VersionedEntry::prev(self)
  }
}

impl<'a, P> memtable::MultipleVersionMemtableEntry<'a> for VersionedEntry<'a, P, ()>
where
  P: Ord,
{
  fn version(&self) -> u64 {
    VersionedEntry::version(self)
  }
}

impl<P> memtable::BaseTable for MultipleVersionTable<P>
where
  P: Send + Ord,
{
  type Pointer = P;

  type Item<'a>
    = Entry<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = Iter<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R>
    = Range<'a, Q, R, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;

  type Options = ();
  type Error = Infallible;

  fn new(_: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
  {
    Ok(Self(SkipMap::new()))
  }

  #[inline]
  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: Pointer + Ord + 'static,
  {
    self.0.insert_unchecked(ele.version(), ele, ());
    Ok(())
  }

  #[inline]
  fn remove(&mut self, key: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: crate::sealed::Pointer + Ord + 'static,
  {
    self.0.remove_unchecked(key.version(), key);
    Ok(())
  }
}

impl<P> memtable::MultipleVersionMemtable for MultipleVersionTable<P>
where
  P: Send + Ord + WithVersion,
{
  type MultipleVersionItem<'a>
    = VersionedEntry<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type AllIterator<'a>
    = AllVersionsIter<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type AllRange<'a, Q, R>
    = AllVersionsRange<'a, Q, R, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;

  fn upper_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.upper_bound(version, bound)
  }

  fn lower_bound<Q>(&self, version: u64, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.lower_bound(version, bound)
  }

  fn first(&self, version: u64) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.0.front(version)
  }

  fn last(&self, version: u64) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.0.back(version)
  }

  fn get<Q>(&self, version: u64, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.get(version, key)
  }

  fn contains<Q>(&self, version: u64, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.contains_key(version, key)
  }

  fn iter(&self, version: u64) -> Self::Iterator<'_> {
    self.0.iter(version)
  }

  fn iter_all_versions(&self, version: u64) -> Self::AllIterator<'_> {
    self.0.iter_all_versions(version)
  }

  fn range<'a, Q, R>(&'a self, version: u64, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.range(version, range)
  }

  fn range_all_versions<'a, Q, R>(&'a self, version: u64, range: R) -> Self::AllRange<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.range_all_versions(version, range)
  }
}
