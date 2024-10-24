use core::{convert::Infallible, ops::RangeBounds};

use crossbeam_skiplist::{set::Entry, SkipSet};
use dbutils::equivalent::Comparable;

use crate::{memtable, sealed::WithoutVersion};

/// An memory table implementation based on [`crossbeam_skiplist::SkipSet`].
pub struct Table<P>(SkipSet<P>);

impl<P> Default for Table<P> {
  #[inline]
  fn default() -> Self {
    Self(SkipSet::new())
  }
}

impl<'a, P> memtable::MemtableEntry<'a> for Entry<'a, P>
where
  P: Ord,
{
  type Pointer = P;

  #[inline]
  fn pointer(&self) -> &Self::Pointer {
    self
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

impl<P> memtable::BaseTable for Table<P>
where
  P: Send + Ord + std::fmt::Debug,
{
  type Pointer = P;

  type Item<'a>
    = Entry<'a, P>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a>
    = crossbeam_skiplist::set::Iter<'a, P>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R>
    = crossbeam_skiplist::set::Range<'a, Q, R, Self::Pointer>
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
    Ok(Self(SkipSet::new()))
  }

  #[inline]
  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: Ord + 'static,
  {
    self.0.insert(ele);
    Ok(())
  }

  #[inline]
  fn remove(&mut self, key: Self::Pointer) -> Result<(), Self::Error>
  where
    Self::Pointer: crate::sealed::Pointer + Ord + 'static,
  {
    self.0.remove(&key);
    Ok(())
  }
}

impl<P> memtable::Memtable for Table<P>
where
  P: Send + Ord + WithoutVersion + std::fmt::Debug,
{
  #[inline]
  fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  fn upper_bound<Q>(&self, bound: core::ops::Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.upper_bound(bound)
  }

  #[inline]
  fn lower_bound<Q>(&self, bound: core::ops::Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.0.lower_bound(bound)
  }

  #[inline]
  fn first(&self) -> Option<Self::Item<'_>> {
    self.0.front()
  }

  #[inline]
  fn last(&self) -> Option<Self::Item<'_>> {
    self.0.back()
  }

  #[inline]
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<P>,
  {
    self.0.get(key)
  }

  #[inline]
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<P>,
  {
    self.0.contains(key)
  }

  #[inline]
  fn iter(&self) -> Self::Iterator<'_> {
    self.0.iter()
  }

  #[inline]
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<P>,
  {
    self.0.range(range)
  }
}