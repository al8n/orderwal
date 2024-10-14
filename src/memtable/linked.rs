use core::ops::RangeBounds;

use crossbeam_skiplist::{set::Entry, SkipSet};
use dbutils::equivalent::Comparable;

use crate::{error::Error, memtable};

/// An memory table implementation based on [`crossbeam_skiplist::SkipSet`].
pub struct LinkedTable<P>(SkipSet<P>);

impl<P> Default for LinkedTable<P> {
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

impl<P> memtable::Memtable for LinkedTable<P>
where
  P: Send + Ord,
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
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<Self::Pointer>;

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
  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Error>
  where
    P: Ord + 'static,
  {
    self.0.insert(ele);
    Ok(())
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
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    Q: ?Sized + Comparable<P>,
  {
    self.0.range(range)
  }
}
