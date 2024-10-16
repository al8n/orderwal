use core::ops::{Bound, RangeBounds};

use dbutils::{equivalent::Comparable, traits::{KeyRef, Type}};
use skl::{map::{sync::{Entry, Iter, Range, SkipMap}, Map}, Arena as _, Container};

use crate::error::Error;

use super::{Memtable, MemtableEntry};


pub struct ArenaTable<P> {
  map: SkipMap<P, ()>,
}

impl<P> Default for ArenaTable<P> {
  #[inline]
  fn default() -> Self {
    todo!()
  }
}

impl<'a, P> MemtableEntry<'a> for Entry<'a, P, ()>
where
  P: Type<Ref<'a> = P> + KeyRef<'a, P>,
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

impl<P> Memtable for ArenaTable<P>
where
  for<'a> P: Type<Ref<'a> = P> + KeyRef<'a, P> + 'static,
{
  type Pointer = P;

  type Item<'a> = Entry<'a, Self::Pointer, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Iterator<'a> = Iter<'a, P, ()>
  where
    Self::Pointer: 'a,
    Self: 'a;

  type Range<'a, Q, R> = Range<'a, P, (), Q, R>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;
  
  type Options = ();

  #[inline]
  fn new(_: Self::Options) -> Result<Self, Error> {
    todo!()
  }

  #[inline]
  fn len(&self) -> usize {
    self.map.len()
  }

  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.upper_bound(bound)
  }

  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.lower_bound(bound)
  }

  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Error>
  where
    Self::Pointer: Ord + 'static,
  {
    self.map.insert(&ele, &()).map(|_| ()).map_err(|_| Error::insufficient_space(0, 0))
  }

  fn first(&self) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.first()
  }

  fn last(&self) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord,
  {
    self.map.last()
  }

  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.get(key)
  }

  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.contains_key(key)
  }

  fn iter(&self) -> Self::Iterator<'_> {
    self.map.iter()
  }

  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>,
  {
    self.map.range(range)
  }
}
