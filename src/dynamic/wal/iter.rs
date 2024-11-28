use core::{borrow::Borrow, iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use crate::{dynamic::{
  memtable::{BaseTable, MemtableEntry},
  types::Entry,
}, WithVersion, WithoutVersion};

/// Iterator over the entries in the WAL.
pub struct BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
{
  iter: I,
  version: u64,
  _m: PhantomData<(&'a M, V)>,
}

impl<'a, V, I, M> BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
{
  /// Returns the query version of the iterator.
  #[inline]
  pub(super) const fn version(&self) -> u64
  {
    self.version
  }
}

impl<'a, V, I, M> BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: WithoutVersion,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  #[inline]
  pub(super) fn new(iter: I) -> Self {
    Self {
      version: 0,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, V, I, M> BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: WithVersion,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  #[inline]
  pub(super) fn with_version(version: u64, iter: I) -> Self {
    Self {
      version,
      iter, 
      _m: PhantomData,
    }
  }
}

impl<'a, V, I, M> Iterator for BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a>,
  V: crate::dynamic::types::Value<'a> + 'a,
  I: Iterator<Item = M::Entry<'a, V>>,
{
  type Item = M::Entry<'a, V>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next()
  }
}

impl<'a, V, I, M> DoubleEndedIterator for BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a>,
  V: crate::dynamic::types::Value<'a> + 'a,
  I: DoubleEndedIterator<Item = M::Entry<'a, V>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back()
  }
}

impl<'a, V, I, M> FusedIterator for BaseIter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a>,
  V: crate::dynamic::types::Value<'a> + 'a,
  I: FusedIterator<Item = M::Entry<'a, V>>,
{
}

/// Iterator over the entries in the WAL.
pub struct Iter<'a, V, I, M>
where
  M: BaseTable + 'a,
{
  iter: BaseIter<'a, V, I, M>,
  version: u64,
}

impl<'a, V, I, M> Iter<'a, V, I, M>
where
  M: BaseTable + 'a,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, V, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }
}

impl<'a, V, I, M> Iter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a> + WithVersion,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, V, I, M> Iterator for Iter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a>,
  V: crate::dynamic::types::Value<'a> + 'a,
  I: Iterator<Item = M::Entry<'a, V>>,
{
  type Item = Entry<'a, M::Entry<'a, V>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, V, I, M> DoubleEndedIterator for Iter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a>,
  V: crate::dynamic::types::Value<'a> + 'a,
  I: DoubleEndedIterator<Item = M::Entry<'a, V>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, V, I, M> FusedIterator for Iter<'a, V, I, M>
where
  M: BaseTable + 'a,
  M::Entry<'a, V>: MemtableEntry<'a>,
  V: crate::dynamic::types::Value<'a> + 'a,
  I: FusedIterator<Item = M::Entry<'a, V>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: BaseTable,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  iter: BaseIter<'a, V, B::Range<'a, V, Q, R>, B>,
  version: u64,
}

impl<'a, V, R, Q, B> Range<'a, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: BaseTable + 'a,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, V, B::Range<'a, V, Q, R>, B>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64
  where
    B::Entry<'a, V>: WithVersion,
  {
    self.version
  }
}

impl<'a, V, R, Q, B> Iterator for Range<'a, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: BaseTable + 'a,
  B::Range<'a, V, Q, R>: Iterator<Item = B::Entry<'a, V>>,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  type Item = Entry<'a, B::Entry<'a, V>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, V, R, Q, B> DoubleEndedIterator for Range<'a, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: BaseTable + 'a,
  B::Range<'a, V, Q, R>: DoubleEndedIterator<Item = B::Entry<'a, V>>,
  V: crate::dynamic::types::Value<'a> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, V, R, Q, B> FusedIterator for Range<'a, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: BaseTable + 'a,
  B::Range<'a, V, Q, R>: FusedIterator<Item = B::Entry<'a, V>>,
  V: crate::dynamic::types::Value<'a> + 'a,
{
}
