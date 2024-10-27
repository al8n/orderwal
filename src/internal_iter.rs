use core::{iter::FusedIterator, marker::PhantomData};

use dbutils::CheapClone;

use crate::memtable::{BaseTable, MemtableEntry, MultipleVersionMemtable};

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M>
where
  M: BaseTable + 'a,
{
  iter: I,
  version: Option<u64>,
  head: Option<M::Item<'a>>,
  tail: Option<M::Item<'a>>,
  _m: PhantomData<&'a ()>,
}

impl<I, M: BaseTable> Iter<'_, I, M>
{
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: I) -> Self {
    Self {
      version,
      iter,
      head: None,
      tail: None,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub(super) const fn version(&self) -> Option<u64> {
    self.version
  }
}

impl<'a, I, M> Iterator for Iter<'a, I, M>
where
  M: BaseTable + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = M::Item<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().inspect(|ent| {
      self.head = Some(ent.pointer().cheap_clone());
    })
  }
}

impl<'a, I, M> DoubleEndedIterator for Iter<'a, I, M>
where
  M: BaseTable + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().inspect(|ent| {
      self.tail = Some(ent.pointer().cheap_clone());
    })
  }
}

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
  M: BaseTable + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
{
  iter: I,
  version: u64,
  head: Option<M::MultipleVersionItem<'a>>,
  tail: Option<M::MultipleVersionItem<'a>>,
  _m: PhantomData<&'a ()>,
}

impl<I, M> MultipleVersionBaseIter<'_, I, M>
where
  M: MultipleVersionMemtable,
{
  #[inline]
  pub(super) fn new(version: u64, iter: I) -> Self {
    Self {
      version,
      iter,
      head: None,
      tail: None,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub(super) const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, I, M> Iterator for MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  I: Iterator<Item = M::MultipleVersionItem<'a>>,
{
  type Item = M::MultipleVersionItem<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().inspect(|ent| {
      self.head = Some(ent.pointer().cheap_clone());
    })
  }
}

impl<'a, I, M> DoubleEndedIterator for MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  I: DoubleEndedIterator<Item = M::MultipleVersionItem<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().inspect(|ent| {
      self.tail = Some(ent.pointer().cheap_clone());
    })
  }
}

impl<'a, I, M> FusedIterator for MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  I: FusedIterator<Item = M::MultipleVersionItem<'a>>,
{
}
