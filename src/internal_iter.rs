use core::{iter::FusedIterator, marker::PhantomData};

use dbutils::CheapClone;

use crate::memtable::{BaseTable, MemtableEntry};

use super::sealed::Pointer;

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M: BaseTable> {
  iter: I,
  version: Option<u64>,
  head: Option<M::Pointer>,
  tail: Option<M::Pointer>,
  _m: PhantomData<&'a ()>,
}

impl<I, M: BaseTable> Iter<'_, I, M> {
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
  M::Pointer: Pointer + CheapClone + 'static,
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
  M::Pointer: Pointer + CheapClone + 'static,
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
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}
