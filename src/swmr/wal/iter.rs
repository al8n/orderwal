use dbutils::Comparator;

use super::{EntryRef, Pointer};

/// An iterator over the entries in the WAL.
pub struct Iter<'a, C> {
  iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Iter<'a, C> {
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C: Comparator> Iterator for Iter<'a, C> {
  type Item = EntryRef<'a, C>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| EntryRef::new(ptr))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl<'a, C: Comparator> DoubleEndedIterator for Iter<'a, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| EntryRef::new(ptr))
  }
}
