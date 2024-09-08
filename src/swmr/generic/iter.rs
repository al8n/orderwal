use super::{EntryRef, KeyRef, Pointer, Type};

/// An iterator over the entries in the WAL.
pub struct Iter<'a, K, V> {
  iter: crossbeam_skiplist::set::Iter<'a, Pointer<K, V>>,
}

impl<'a, K, V> Iter<'a, K, V> {
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Iter<'a, Pointer<K, V>>) -> Self {
    Self { iter }
  }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
{
  type Item = EntryRef<'a, K, V>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| EntryRef::new(ptr))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| EntryRef::new(ptr))
  }
}
