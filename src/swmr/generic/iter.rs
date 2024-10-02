use core::ops::Bound;

use crossbeam_skiplist::Comparable;

use super::{GenericEntryRef, GenericPointer as Pointer, KeyRef, Query, Type};

type SetRange<'a, Q, K, V> = crossbeam_skiplist::set::Range<
  'a,
  Query<'a, K, Q>,
  (Bound<Query<'a, K, Q>>, Bound<Query<'a, K, Q>>),
  Pointer<K, V>,
>;

/// An iterator over the entries in the WAL.
pub struct Iter<'a, K: ?Sized, V: ?Sized> {
  iter: crossbeam_skiplist::set::Iter<'a, Pointer<K, V>>,
}

impl<'a, K, V> Iter<'a, K, V>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Iter<'a, Pointer<K, V>>) -> Self {
    Self { iter }
  }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
{
  type Item = GenericEntryRef<'a, K, V>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| GenericEntryRef::new(ptr))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.iter.size_hint()
  }
}

impl<K, V> DoubleEndedIterator for Iter<'_, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| GenericEntryRef::new(ptr))
  }
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, Q, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
  Q: Ord + ?Sized + for<'b> Comparable<K::Ref<'b>>,
{
  iter: SetRange<'a, Q, K, V>,
}

impl<'a, Q, K, V> Range<'a, Q, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
  Q: Ord + ?Sized + for<'b> Comparable<K::Ref<'b>>,
{
  #[inline]
  pub(super) fn new(iter: SetRange<'a, Q, K, V>) -> Self {
    Self { iter }
  }
}

impl<'a, Q, K, V> Iterator for Range<'a, Q, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
  Q: Ord + ?Sized + for<'b> Comparable<K::Ref<'b>>,
{
  type Item = GenericEntryRef<'a, K, V>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| GenericEntryRef::new(ptr))
  }
}

impl<Q, K, V> DoubleEndedIterator for Range<'_, Q, K, V>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized,
  Q: Ord + ?Sized + for<'b> Comparable<K::Ref<'b>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| GenericEntryRef::new(ptr))
  }
}
