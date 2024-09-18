use core::ops::Bound;

use crossbeam_skiplist::Comparable;

use super::{GenericEntryRef, GenericPointer as Pointer, KeyRef, Owned, Ref, Type};

type SetRefRange<'a, Q, K, V> = crossbeam_skiplist::set::Range<
  'a,
  Ref<'a, K, Q>,
  (Bound<Ref<'a, K, Q>>, Bound<Ref<'a, K, Q>>),
  Pointer<K, V>,
>;
type SetRange<'a, Q, K, V> = crossbeam_skiplist::set::Range<
  'a,
  Owned<'a, K, Q>,
  (Bound<Owned<'a, K, Q>>, Bound<Owned<'a, K, Q>>),
  Pointer<K, V>,
>;

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
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| GenericEntryRef::new(ptr))
  }
}

/// An iterator over a subset of the entries in the WAL.
pub struct RefRange<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
{
  iter: SetRefRange<'a, Q, K, V>,
}

impl<'a, Q, K, V> RefRange<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
{
  #[inline]
  pub(super) fn new(iter: SetRefRange<'a, Q, K, V>) -> Self {
    Self { iter }
  }
}

impl<'a, Q, K, V> Iterator for RefRange<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
{
  type Item = GenericEntryRef<'a, K, V>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| GenericEntryRef::new(ptr))
  }
}

impl<'a, Q, K, V> DoubleEndedIterator for RefRange<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| GenericEntryRef::new(ptr))
  }
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>> + Comparable<K>,
{
  iter: SetRange<'a, Q, K, V>,
}

impl<'a, Q, K, V> Range<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>> + Comparable<K>,
{
  #[inline]
  pub(super) fn new(iter: SetRange<'a, Q, K, V>) -> Self {
    Self { iter }
  }
}

impl<'a, Q, K, V> Iterator for Range<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>> + Comparable<K>,
{
  type Item = GenericEntryRef<'a, K, V>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| GenericEntryRef::new(ptr))
  }
}

impl<'a, Q, K, V> DoubleEndedIterator for Range<'a, Q, K, V>
where
  K: Type + Ord,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  Q: Ord + ?Sized + Comparable<K::Ref<'a>> + Comparable<K>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| GenericEntryRef::new(ptr))
  }
}
