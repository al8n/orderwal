use core::{iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use dbutils::{equivalent::Comparable, traits::Type};

use super::{
  super::super::{
    iter::*,
    sealed::{Memtable, Pointer},
  },
  kv_ref, ty_ref, GenericComparator, GenericQueryRange, Query,
};

/// Iterator over the entries in the WAL.
pub struct GenericIter<'a, K: ?Sized, V: ?Sized, I, P> {
  iter: Iter<'a, I, P>,
  _m: PhantomData<(&'a K, &'a V)>,
}

impl<'a, K: ?Sized, V: ?Sized, I, P> GenericIter<'a, K, V, I, P> {
  #[inline]
  pub(super) fn new(iter: Iter<'a, I, P>) -> Self {
    Self {
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, K, V, I, P> Iterator for GenericIter<'a, K, V, I, P>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  P: Pointer,
  I: Iterator<Item = &'a P>,
{
  type Item = (K::Ref<'a>, V::Ref<'a>);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(kv_ref::<K, V>)
  }
}

impl<'a, K, V, I, P> DoubleEndedIterator for GenericIter<'a, K, V, I, P>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  P: Pointer,
  I: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(kv_ref::<K, V>)
  }
}

impl<'a, K, V, I, P> FusedIterator for GenericIter<'a, K, V, I, P>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  P: Pointer,
  I: FusedIterator<Item = &'a P>,
{
}

/// Iterator over the keys in the WAL.
pub struct GenericKeys<'a, K: ?Sized, I, P> {
  iter: Keys<'a, I, P>,
  _m: PhantomData<&'a K>,
}

impl<'a, K: ?Sized, I, P> GenericKeys<'a, K, I, P> {
  #[inline]
  pub(super) fn new(iter: Keys<'a, I, P>) -> Self {
    Self {
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, K, I, P> Iterator for GenericKeys<'a, K, I, P>
where
  K: ?Sized + Type,
  P: Pointer,
  I: Iterator<Item = &'a P>,
{
  type Item = K::Ref<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(ty_ref::<K>)
  }
}

impl<'a, K, I, P> DoubleEndedIterator for GenericKeys<'a, K, I, P>
where
  K: ?Sized + Type,
  P: Pointer,
  I: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(ty_ref::<K>)
  }
}

impl<'a, K, I, P> FusedIterator for GenericKeys<'a, K, I, P>
where
  K: ?Sized + Type,
  P: Pointer,
  I: FusedIterator<Item = &'a P>,
{
}

/// Iterator over the values in the WAL.
pub struct GenericValues<'a, V: ?Sized, I, P> {
  iter: Values<'a, I, P>,
  _m: PhantomData<&'a V>,
}

impl<'a, V: ?Sized, I, P> GenericValues<'a, V, I, P> {
  #[inline]
  pub(super) fn new(iter: Values<'a, I, P>) -> Self {
    Self {
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, V, I, P> Iterator for GenericValues<'a, V, I, P>
where
  V: ?Sized + Type,
  P: Pointer,
  I: Iterator<Item = &'a P>,
{
  type Item = V::Ref<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(ty_ref::<V>)
  }
}

impl<'a, V, I, P> DoubleEndedIterator for GenericValues<'a, V, I, P>
where
  V: ?Sized + Type,
  P: Pointer,
  I: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(ty_ref::<V>)
  }
}

impl<'a, V, I, P> FusedIterator for GenericValues<'a, V, I, P>
where
  V: ?Sized + Type,
  P: Pointer,
  I: FusedIterator<Item = &'a P>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B::Pointer>,
  _m: PhantomData<&'a V>,
}

impl<'a, K, V, R, Q, B> GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B::Pointer>,
  ) -> Self {
    Self {
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, K, V, R, Q, B> Iterator for GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  type Item = (K::Ref<'a>, V::Ref<'a>);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(kv_ref::<K, V>)
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(kv_ref::<K, V>)
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  iter: RangeKeys<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B::Pointer>,
}

impl<'a, K, R, Q, B> GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: RangeKeys<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B::Pointer>,
  ) -> Self {
    Self { iter }
  }
}

impl<'a, K, R, Q, B> Iterator for GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  type Item = K::Ref<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(ty_ref::<K>)
  }
}

impl<'a, K, R, Q, B> DoubleEndedIterator for GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(ty_ref::<K>)
  }
}

impl<'a, K, R, Q, B> FusedIterator for GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  iter: RangeValues<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B::Pointer>,
  _m: PhantomData<&'a V>,
}

impl<'a, K, V, R, Q, B> GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: RangeValues<
      'a,
      B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>,
      B::Pointer,
    >,
  ) -> Self {
    Self {
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, K, V, R, Q, B> Iterator for GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  type Item = V::Ref<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(ty_ref::<V>)
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(ty_ref::<V>)
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'a,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = &'a B::Pointer>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + 'a,
{
}
