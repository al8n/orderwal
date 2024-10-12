use core::{iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use dbutils::{equivalent::Comparable, traits::Type, CheapClone};

use crate::sealed::WithVersion;

use super::{
  super::super::{
    iter::*,
    sealed::{Memtable, Pointer},
  },
  GenericComparator, GenericEntry, GenericKey, GenericQueryRange, GenericValue, Query,
};

/// Iterator over the entries in the WAL.
pub struct GenericIter<'a, K: ?Sized, V: ?Sized, I, M: Memtable> {
  iter: Iter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<(&'a K, &'a V)>,
}

impl<'a, K: ?Sized, V: ?Sized, I, M: Memtable> GenericIter<'a, K, V, I, M> {
  #[inline]
  pub(super) fn new(iter: Iter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    M::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, K, V, I, M> Iterator for GenericIter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = GenericEntry<'a, K, V, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| GenericEntry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, I, M> DoubleEndedIterator for GenericIter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| GenericEntry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, I, M> FusedIterator for GenericIter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct GenericKeys<'a, K: ?Sized, I, M: Memtable> {
  iter: Iter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<&'a K>,
}

impl<'a, K: ?Sized, I, M: Memtable> GenericKeys<'a, K, I, M> {
  #[inline]
  pub(super) fn new(iter: Iter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the keys in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    M::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, K, I, M> Iterator for GenericKeys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = GenericKey<'a, K, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| GenericKey::with_version_in(ent, self.version))
  }
}

impl<'a, K, I, M> DoubleEndedIterator for GenericKeys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| GenericKey::with_version_in(ent, self.version))
  }
}

impl<'a, K, I, M> FusedIterator for GenericKeys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct GenericValues<'a, V: ?Sized, I, M: Memtable> {
  iter: Iter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, V: ?Sized, I, M: Memtable> GenericValues<'a, V, I, M> {
  #[inline]
  pub(super) fn new(iter: Iter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the values in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    M::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, V, I, M> Iterator for GenericValues<'a, V, I, M>
where
  V: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = GenericValue<'a, V, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| GenericValue::with_version_in(ent, self.version))
  }
}

impl<'a, V, I, M> DoubleEndedIterator for GenericValues<'a, V, I, M>
where
  V: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| GenericValue::with_version_in(ent, self.version))
  }
}

impl<'a, V, I, M> FusedIterator for GenericValues<'a, V, I, M>
where
  V: ?Sized + Type,
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
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
  iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
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
    iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    B::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, K, V, R, Q, B> Iterator for GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
  type Item = GenericEntry<'a, K, V, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| GenericEntry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| GenericEntry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for GenericRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
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
  iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
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
    iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the keys in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    B::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, K, R, Q, B> Iterator for GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
  type Item = GenericKey<'a, K, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| GenericKey::with_version_in(ent, self.version))
  }
}

impl<'a, K, R, Q, B> DoubleEndedIterator for GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| GenericKey::with_version_in(ent, self.version))
  }
}

impl<'a, K, R, Q, B> FusedIterator for GenericRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
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
  iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
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
    iter: Range<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    B::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, K, V, R, Q, B> Iterator for GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
  type Item = GenericValue<'a, V, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| GenericValue::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| GenericValue::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for GenericRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q>,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Ord + Comparable<K::Ref<'a>>,
  for<'b> Query<'b, K, Q>: Comparable<B::Pointer> + Ord,
  B: Memtable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer<Comparator = GenericComparator<K>> + CheapClone + 'static,
{
}
