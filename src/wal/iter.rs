use core::{iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use dbutils::{equivalent::Comparable, traits::Type, CheapClone};

use crate::{
  memtable::{BaseTable, MultipleVersionMemtable},
  sealed::WithVersion,
  types::{Entry, Key, Value},
};

use super::{
  super::{internal_iter::Iter as BaseIter, sealed::Pointer},
  GenericQueryRange, Query,
};

/// Iterator over the entries in the WAL.
pub struct Iter<'a, K: ?Sized, V: ?Sized, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<(&'a K, &'a V)>,
}

impl<'a, K: ?Sized, V: ?Sized, I, M: BaseTable> Iter<'a, K, V, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
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

impl<'a, K, V, I, M> Iterator for Iter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, K, V, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, I, M> DoubleEndedIterator for Iter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, I, M> FusedIterator for Iter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, K: ?Sized, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<&'a K>,
}

impl<'a, K: ?Sized, I, M: BaseTable> Keys<'a, K, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
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

impl<'a, K, I, M> Iterator for Keys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, K, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, I, M> DoubleEndedIterator for Keys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, I, M> FusedIterator for Keys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, V: ?Sized, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, V: ?Sized, I, M: BaseTable> Values<'a, V, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
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

impl<'a, V, I, M> Iterator for Values<'a, V, I, M>
where
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, V, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, V, I, M> DoubleEndedIterator for Values<'a, V, I, M>
where
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, V, I, M> FusedIterator for Values<'a, V, I, M>
where
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'a,
  B::Pointer: Pointer + 'a,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, K, V, R, Q, B> Range<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'a,
  B::Pointer: Pointer + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
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

impl<'a, K, V, R, Q, B> Iterator for Range<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
  type Item = Entry<'a, K, V, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for Range<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for Range<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'a,
  B::Pointer: Pointer + 'a,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
}

impl<'a, K, R, Q, B> RangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'a,
  B::Pointer: Pointer + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
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

impl<'a, K, R, Q, B> Iterator for RangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
  type Item = Key<'a, K, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, R, Q, B> DoubleEndedIterator for RangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, R, Q, B> FusedIterator for RangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'a,
  B::Pointer: Pointer + 'a,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, K, V, R, Q, B> RangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'a,
  B::Pointer: Pointer + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
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

impl<'a, K, V, R, Q, B> Iterator for RangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
  type Item = Value<'a, V, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for RangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for RangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: BaseTable + 'static,
  B::Range<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + CheapClone + 'static,
{
}

/// Iterator over the entries in the WAL.
pub struct MultipleVersionIter<'a, K: ?Sized, V: ?Sized, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<(&'a K, &'a V)>,
}

impl<'a, K: ?Sized, V: ?Sized, I, M: BaseTable> MultipleVersionIter<'a, K, V, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
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

impl<'a, K, V, I, M> Iterator for MultipleVersionIter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, K, V, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, I, M> DoubleEndedIterator for MultipleVersionIter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, I, M> FusedIterator for MultipleVersionIter<'a, K, V, I, M>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct MultipleVersionKeys<'a, K: ?Sized, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<&'a K>,
}

impl<'a, K: ?Sized, I, M: BaseTable> MultipleVersionKeys<'a, K, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
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

impl<'a, K, I, M> Iterator for MultipleVersionKeys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, K, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, I, M> DoubleEndedIterator for MultipleVersionKeys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, I, M> FusedIterator for MultipleVersionKeys<'a, K, I, M>
where
  K: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct MultipleVersionValues<'a, V: ?Sized, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, V: ?Sized, I, M: BaseTable> MultipleVersionValues<'a, V, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
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

impl<'a, V, I, M> Iterator for MultipleVersionValues<'a, V, I, M>
where
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, V, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, V, I, M> DoubleEndedIterator for MultipleVersionValues<'a, V, I, M>
where
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, V, I, M> FusedIterator for MultipleVersionValues<'a, V, I, M>
where
  V: ?Sized + Type,
  M: BaseTable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct MultipleVersionRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Pointer: Pointer + WithVersion + 'a,
{
  iter: BaseIter<'a, B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, K, V, R, Q, B> MultipleVersionRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Pointer: Pointer + WithVersion + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
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

impl<'a, K, V, R, Q, B> Iterator for MultipleVersionRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
  type Item = Entry<'a, K, V, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for MultipleVersionRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for MultipleVersionRange<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct MultipleVersionRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Pointer: WithVersion + 'a,
  B::Pointer: Pointer + WithVersion + 'a,
{
  iter: BaseIter<'a, B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
}

impl<'a, K, R, Q, B> MultipleVersionRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Pointer: Pointer + WithVersion + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
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

impl<'a, K, R, Q, B> Iterator for MultipleVersionRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
  type Item = Key<'a, K, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, R, Q, B> DoubleEndedIterator for MultipleVersionRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, K, R, Q, B> FusedIterator for MultipleVersionRangeKeys<'a, K, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct MultipleVersionRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Pointer: Pointer + WithVersion + 'a,
{
  iter: BaseIter<'a, B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
  version: Option<u64>,
  _m: PhantomData<&'a V>,
}

impl<'a, K, V, R, Q, B> MultipleVersionRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Pointer: Pointer + WithVersion + 'a,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>, B>,
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

impl<'a, K, V, R, Q, B> Iterator for MultipleVersionRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>: Iterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
  type Item = Value<'a, V, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> DoubleEndedIterator for MultipleVersionRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, K, V, R, Q, B> FusedIterator for MultipleVersionRangeValues<'a, K, V, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  K: Type + Ord + ?Sized,
  V: ?Sized + Type,
  Q: ?Sized + Comparable<K::Ref<'a>>,
  B: MultipleVersionMemtable + 'static,
  B::AllRange<'a, Query<'a, K, Q>, GenericQueryRange<'a, K, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  B::Pointer: Pointer + WithVersion + CheapClone + 'static,
{
}
