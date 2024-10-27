use core::{iter::FusedIterator, ops::RangeBounds};

use dbutils::{equivalent::Comparable, traits::Type};

use crate::{
  memtable::{BaseTable, MultipleVersionMemtable},
  sealed::WithVersion,
  types::{Entry, Key, MultipleVersionEntry, Value},
};

use super::{
  super::internal_iter::{Iter as BaseIter, MultipleVersionBaseIter}, GenericQueryRange, Query
};

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: BaseTable> Iter<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    M::Item<'a>: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, I, M> Iterator for Iter<'a, I, M>
where
  M: BaseTable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, M::Key, M::Value, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Iter<'a, I, M>
where
  M: BaseTable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
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

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
  M: BaseTable + 'static,
  M::Key: Type + Ord,
  M::Value: Type,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: BaseTable> Keys<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the keys in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    M::Item<'a>: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, I, M> Iterator for Keys<'a, I, M>
where
  M::Key: Type,
  M: BaseTable + 'a,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, M::Key, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Keys<'a, I, M>
where
  M::Key: Type,
  M: BaseTable + 'a,
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

impl<'a, I, M> FusedIterator for Keys<'a, I, M>
where
  M::Key: Type,
  M: BaseTable + 'a,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, I, M: BaseTable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: BaseTable> Values<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the values in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    M::Item<'a>: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, I, M> Iterator for Values<'a, I, M>
where
  M: BaseTable + 'a,
  M::Value: Type,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, M::Value, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Values<'a, I, M>
where
  M: BaseTable + 'a,
  M::Value: Type,
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

impl<'a, I, M> FusedIterator for Values<'a, I, M>
where
  M::Value: Type,
  M: BaseTable + 'a,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B::Key: Type + Ord,
  B: BaseTable + 'a,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>, B>,
  version: Option<u64>,
}

impl<'a, R, Q, B> Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a, 
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    B::Item<'a>: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, R, Q, B> Iterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
{
  type Item = Entry<'a, B::Key, B::Value, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a, 
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>: FusedIterator<Item = B::Item<'a>>,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>, B>,
  version: Option<u64>,
}

impl<'a, R, Q, B> RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>, B>,
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
    B::Item<'a>: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, R, Q, B> Iterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
{
  type Item = Key<'a, B::Key, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>: FusedIterator<Item = B::Item<'a>>,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>, B>,
  version: Option<u64>,
}

impl<'a, R, Q, B> RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub fn version(&self) -> u64
  where
    B::Item<'a>: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, R, Q, B> Iterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
{
  type Item = Value<'a, B::Value, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: BaseTable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>: FusedIterator<Item = B::Item<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct MultipleVersionIter<'a, I, M: MultipleVersionMemtable> {
  iter: MultipleVersionBaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M: MultipleVersionMemtable> MultipleVersionIter<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: MultipleVersionBaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64
  where
    M::Item<'a>: WithVersion,
  {
    self.version
  }
}

impl<'a, I, M> Iterator for MultipleVersionIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  I: Iterator<Item = M::MultipleVersionItem<'a>>,
{
  type Item = MultipleVersionEntry<'a, M::Key, M::Value, M::MultipleVersionItem<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| MultipleVersionEntry::with_version(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for MultipleVersionIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  I: DoubleEndedIterator<Item = M::MultipleVersionItem<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| MultipleVersionEntry::with_version(ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for MultipleVersionIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  I: FusedIterator<Item = M::MultipleVersionItem<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
{
  iter: MultipleVersionBaseIter<
    'a,
    B::AllRange<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>,
    B,
  >,
  version: u64,
}

impl<'a, R, Q, B> MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
{
  #[inline]
  pub(super) fn new(
    iter: MultipleVersionBaseIter<
      'a,
      B::AllRange<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>,
      B,
    >,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64
  where
    B::Item<'a>: WithVersion,
  {
    self.version
  }
}

impl<'a, R, Q, B> Iterator for MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::AllRange<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>:
    Iterator<Item = B::MultipleVersionItem<'a>>,
{
  type Item = MultipleVersionEntry<'a, B::Key, B::Value, B::MultipleVersionItem<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| MultipleVersionEntry::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::AllRange<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::MultipleVersionItem<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| MultipleVersionEntry::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::AllRange<'a, Query<'a, B::Key, Q>, GenericQueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::MultipleVersionItem<'a>>,
{
}
