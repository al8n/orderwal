use core::{iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use dbutils::{equivalent::Comparable, types::Type};

use crate::{
  memtable::{BaseEntry, MultipleVersionMemtable, VersionedMemtableEntry},
  types::multiple_version::{Entry, Key, MultipleVersionEntry, Value},
  wal::{KeyPointer, ValuePointer},
};

use super::{Query, QueryRange};

/// Iterator over the entries in the WAL.
pub struct BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: I,
  version: u64,
  head: Option<(KeyPointer<M::Key>, ValuePointer<M::Value>)>,
  tail: Option<(KeyPointer<M::Key>, ValuePointer<M::Value>)>,
  _m: PhantomData<&'a ()>,
}

impl<'a, I, M> BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
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

impl<'a, I, M> Iterator for BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = M::Item<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().inspect(|ent| {
      self.head = Some((ent.key(), ent.value().unwrap()));
    })
  }
}

impl<'a, I, M> DoubleEndedIterator for BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().inspect(|ent| {
      self.tail = Some((ent.key(), ent.value().unwrap()));
    })
  }
}

impl<'a, I, M> FusedIterator for BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: I,
  version: u64,
  head: Option<(KeyPointer<M::Key>, Option<ValuePointer<M::Value>>)>,
  tail: Option<(KeyPointer<M::Key>, Option<ValuePointer<M::Value>>)>,
  _m: PhantomData<&'a ()>,
}

impl<'a, I, M> MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: Iterator<Item = M::VersionedItem<'a>>,
{
  type Item = M::VersionedItem<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().inspect(|ent| {
      self.head = Some((ent.key(), ent.value()));
    })
  }
}

impl<'a, I, M> DoubleEndedIterator for MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::VersionedItem<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().inspect(|ent| {
      self.tail = Some((ent.key(), ent.value()));
    })
  }
}

impl<'a, I, M> FusedIterator for MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::VersionedItem<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> Iter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, I, M> Iterator for Iter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  M::Key: Type + Ord,
  M::Value: Type,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Iter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  M::Key: Type + Ord,
  M::Value: Type,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
  M::Key: Type + Ord,
  M::Value: Type,
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> Keys<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the keys in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, I, M> Iterator for Keys<'a, I, M>
where
  M::Key: Type,
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Keys<'a, I, M>
where
  M::Key: Type,
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version(ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for Keys<'a, I, M>
where
  M::Key: Type,
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> Values<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the values in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, I, M> Iterator for Values<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Value: Type,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Values<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Value: Type,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version(ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for Values<'a, I, M>
where
  M::Value: Type,
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  version: u64,
}

impl<'a, R, Q, B> Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, R, Q, B> Iterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = Entry<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Entry::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  version: u64,
}

impl<'a, R, Q, B> RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the keys in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, R, Q, B> Iterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = Key<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Key::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  version: u64,
}

impl<'a, R, Q, B> RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, R, Q, B> Iterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = Value<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| Value::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}

/// Iterator over the entries in the WAL.
pub struct MultipleVersionIter<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: MultipleVersionBaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> MultipleVersionIter<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: MultipleVersionBaseIter<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
    }
  }

  /// Returns the query version of the entries in the iterator.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, I, M> Iterator for MultipleVersionIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: Iterator<Item = M::VersionedItem<'a>>,
{
  type Item = MultipleVersionEntry<'a, M::VersionedItem<'a>>;

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
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::VersionedItem<'a>>,
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
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::VersionedItem<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: MultipleVersionBaseIter<
    'a,
    B::RangeAll<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>,
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
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: MultipleVersionBaseIter<
      'a,
      B::RangeAll<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>,
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
  pub const fn version(&self) -> u64 {
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
  B::RangeAll<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    Iterator<Item = B::VersionedItem<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = MultipleVersionEntry<'a, B::VersionedItem<'a>>;

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
  B::RangeAll<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::VersionedItem<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
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
  B::RangeAll<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::VersionedItem<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}
