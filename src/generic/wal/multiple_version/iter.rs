use core::{iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use dbutils::{equivalent::Comparable, types::Type};

use crate::generic::{
  memtable::{BaseEntry, MultipleVersionMemtable, MultipleVersionMemtableEntry},
  types::multiple_version::{Entry, Key, Value, VersionedEntry},
  wal::{RecordPointer, ValuePointer},
};

use super::{Query, QueryRange};

/// Iterator over the entries in the WAL.
pub struct BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: I,
  version: u64,
  head: Option<(RecordPointer<M::Key>, ValuePointer<M::Value>)>,
  tail: Option<(RecordPointer<M::Key>, ValuePointer<M::Value>)>,
  _m: PhantomData<&'a ()>,
}

impl<'a, I, M> BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: I,
  version: u64,
  head: Option<(RecordPointer<M::Key>, Option<ValuePointer<M::Value>>)>,
  tail: Option<(RecordPointer<M::Key>, Option<ValuePointer<M::Value>>)>,
  _m: PhantomData<&'a ()>,
}

impl<'a, I, M> MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: Iterator<Item = M::MultipleVersionEntry<'a>>,
{
  type Item = M::MultipleVersionEntry<'a>;

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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::MultipleVersionEntry<'a>>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: FusedIterator<Item = M::MultipleVersionEntry<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> Iter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> Keys<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> Values<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
}

/// Iterator over the entries in the WAL.
pub struct IterAll<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: MultipleVersionBaseIter<'a, I, M>,
  version: u64,
}

impl<'a, I, M> IterAll<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
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

impl<'a, I, M> Iterator for IterAll<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: Iterator<Item = M::MultipleVersionEntry<'a>>,
{
  type Item = VersionedEntry<'a, M::MultipleVersionEntry<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| VersionedEntry::with_version(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for IterAll<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::MultipleVersionEntry<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| VersionedEntry::with_version(ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for IterAll<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  M::Key: Type + Ord,
  M::Value: Type,
  for<'b> M::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> M::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
  I: FusedIterator<Item = M::MultipleVersionEntry<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  iter: MultipleVersionBaseIter<
    'a,
    B::MultipleVersionRange<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>,
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
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: MultipleVersionBaseIter<
      'a,
      B::MultipleVersionRange<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>,
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
  B::MultipleVersionRange<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    Iterator<Item = B::MultipleVersionEntry<'a>>,
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  type Item = VersionedEntry<'a, B::MultipleVersionEntry<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| VersionedEntry::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::MultipleVersionRange<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::MultipleVersionEntry<'a>>,
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ent| VersionedEntry::with_version(ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for MultipleVersionRange<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: MultipleVersionMemtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::MultipleVersionRange<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::MultipleVersionEntry<'a>>,
  for<'b> B::Item<'b>: MultipleVersionMemtableEntry<'b>,
  for<'b> B::MultipleVersionEntry<'b>: MultipleVersionMemtableEntry<'b>,
{
}
