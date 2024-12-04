use core::{iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use crate::{
  generic::memtable::{BaseEntry, Memtable, MemtableEntry},
  generic::types::base::{Entry, Key, Value},
  generic::wal::{RecordPointer, ValuePointer},
};

use dbutils::{equivalent::Comparable, types::Type};

use super::{Query, QueryRange};

/// Iterator over the entries in the WAL.
pub struct BaseIter<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  iter: I,
  head: Option<(RecordPointer<M::Key>, ValuePointer<M::Value>)>,
  tail: Option<(RecordPointer<M::Key>, ValuePointer<M::Value>)>,
  _m: PhantomData<&'a ()>,
}

impl<I, M> BaseIter<'_, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: I) -> Self {
    Self {
      iter,
      head: None,
      tail: None,
      _m: PhantomData,
    }
  }
}

impl<'a, I, M> Iterator for BaseIter<'a, I, M>
where
  M: Memtable + 'a,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = M::Item<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().inspect(|ent| {
      self.head = Some((ent.key(), ent.value()));
    })
  }
}

impl<'a, I, M> DoubleEndedIterator for BaseIter<'a, I, M>
where
  M: Memtable + 'a,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().inspect(|ent| {
      self.tail = Some((ent.key(), ent.value()));
    })
  }
}

impl<'a, I, M> FusedIterator for BaseIter<'a, I, M>
where
  M: Memtable + 'a,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
}

impl<'a, I, M> Iter<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self { iter }
  }
}

impl<'a, I, M> Iterator for Iter<'a, I, M>
where
  M: Memtable + 'a,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  M::Key: Type + Ord,
  M::Value: Type,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Entry::new)
  }
}

impl<'a, I, M> DoubleEndedIterator for Iter<'a, I, M>
where
  M: Memtable + 'a,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  M::Key: Type + Ord,
  M::Value: Type,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Entry::new)
  }
}

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
  M: Memtable + 'a,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  M::Key: Type + Ord,
  M::Value: Type,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
}

impl<'a, I, M> Keys<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self { iter }
  }
}

impl<'a, I, M> Iterator for Keys<'a, I, M>
where
  M: Memtable + 'a,
  M::Key: Type,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Key::new)
  }
}

impl<'a, I, M> DoubleEndedIterator for Keys<'a, I, M>
where
  M: Memtable + 'a,
  M::Key: Type,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Key::new)
  }
}

impl<'a, I, M> FusedIterator for Keys<'a, I, M>
where
  M: Memtable + 'a,
  M::Key: Type,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  iter: BaseIter<'a, I, M>,
}

impl<'a, I, M> Values<'a, I, M>
where
  M: Memtable,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(iter: BaseIter<'a, I, M>) -> Self {
    Self { iter }
  }
}

impl<'a, I, M> Iterator for Values<'a, I, M>
where
  M: Memtable + 'a,
  M::Value: Type,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Value::new)
  }
}

impl<'a, I, M> DoubleEndedIterator for Values<'a, I, M>
where
  M: Memtable + 'a,
  M::Value: Type,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Value::new)
  }
}

impl<'a, I, M> FusedIterator for Values<'a, I, M>
where
  M: Memtable + 'a,
  M::Value: Type,
  for<'b> M::Item<'b>: MemtableEntry<'b>,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
}

impl<'a, R, Q, B> Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self { iter }
  }
}

impl<'a, R, Q, B> Iterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  type Item = Entry<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Entry::new)
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Entry::new)
  }
}

impl<'a, R, Q, B> FusedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
}

impl<'a, R, Q, B> RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self { iter }
  }
}

impl<'a, R, Q, B> Iterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  type Item = Key<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Key::new)
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Key::new)
  }
}

impl<'a, R, Q, B> FusedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
}

impl<'a, R, Q, B> RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>, B>,
  ) -> Self {
    Self { iter }
  }
}

impl<'a, R, Q, B> Iterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  type Item = Value<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Value::new)
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Value::new)
  }
}

impl<'a, R, Q, B> FusedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Comparable<<B::Key as Type>::Ref<'a>>,
  B: Memtable + 'a,
  B::Key: Type + Ord,
  B::Value: Type,
  B::Range<'a, Query<'a, B::Key, Q>, QueryRange<'a, B::Key, Q, R>>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: MemtableEntry<'b>,
{
}
