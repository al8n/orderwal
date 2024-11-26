use core::{borrow::Borrow, iter::FusedIterator, marker::PhantomData, ops::RangeBounds};

use crate::dynamic::{
  memtable::{BaseEntry, MultipleVersionMemtable, VersionedMemtableEntry},
  types::multiple_version::{Entry, Key, Value, VersionedEntry},
  wal::{KeyPointer, ValuePointer},
};

/// Iterator over the entries in the WAL.
pub struct BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: I,
  version: u64,
  head: Option<(KeyPointer, ValuePointer)>,
  tail: Option<(KeyPointer, ValuePointer)>,
  ptr: *const u8,
  _m: PhantomData<&'a M>,
}

impl<'a, I, M> BaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(version: u64, iter: I, ptr: *const u8) -> Self {
    Self {
      version,
      iter,
      head: None,
      tail: None,
      ptr,
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
  type Item = (*const u8, M::Item<'a>);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ent| {
      self.head = Some((ent.key(), ent.value().unwrap()));
      (self.ptr, ent)
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
    self.iter.next_back().map(|ent| {
      self.tail = Some((ent.key(), ent.value().unwrap()));
      (self.ptr, ent)
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
  head: Option<(KeyPointer, Option<ValuePointer>)>,
  tail: Option<(KeyPointer, Option<ValuePointer>)>,
  ptr: *const u8,
  _m: PhantomData<&'a M>,
}

impl<'a, I, M> MultipleVersionBaseIter<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(version: u64, iter: I, ptr: *const u8) -> Self {
    Self {
      version,
      iter,
      head: None,
      tail: None,
      ptr,
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
  type Item = (*const u8, M::VersionedItem<'a>);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ent| {
      self.head = Some((ent.key(), ent.value()));
      (self.ptr, ent)
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
    self.iter.next_back().map(|ent| {
      self.tail = Some((ent.key(), ent.value()));
      (self.ptr, ent)
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
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|(ptr, ent)| Entry::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Iter<'a, I, M>
where
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
      .map(|(ptr, ent)| Entry::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
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
  pub(super) fn new( iter: BaseIter<'a, I, M>) -> Self {
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
      .map(|(ptr, ent)| Key::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Keys<'a, I, M>
where
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
      .map(|(ptr, ent)| Key::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for Keys<'a, I, M>
where
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
  pub(super) fn new( iter: BaseIter<'a, I, M>) -> Self {
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
      .map(|(ptr, ent)| Value::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Values<'a, I, M>
where
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
      .map(|(ptr, ent)| Value::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for Values<'a, I, M>
where
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
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Q, R>, B>,
  version: u64,
  
}

impl<'a, R, Q, B> Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Q, R>, B>,
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
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::Range<'a, Q, R>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = Entry<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|(ptr, ent)| Entry::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::Range<'a, Q, R>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|(ptr, ent)| Entry::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for Range<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::Range<'a, Q, R>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Q, R>, B>,
  version: u64,
  
}

impl<'a, R, Q, B> RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Q, R>, B>,
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
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::Range<'a, Q, R>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = Key<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|(ptr, ent)| Key::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::Range<'a, Q, R>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|(ptr, ent)| Key::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeKeys<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::Range<'a, Q, R>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: BaseIter<'a, B::Range<'a, Q, R>, B>,
  version: u64,
  
}

impl<'a, R, Q, B> RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    iter: BaseIter<'a, B::Range<'a, Q, R>, B>,
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
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  
  B::Range<'a, Q, R>: Iterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = Value<'a, B::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|(ptr, ent)| Value::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  
  B::Range<'a, Q, R>:
    DoubleEndedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|(ptr, ent)| Value::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeValues<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  
  B::Range<'a, Q, R>:
    FusedIterator<Item = B::Item<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}

/// Iterator over the entries in the WAL.
pub struct IterAll<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: MultipleVersionBaseIter<'a, I, M>,
  version: u64,
  
}

impl<'a, I, M> IterAll<'a, I, M>
where
  M: MultipleVersionMemtable,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new( iter: MultipleVersionBaseIter<'a, I, M>) -> Self {
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
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: Iterator<Item = M::VersionedItem<'a>>,
{
  type Item = VersionedEntry<'a, M::VersionedItem<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|(ptr, ent)| VersionedEntry::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for IterAll<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: DoubleEndedIterator<Item = M::VersionedItem<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|(ptr, ent)| VersionedEntry::with_version(ptr, ent, self.version))
  }
}

impl<'a, I, M> FusedIterator for IterAll<'a, I, M>
where
  M: MultipleVersionMemtable + 'a,
  for<'b> M::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> M::VersionedItem<'b>: VersionedMemtableEntry<'b>,
  I: FusedIterator<Item = M::VersionedItem<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct RangeAll<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  iter: MultipleVersionBaseIter<
    'a,
    B::RangeAll<'a, Q, R>,
    B,
  >,
  version: u64,
  
}

impl<'a, R, Q, B> RangeAll<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  pub(super) fn new(
    
    iter: MultipleVersionBaseIter<
      'a,
      B::RangeAll<'a, Q, R>,
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

impl<'a, R, Q, B> Iterator for RangeAll<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  B::RangeAll<'a, Q, R>:
    Iterator<Item = B::VersionedItem<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  type Item = VersionedEntry<'a, B::VersionedItem<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|(ptr, ent)| VersionedEntry::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> DoubleEndedIterator for RangeAll<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  
  B::RangeAll<'a, Q, R>:
    DoubleEndedIterator<Item = B::VersionedItem<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|(ptr, ent)| VersionedEntry::with_version(ptr, ent, self.version))
  }
}

impl<'a, R, Q, B> FusedIterator for RangeAll<'a, R, Q, B>
where
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  B: MultipleVersionMemtable + 'a,
  
  B::RangeAll<'a, Q, R>:
    FusedIterator<Item = B::VersionedItem<'a>>,
  for<'b> B::Item<'b>: VersionedMemtableEntry<'b>,
  for<'b> B::VersionedItem<'b>: VersionedMemtableEntry<'b>,
{
}
