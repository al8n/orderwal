use core::iter::FusedIterator;

use dbutils::CheapClone;

use crate::{
  iter::{Iter as BaseIter, Range as BaseRange},
  sealed::{Memtable, Pointer, WithVersion},
};

use super::entry::{Entry, Key, Value};

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M: Memtable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: Memtable> Iter<'a, I, M> {
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
    M::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, I, M> Iterator for Iter<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, M::Item<'a>>;

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
  M: Memtable + 'static,
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

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, I, M: Memtable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: Memtable> Keys<'a, I, M> {
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
    M::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, I, M> Iterator for Keys<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, M::Item<'a>>;

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
  M: Memtable + 'static,
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

impl<'a, I, M> FusedIterator for Keys<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, I, M: Memtable> {
  iter: BaseIter<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: Memtable> Values<'a, I, M> {
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
    M::Pointer: WithVersion,
  {
    self.version.unwrap()
  }
}

impl<'a, I, M> Iterator for Values<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, M::Item<'a>>;

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
  M: Memtable + 'static,
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

impl<'a, I, M> FusedIterator for Values<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the entries in the WAL.
pub struct Range<'a, I, M: Memtable> {
  iter: BaseRange<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: Memtable> Range<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseRange<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
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

impl<'a, I, M> Iterator for Range<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Entry<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Entry::with_version_in(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for Range<'a, I, M>
where
  M: Memtable + 'static,
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

impl<'a, I, M> FusedIterator for Range<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the keys in the WAL.
pub struct RangeKeys<'a, I, M: Memtable> {
  iter: BaseRange<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: Memtable> RangeKeys<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseRange<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
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

impl<'a, I, M> Iterator for RangeKeys<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Key<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Key::with_version_in(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for RangeKeys<'a, I, M>
where
  M: Memtable + 'static,
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

impl<'a, I, M> FusedIterator for RangeKeys<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// Iterator over the values in the WAL.
pub struct RangeValues<'a, I, M: Memtable> {
  iter: BaseRange<'a, I, M>,
  version: Option<u64>,
}

impl<'a, I, M: Memtable> RangeValues<'a, I, M> {
  #[inline]
  pub(super) fn new(iter: BaseRange<'a, I, M>) -> Self {
    Self {
      version: iter.version(),
      iter,
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

impl<'a, I, M> Iterator for RangeValues<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = Value<'a, M::Item<'a>>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ent| Value::with_version_in(ent, self.version))
  }
}

impl<'a, I, M> DoubleEndedIterator for RangeValues<'a, I, M>
where
  M: Memtable + 'static,
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

impl<'a, I, M> FusedIterator for RangeValues<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}
