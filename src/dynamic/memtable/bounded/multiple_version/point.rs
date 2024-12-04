use core::cell::OnceCell;

use skl::{dynamic::BytesComparator, generic::{multiple_version::sync::{Entry, Iter, Range}, GenericValue, LazyRef}};

use crate::{dynamic::memtable::{bounded::MemtableComparator, MemtableEntry}, types::{RawEntryRef, RecordPointer}, WithVersion};

/// Range update entry.
pub struct PointEntry<'a, L, C>
where
  L: GenericValue<'a>,
{
  ent: Entry<'a, RecordPointer, L, MemtableComparator<C>>,
  data: OnceCell<RawEntryRef<'a>>,
}

impl<'a, L, C> Clone for PointEntry<'a, L, C>
where
  L: GenericValue<'a> + Clone
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      data: self.data.clone(),
    }
  }
}

impl<'a, L, C> PointEntry<'a, L, C>
where
  L: GenericValue<'a> + Clone
{
  #[inline]
  fn new(ent: Entry<'a, RecordPointer, L, MemtableComparator<C>>) -> Self {
    Self {
      ent,
      data: OnceCell::new(),
    }
  }
}

impl<'a, C> MemtableEntry<'a> for PointEntry<'a, LazyRef<'a, ()>, C>
where
  C: BytesComparator,
{
  type Value = &'a [u8];

  #[inline]
  fn key(&self) -> &'a [u8] {
    self.data.get_or_init(|| {
      self.ent.comparator().fetch_entry(self.ent.key())
    }).key()
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self.data.get_or_init(|| {
      self.ent.comparator().fetch_entry(self.ent.key())
    }).value().expect("entry must have a value") 
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    self.ent.next().map(|ent| Self {
      ent,
      data: OnceCell::new(),
    })
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(|ent| Self {
      ent,
      data: OnceCell::new(),
    })
  }
}

impl<'a, C> MemtableEntry<'a> for PointEntry<'a, Option<LazyRef<'a, ()>>, C>
where
  C: BytesComparator,
{
  type Value = Option<&'a [u8]>;

  #[inline]
  fn key(&self) -> &'a [u8] {
    self.data.get_or_init(|| {
      self.ent.comparator().fetch_entry(self.ent.key())
    }).key()
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self.data.get_or_init(|| {
      self.ent.comparator().fetch_entry(self.ent.key())
    }).value()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    self.ent.next().map(|ent| Self {
      ent,
      data: OnceCell::new(),
    })
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(|ent| Self {
      ent,
      data: OnceCell::new(),
    })
  }
}

impl<'a, L, C> WithVersion for PointEntry<'a, L, C>
where
  C: BytesComparator,
  L: GenericValue<'a> + 'a,
{
  #[inline]
  fn version(&self) -> u64 {
    self.ent.version()
  }
}

/// The iterator for point entries.
pub struct PointIter<'a, L, C>
where
  L: GenericValue<'a>,
{
  iter: Iter<'a, RecordPointer, L, MemtableComparator<C>>,
}

impl<'a, L, C> PointIter<'a, L, C>
where
  L: GenericValue<'a>,
{
  #[inline]
  pub(super) const fn new(iter: Iter<'a, RecordPointer, L, MemtableComparator<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, L, C> Iterator for PointIter<'a, L, C>
where
  C: BytesComparator,
  L: GenericValue<'a> + Clone + 'a,
{
  type Item = PointEntry<'a, L, C>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(PointEntry::new)
  }
}

impl<'a, L, C> DoubleEndedIterator for PointIter<'a, L, C>
where
  C: BytesComparator,
  L: GenericValue<'a> + Clone + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(PointEntry::new)
  }
}


