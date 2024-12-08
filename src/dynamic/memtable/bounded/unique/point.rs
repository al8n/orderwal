use skl::generic::unique::sync::{Entry, Iter, Range};

use crate::{
  dynamic::memtable::{bounded::comparator::MemtableComparator, MemtableEntry},
  types::{RawEntryRef, RecordPointer},
};

/// Point entry.
pub struct PointEntry<'a, C> {
  ent: Entry<'a, RecordPointer, (), MemtableComparator<C>>,
  data: core::cell::OnceCell<RawEntryRef<'a>>,
}
impl<C> Clone for PointEntry<'_, C> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      data: self.data.clone(),
    }
  }
}
impl<'a, C> PointEntry<'a, C> {
  #[inline]
  pub(super) fn new(ent: Entry<'a, RecordPointer, (), MemtableComparator<C>>) -> Self {
    Self {
      ent,
      data: core::cell::OnceCell::new(),
    }
  }
}

impl<'a, C> MemtableEntry<'a> for PointEntry<'a, C>
where
  C: dbutils::equivalentor::BytesComparator,
{
  type Value = &'a [u8];
  #[inline]
  fn key(&self) -> &'a [u8] {
    self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()))
      .key()
  }
  #[inline]
  fn value(&self) -> Self::Value {
    let ent = self
      .data
      .get_or_init(|| self.ent.comparator().fetch_entry(self.ent.key()));
    ent
      .value()
      .expect("entry in Active state must have a value")
  }
  #[inline]
  fn next(&mut self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }
  #[inline]
  fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }
}

iter_wrapper!(
  /// The iterator for point entries.
  IterPoints(Iter) yield PointEntry by MemtableComparator
);

range_wrapper!(
  /// The iterator over a subset of point entries.
  RangePoints(Range) yield PointEntry by MemtableComparator
);
