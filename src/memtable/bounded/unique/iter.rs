use core::ops::{ControlFlow, RangeBounds};

use skl::Active;

use crate::types::TypeMode;

use super::*;

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, C, T>
where
  C: 'static,
  T: TypeMode,
{
  table: &'a Table<C, T>,
  iter: IterPoints<'a, Active, C, T>,
}

impl<'a, C, T> Iter<'a, C, T>
where
  C: 'static,
  T: TypeMode,
  T::Comparator<C>: 'static,
{
  pub(in crate::memtable) fn new(table: &'a Table<C, T>) -> Self {
    Self {
      iter: IterPoints::new(table.skl.iter()),
      table,
    }
  }
}

impl<'a, C, T> Iterator for Iter<'a, C, T>
where
  C: 'static,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, RecordPointer>
    + Comparator<<T::Key<'a> as Pointee<'a>>::Output>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, <T::Key<'a> as Pointee<'a>>::Output>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, Active, C, T>: RangeUpdateEntryTrait<'a, Value = <T::Value<'a> as Pointee<'a>>::Output>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  type Item = Entry<'a, Active, C, T>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let next = self.iter.next()?;
      match self.table.validate(next) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}

impl<'a, C, T> DoubleEndedIterator for Iter<'a, C, T>
where
  C: 'static,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, RecordPointer>
    + Comparator<<T::Key<'a> as Pointee<'a>>::Output>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, <T::Key<'a> as Pointee<'a>>::Output>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, Active, C, T>: RangeUpdateEntryTrait<'a, Value = <T::Value<'a> as Pointee<'a>>::Output>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let prev = self.iter.next_back()?;
      match self.table.validate(prev) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}

/// An iterator over the entries of a `Memtable`.
pub struct Range<'a, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  C: 'static,
  T: TypeMode,
{
  table: &'a Table<C, T>,
  iter: RangePoints<'a, Active, Q, R, C, T>,
}

impl<'a, Q, R, C, T> Range<'a, Q, R, C, T>
where
  C: 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized,
  T: TypeMode,
  T::Comparator<C>: 'static,
{
  pub(in crate::memtable) fn new(table: &'a Table<C, T>, r: R) -> Self {
    Self {
      iter: RangePoints::new(table.skl.range(r)),
      table,
    }
  }
}

impl<'a, Q, R, C, T> Iterator for Range<'a, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  C: 'static,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, Q>
    + Comparator<<T::Key<'a> as Pointee<'a>>::Output>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, <T::Key<'a> as Pointee<'a>>::Output>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, Active, C, T>: RangeUpdateEntryTrait<'a, Value = <T::Value<'a> as Pointee<'a>>::Output>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  type Item = Entry<'a, Active, C, T>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let next = self.iter.next()?;
      match self.table.validate(next) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}

impl<'a, Q, R, C, T> DoubleEndedIterator for Range<'a, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  C: 'static,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, Q>
    + Comparator<<T::Key<'a> as Pointee<'a>>::Output>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<'a, RecordPointer>
    + TypeRefQueryComparator<'a, RecordPointer, <T::Key<'a> as Pointee<'a>>::Output>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, Active, C, T>: RangeUpdateEntryTrait<'a, Value = <T::Value<'a> as Pointee<'a>>::Output>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let prev = self.iter.next_back()?;
      match self.table.validate(prev) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}
