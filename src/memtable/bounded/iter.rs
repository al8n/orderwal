use core::ops::{ControlFlow, RangeBounds};

use skl::{
  generic::{
    multiple_version::Map as _, Comparator, LazyRef, TypeRefComparator, TypeRefQueryComparator,
  },
  Active, MaybeTombstone, State, Transformable,
};

use crate::{
  memtable::{
    RangeDeletionEntry as RangeDeletionEntryTrait, RangeEntry,
    RangeUpdateEntry as RangeUpdateEntryTrait,
  },
  types::{
    sealed::{PointComparator, Pointee, RangeComparator},
    Query, RecordPointer, RefQuery, TypeMode,
  },
};

use super::{
  entry::Entry,
  point::{IterPoints, RangePoints},
  range_deletion::RangeDeletionEntry,
  range_update::RangeUpdateEntry,
  Table,
};

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, S, C, T>
where
  C: 'static,
  T: TypeMode,
  S: State,
{
  table: &'a Table<C, T>,
  iter: IterPoints<'a, S, C, T>,
  query_version: u64,
}

impl<'a, C, T> Iter<'a, MaybeTombstone, C, T>
where
  C: 'static,
  T: TypeMode,
  T::Comparator<C>: 'static,
{
  pub(in crate::memtable) fn with_tombstone(version: u64, table: &'a Table<C, T>) -> Self {
    Self {
      iter: IterPoints::new(table.skl.iter_all(version)),
      query_version: version,
      table,
    }
  }
}

impl<'a, C, T> Iter<'a, Active, C, T>
where
  C: 'static,
  T: TypeMode,
  T::Comparator<C>: 'static,
{
  pub(in crate::memtable) fn new(version: u64, table: &'a Table<C, T>) -> Self {
    Self {
      iter: IterPoints::new(table.skl.iter(version)),
      query_version: version,
      table,
    }
  }
}

impl<'a, S, C, T> Iterator for Iter<'a, S, C, T>
where
  C: 'static,
  S: State + 'a,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<'a, Value = Option<<S::Data<'a, T::Value<'a>> as Transformable>::Output>>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
{
  type Item = Entry<'a, S, C, T>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let next = self.iter.next()?;
      match self.table.validate(self.query_version, next) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}

impl<'a, S, C, T> DoubleEndedIterator for Iter<'a, S, C, T>
where
  C: 'static,
  S: State + 'a,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<'a, Value = Option<<S::Data<'a, T::Value<'a>> as Transformable>::Output>>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let prev = self.iter.next_back()?;
      match self.table.validate(self.query_version, prev) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}

/// An iterator over the entries of a `Memtable`.
pub struct Range<'a, S, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  C: 'static,
  T: TypeMode,
  S: State,
{
  table: &'a Table<C, T>,
  iter: RangePoints<'a, S, Q, R, C, T>,
  query_version: u64,
}

impl<'a, Q, R, C, T> Range<'a, Active, Q, R, C, T>
where
  C: 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized,
  T: TypeMode,
  T::Comparator<C>: 'static,
{
  pub(in crate::memtable) fn new(version: u64, table: &'a Table<C, T>, r: R) -> Self {
    Self {
      iter: RangePoints::new(table.skl.range(version, r.into())),
      query_version: version,
      table,
    }
  }
}

impl<'a, Q, R, C, T> Range<'a, MaybeTombstone, Q, R, C, T>
where
  C: 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized,
  T: TypeMode,
  T::Comparator<C>: 'static,
{
  pub(in crate::memtable) fn with_tombstone(version: u64, table: &'a Table<C, T>, r: R) -> Self {
    Self {
      iter: RangePoints::new(table.skl.range_all(version, r.into())),
      query_version: version,
      table,
    }
  }
}

impl<'a, S, Q, R, C, T> Iterator for Range<'a, S, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  C: 'static,
  S: State + 'a,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, Query<Q>>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<'a, Value = Option<<S::Data<'a, T::Value<'a>> as Transformable>::Output>>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
{
  type Item = Entry<'a, S, C, T>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let next = self.iter.next()?;
      match self.table.validate(self.query_version, next) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}

impl<'a, S, Q, R, C, T> DoubleEndedIterator for Range<'a, S, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  C: 'static,
  S: State + 'a,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, LazyRef<'a, RecordPointer>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Value<'a>: Pointee<'a, Input = &'a [u8]>,
  <T::Key<'a> as Pointee<'a>>::Output: 'a,
  <T::Value<'a> as Pointee<'a>>::Output: 'a,
  T::Comparator<C>: PointComparator<C>
    + TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, Query<Q>>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: TypeRefComparator<RecordPointer>
    + TypeRefQueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeDeletionEntry<'a, Active, C, T>:
    RangeDeletionEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<'a, Value = Option<<S::Data<'a, T::Value<'a>> as Transformable>::Output>>
    + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  <MaybeTombstone as State>::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    loop {
      let prev = self.iter.next_back()?;
      match self.table.validate(self.query_version, prev) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}
