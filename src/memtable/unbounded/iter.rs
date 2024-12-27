use core::ops::{ControlFlow, RangeBounds};

use dbutils::{
  equivalentor::{Comparator, QueryComparator},
  state::{Active, MaybeTombstone, State},
};

use crate::{
  memtable::{
    sealed, Entry, RangeEntry, RangeRemoveEntry as RangeRemoveEntryTrait,
    RangeUpdateEntry as RangeUpdateEntryTrait, Transfer,
  },
  types::{
    sealed::{PointComparator, Pointee, RangeComparator},
    Query, RecordPointer, RefQuery, TypeMode,
  },
};

use super::{
  EntryRef,
  IterPoints, RangePoints,
  RangeRemoveEntry,
  RangeUpdateEntry,
  PointEntryRef, Table,
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
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + Comparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer>
    + QueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeRemoveEntry<'a, Active, C, T>:
    RangeRemoveEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  PointEntryRef<'a, S, C, T>: Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  MaybeTombstone: Transfer<'a, T::Value<'a>>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<
      'a,
      Value = <MaybeTombstone as State>::Data<
        'a,
        <MaybeTombstone as sealed::Sealed<'a, T::Value<'a>>>::Value,
      >,
    > + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  type Item = EntryRef<'a, S, C, T>;

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
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + Comparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer>
    + QueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeRemoveEntry<'a, Active, C, T>:
    RangeRemoveEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  PointEntryRef<'a, S, C, T>: Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  MaybeTombstone: Transfer<'a, T::Value<'a>>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<
      'a,
      Value = <MaybeTombstone as State>::Data<
        'a,
        <MaybeTombstone as sealed::Sealed<'a, T::Value<'a>>>::Value,
      >,
    > + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
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
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + Comparator<RecordPointer>
    + QueryComparator<RecordPointer, Query<Q>>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer>
    + QueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeRemoveEntry<'a, Active, C, T>:
    RangeRemoveEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  PointEntryRef<'a, S, C, T>: Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  MaybeTombstone: Transfer<'a, T::Value<'a>>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<
      'a,
      Value = <MaybeTombstone as State>::Data<
        'a,
        <MaybeTombstone as sealed::Sealed<'a, T::Value<'a>>>::Value,
      >,
    > + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
{
  type Item = EntryRef<'a, S, C, T>;

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
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + Comparator<RecordPointer>
    + QueryComparator<RecordPointer, Query<Q>>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer>
    + QueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  RangeRemoveEntry<'a, Active, C, T>:
    RangeRemoveEntryTrait<'a> + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  PointEntryRef<'a, S, C, T>: Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  MaybeTombstone: Transfer<'a, T::Value<'a>>,
  RangeUpdateEntry<'a, MaybeTombstone, C, T>: RangeUpdateEntryTrait<
      'a,
      Value = <MaybeTombstone as State>::Data<
        'a,
        <MaybeTombstone as sealed::Sealed<'a, T::Value<'a>>>::Value,
      >,
    > + RangeEntry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output>,
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
