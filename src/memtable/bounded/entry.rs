use core::ops::ControlFlow;

use skl::{
  generic::{Comparator, LazyRef, TypeRefComparator, TypeRefQueryComparator},
  Active, MaybeTombstone, State, Transformable,
};

use crate::{
  memtable::{
    MemtableEntry, RangeDeletionEntry as RangeDeletionEntryTrait, RangeEntry,
    RangeUpdateEntry as RangeUpdateEntryTrait,
  },
  types::{
    sealed::{PointComparator, Pointee, RangeComparator},
    Query, RecordPointer, RefQuery, TypeMode,
  },
  WithVersion,
};

use super::{
  point::PointEntry, range_deletion::RangeDeletionEntry, range_update::RangeUpdateEntry, Table,
};

/// Entry in the memtable.
pub struct Entry<'a, S, C, T>
where
  S: State,
  T: TypeMode,
{
  table: &'a Table<C, T>,
  point_ent: PointEntry<'a, S, C, T>,
  key: <T::Key<'a> as Pointee<'a>>::Output,
  val: Option<S::Data<'a, T::Value<'a>>>,
  version: u64,
  query_version: u64,
}

impl<'a, S, C, T> core::fmt::Debug for Entry<'a, S, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, ()>>: Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  <S::Data<'a, T::Value<'a>> as Transformable>::Output: core::fmt::Debug,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  <T::Key<'a> as Pointee<'a>>::Output: core::fmt::Debug,
  T::Comparator<C>: PointComparator<C> + TypeRefComparator<RecordPointer>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.value_in())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, S, C, T> Clone for Entry<'a, S, C, T>
where
  S: State,
  S::Data<'a, T::Value<'a>>: Transformable + Clone,
  <S::Data<'a, T::Value<'a>> as Transformable>::Output: Clone,
  PointEntry<'a, S, C, T>: Clone,
  T: TypeMode,
  T::Key<'a>: Clone,
  T::Value<'a>: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      table: self.table,
      point_ent: self.point_ent.clone(),
      key: self.key,
      val: self.val.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, S, C, T> MemtableEntry<'a> for Entry<'a, S, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, ()>>: Clone + Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, T::Value<'a>>: Transformable<Input = Option<&'a [u8]>> + 'a,
  <S::Data<'a, T::Value<'a>> as Transformable>::Output: Clone,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
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
  type Key = <T::Key<'a> as Pointee<'a>>::Output;

  type Value = <S::Data<'a, T::Value<'a>> as Transformable>::Output;

  #[inline]
  fn key(&self) -> Self::Key {
    self.key
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self.value_in()
  }

  #[inline]
  fn next(&self) -> Option<Self> {
    let mut next = self.point_ent.next();
    while let Some(ent) = next {
      match self.table.validate(self.query_version, ent) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(ent) => next = ent.next(),
      }
    }
    None
  }

  #[inline]
  fn prev(&self) -> Option<Self> {
    let mut prev = self.point_ent.prev();
    while let Some(ent) = prev {
      match self.table.validate(self.query_version, ent) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(ent) => prev = ent.prev(),
      }
    }
    None
  }
}

impl<'a, S, C, T> Entry<'a, S, C, T>
where
  S: State,
  S::Data<'a, T::Value<'a>>: Transformable,
  T: TypeMode,
{
  #[inline]
  pub(crate) fn new(
    table: &'a Table<C, T>,
    query_version: u64,
    point_ent: PointEntry<'a, S, C, T>,
    key: <T::Key<'a> as Pointee<'a>>::Output,
    val: Option<S::Data<'a, T::Value<'a>>>,
    version: u64,
  ) -> Self {
    Self {
      table,
      point_ent,
      key,
      val,
      version,
      query_version,
    }
  }
}

impl<'a, S, C, T> Entry<'a, S, C, T>
where
  C: 'static,
  S: State,
  S::Data<'a, LazyRef<'a, ()>>: skl::Transformable<Input = Option<&'a [u8]>>,
  S::Data<'a, T::Value<'a>>: skl::Transformable<Input = Option<&'a [u8]>> + 'a,
  T: TypeMode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: PointComparator<C> + TypeRefComparator<RecordPointer>,
{
  #[inline]
  fn value_in(&self) -> <S::Data<'a, T::Value<'a>> as Transformable>::Output {
    match self.val.as_ref() {
      Some(val) => val.transform(),
      None => self.point_ent.value(),
    }
  }
}

impl<'a, S, C, T> WithVersion for Entry<'a, S, C, T>
where
  S: State,
  S::Data<'a, T::Value<'a>>: Transformable,
  T: TypeMode,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version
  }
}
