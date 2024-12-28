use core::ops::ControlFlow;

use dbutils::{
  equivalentor::{Comparator, QueryComparator},
  state::{Active, MaybeTombstone, State},
};

use crate::{
  memtable::{
    sealed, Entry, RangeEntry, RawEntry, Transfer,
  },
  types::{
    sealed::{PointComparator, Pointee, RangeComparator}, Mode, Query, RecordPointer, RefQuery, Remove, Update
  },
};

use super::{PointEntryRef, RangeEntryRef, Table};

/// Entry in the memtable.
pub struct EntryRef<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  table: &'a Table<C, T>,
  point_ent: PointEntryRef<'a, S, C, T>,
  key: <T::Key<'a> as Pointee<'a>>::Output,
  val: Option<S::Data<'a, T::Value<'a>>>,
  version: u64,
  query_version: u64,
}

impl<'a, S, C, T> core::fmt::Debug for EntryRef<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: core::fmt::Debug,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  <T::Key<'a> as Pointee<'a>>::Output: core::fmt::Debug,
  T::Comparator<C>: PointComparator<C> + Comparator<RecordPointer>,
  PointEntryRef<'a, S, C, T>:
    Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output, Value = S::Data<'a, S::Value>>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.value_in())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, S, C, T> Clone for EntryRef<'a, S, C, T>
where
  S: State,
  S::Data<'a, T::Value<'a>>: Clone,
  PointEntryRef<'a, S, C, T>: Clone,
  T: Mode,
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

impl<'a, S, C, T> RawEntry<'a> for EntryRef<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  S::Data<'a, &'a [u8]>: 'a,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: PointComparator<C> + Comparator<RecordPointer>,
  PointEntryRef<'a, S, C, T>: RawEntry<'a, RawValue = S::Data<'a, &'a [u8]>>,
{
  type RawValue = S::Data<'a, &'a [u8]>;

  #[inline]
  fn raw_key(&self) -> &'a [u8] {
    self.point_ent.raw_key()
  }

  #[inline]
  fn raw_value(&self) -> Self::RawValue {
    match self.val.as_ref() {
      Some(val) => <S as sealed::Sealed<'_, T::Value<'_>>>::input(val),
      None => self.point_ent.raw_value(),
    }
  }
}

impl<'a, S, C, T> Entry<'a> for EntryRef<'a, S, C, T>
where
  C: 'static,
  S: Transfer<'a, T::Value<'a>>,
  MaybeTombstone: Transfer<'a, T::Value<'a>>,
  S::Data<'a, S::Value>: 'a,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]>,
  T::Comparator<C>: PointComparator<C>
    + Comparator<RecordPointer>
    + Comparator<Query<<T::Key<'a> as Pointee<'a>>::Output>>
    + 'static,
  T::RangeComparator<C>: Comparator<RecordPointer>
    + QueryComparator<RecordPointer, RefQuery<<T::Key<'a> as Pointee<'a>>::Output>>
    + RangeComparator<C>
    + 'static,
  PointEntryRef<'a, S, C, T>:
    Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output, Value = S::Data<'a, S::Value>>,
  RangeEntryRef<'a, Active, Remove, C, T>:
    RangeEntry<'a, Remove, Key = <T::Key<'a> as Pointee<'a>>::Output>,
  RangeEntryRef<'a, MaybeTombstone, Update, C, T>: RangeEntry<
    'a,
    Update,
    Key = <T::Key<'a> as Pointee<'a>>::Output,
    Value = <MaybeTombstone as State>::Data<
      'a,
      <MaybeTombstone as sealed::Sealed<'a, T::Value<'a>>>::Value,
    >,
  >,
{
  type Key = <T::Key<'a> as Pointee<'a>>::Output;

  type Value = S::Data<'a, S::Value>;

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

  #[inline]
  fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, S, C, T> EntryRef<'a, S, C, T>
where
  S: State,
  T: Mode,
{
  #[inline]
  pub(crate) fn new(
    table: &'a Table<C, T>,
    query_version: u64,
    point_ent: PointEntryRef<'a, S, C, T>,
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

impl<'a, S, C, T> EntryRef<'a, S, C, T>
where
  C: 'static,
  S: State,
  S: Transfer<'a, T::Value<'a>>,
  T: Mode,
  T::Key<'a>: Pointee<'a, Input = &'a [u8]> + 'a,
  T::Comparator<C>: PointComparator<C> + Comparator<RecordPointer>,
  PointEntryRef<'a, S, C, T>:
    Entry<'a, Key = <T::Key<'a> as Pointee<'a>>::Output, Value = S::Data<'a, S::Value>>,
{
  #[inline]
  fn value_in(&self) -> S::Data<'a, S::Value> {
    match self.val.as_ref() {
      Some(val) => <S as sealed::Sealed<'_, T::Value<'_>>>::transfer(val),
      None => self.point_ent.value(),
    }
  }
}
