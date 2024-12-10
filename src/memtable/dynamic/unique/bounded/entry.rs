use core::ops::ControlFlow;

use skl::{dynamic::BytesComparator, Active};

use crate::{
  memtable::MemtableEntry,
  types::{sealed::Pointee, Dynamic, Kind},
  State,
};

use super::PointEntry;

/// Entry in the memtable.
pub struct Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  table: &'a super::Table<C>,
  point_ent: PointEntry<'a, S, C, T>,
  key: <T::Key<'a> as Pointee<'a>>::Output,
  val: <T::Value<'a> as Pointee<'a>>::Output,
}

impl<'a, S, C, T> core::fmt::Debug for Entry<'a, S, C, T>
where
  S: State<'a>,
  S::BytesValueOutput: core::fmt::Debug,
  C: BytesComparator,
  T: Kind,
  <T::Key<'a> as Pointee<'a>>::Output: core::fmt::Debug,
  <T::Value<'a> as Pointee<'a>>::Output: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.val)
      .finish()
  }
}

impl<'a, S, C, T> Clone for Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
  T::Key<'a>: Clone,
  T::Value<'a>: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      table: self.table,
      point_ent: self.point_ent.clone(),
      key: self.key,
      val: self.val,
    }
  }
}

impl<'a, C> MemtableEntry<'a> for Entry<'a, Active, C, Dynamic>
where
  C: BytesComparator + 'static,
{
  type Key = &'a [u8];

  type Value = &'a [u8];

  #[inline]
  fn key(&self) -> Self::Key {
    self.key
  }

  #[inline]
  fn value(&self) -> Self::Value {
    self.val
  }

  #[inline]
  fn next(&self) -> Option<Self> {
    let mut next = self.point_ent.next();
    while let Some(ent) = next {
      match self.table.validate(ent) {
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
      match self.table.validate(ent) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(ent) => prev = ent.prev(),
      }
    }
    None
  }
}

impl<'a, S, C, T> Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  #[inline]
  pub(crate) fn new(
    table: &'a super::Table<C>,
    point_ent: PointEntry<'a, S, C, T>,
    key: <T::Key<'a> as Pointee<'a>>::Output,
    val: <T::Value<'a> as Pointee<'a>>::Output,
  ) -> Self {
    Self {
      table,
      point_ent,
      key,
      val,
    }
  }
}
