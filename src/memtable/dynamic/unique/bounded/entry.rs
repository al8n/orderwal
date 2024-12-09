use skl::dynamic::BytesComparator;

use crate::{
  types::{sealed::Pointee, Kind},
  State,
};

use super::PointEntry;

/// a
pub struct Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  table: &'a super::Table<C, T>,
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

impl<'a, S, C, T> Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  #[inline]
  pub(crate) fn new(
    table: &'a super::Table<C, T>,
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

  /// Returns the key in the entry.
  #[inline]
  pub const fn key(&self) -> <T::Key<'a> as Pointee<'a>>::Output {
    self.key
  }

  /// Returns the value in the entry.
  #[inline]
  pub const fn value(&self) -> <T::Value<'a> as Pointee<'a>>::Output {
    self.val
  }
}
