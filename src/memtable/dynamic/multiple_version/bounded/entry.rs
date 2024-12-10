use skl::dynamic::BytesComparator;

use crate::{types::Kind, State, WithVersion};

use super::PointEntry;

/// a
pub struct Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  table: &'a super::Table<C, T>,
  point_ent: PointEntry<'a, S, C, T>,
  key: &'a [u8],
  val: S::BytesValueOutput,
  version: u64,
  query_version: u64,
}

impl<'a, S, C, T> core::fmt::Debug for Entry<'a, S, C, T>
where
  S: State<'a>,
  S::BytesValueOutput: core::fmt::Debug,
  C: BytesComparator,
  T: Kind,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.val)
      .field("version", &self.version)
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
      version: self.version,
      query_version: self.query_version,
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
    query_version: u64,
    point_ent: PointEntry<'a, S, C, T>,
    key: &'a [u8],
    val: S::BytesValueOutput,
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

  /// Returns the key in the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  /// Returns the value in the entry.
  #[inline]
  pub const fn value(&self) -> S::BytesValueOutput {
    self.val
  }

  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }
}

impl<'a, S, C, T> WithVersion for Entry<'a, S, C, T>
where
  S: State<'a>,
  T: Kind,
{
  #[inline]
  fn version(&self) -> u64 {
    self.version
  }
}
