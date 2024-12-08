use skl::dynamic::BytesComparator;

use crate::State;

use super::PointEntry;

/// a
pub struct Entry<'a, S, C>
where
  S: State<'a>,
{
  table: &'a super::Table<C>,
  point_ent: PointEntry<'a, S, C>,
  key: &'a [u8],
  val: S::BytesValueOutput,
  version: u64,
  query_version: u64,
}

impl<'a, S, C> core::fmt::Debug for Entry<'a, S, C>
where
  S: State<'a>,
  S::BytesValueOutput: core::fmt::Debug,
  C: BytesComparator,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.val)
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, S, C> Clone for Entry<'a, S, C>
where
  S: State<'a>,
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

impl<'a, S, C> Entry<'a, S, C>
where
  S: State<'a>,
{
  #[inline]
  pub(crate) fn new(
    table: &'a super::Table<C>,
    query_version: u64,
    point_ent: PointEntry<'a, S, C>,
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
