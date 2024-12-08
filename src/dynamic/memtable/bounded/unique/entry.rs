use skl::dynamic::BytesComparator;

use super::PointEntry;

/// a
pub struct Entry<'a, C> {
  table: &'a super::Table<C>,
  point_ent: PointEntry<'a, C>,
  key: &'a [u8],
  val: &'a [u8],
}

impl<C> core::fmt::Debug for Entry<'_, C>
where
  C: BytesComparator,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key)
      .field("value", &self.val)
      .finish()
  }
}

impl<C> Clone for Entry<'_, C> {
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

impl<'a, C> Entry<'a, C> {
  #[inline]
  pub(crate) fn new(
    table: &'a super::Table<C>,
    point_ent: PointEntry<'a, C>,
    key: &'a [u8],
    val: &'a [u8],
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
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  /// Returns the value in the entry.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.val
  }
}
