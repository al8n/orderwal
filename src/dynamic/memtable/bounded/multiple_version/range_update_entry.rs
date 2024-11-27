use core::ops::Bound;

use skl::dynamic::{
  multiple_version::sync::{Entry, VersionedEntry},
  Comparator,
};

use crate::dynamic::memtable::{bounded::MemtableRangeComparator, RangeBaseEntry};

/// Range update entry.
pub struct RangeUpdateEntry<'a, C>(Entry<'a, MemtableRangeComparator<C>>);

impl<C> Clone for RangeUpdateEntry<'_, C> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<C> Copy for RangeUpdateEntry<'_, C> {}

impl<'a, C> RangeBaseEntry<'a> for RangeUpdateEntry<'a, C>
where
  C: Comparator,
{
  #[inline]
  fn start_bound(&self) -> Bound<&'a [u8]> {
    todo!()
  }

  #[inline]
  fn end_bound(&self) -> Bound<&'a [u8]> {
    todo!()
  }

  #[inline]
  fn next(&mut self) -> Option<Self> {
    self.0.next().map(Self)
  }

  #[inline]
  fn prev(&mut self) -> Option<Self> {
    self.0.prev().map(Self)
  }
}

/// Range update entry which may have
pub struct MultipleVersionRangeUpdateEntry<'a, C>(VersionedEntry<'a, MemtableRangeComparator<C>>);

impl<C> Clone for MultipleVersionRangeUpdateEntry<'_, C> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<C> Copy for MultipleVersionRangeUpdateEntry<'_, C> {}
