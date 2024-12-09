use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesComparator, Active};

use crate::memtable::dynamic::multiple_version::DynamicMemtable;

use super::{Dynamic, Entry, IterPoints, RangePoints, Table};

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, C>
where
  C: 'static,
{
  table: &'a Table<C, Dynamic>,
  iter: IterPoints<'a, Active, C, Dynamic>,
  query_version: u64,
}

impl<'a, C> Iter<'a, C>
where
  C: BytesComparator,
{
  pub(super) fn new(version: u64, table: &'a Table<C, Dynamic>) -> Self {
    Self {
      iter: table.iter_points(version),
      query_version: version,
      table,
    }
  }
}

impl<'a, C> Iterator for Iter<'a, C>
where
  C: BytesComparator,
{
  type Item = Entry<'a, Active, C, Dynamic>;

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

impl<C> DoubleEndedIterator for Iter<'_, C>
where
  C: BytesComparator,
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
pub struct Range<'a, Q, R, C>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
{
  table: &'a Table<C, Dynamic>,
  iter: RangePoints<'a, Active, Q, R, C, Dynamic>,
  query_version: u64,
}

impl<'a, Q, R, C> Range<'a, Q, R, C>
where
  C: BytesComparator + 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
{
  pub(super) fn new(version: u64, table: &'a Table<C, Dynamic>, r: R) -> Self {
    Self {
      iter: table.range_points(version, r),
      query_version: version,
      table,
    }
  }
}

impl<'a, Q, R, C> Iterator for Range<'a, Q, R, C>
where
  C: BytesComparator + 'static,
  R: RangeBounds<Q>,
  Q: ?Sized + Borrow<[u8]>,
{
  type Item = Entry<'a, Active, C, Dynamic>;

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

impl<Q, R, C> DoubleEndedIterator for Range<'_, Q, R, C>
where
  R: RangeBounds<Q>,
  Q: ?Sized + Borrow<[u8]>,
  C: BytesComparator + 'static,
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
