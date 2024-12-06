use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesComparator, Active};

use crate::dynamic::memtable::MultipleVersionMemtable;

use super::{Entry, IterPoints, MultipleVersionTable, RangePoints};

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, C>
where
  C: 'static,
{
  table: &'a MultipleVersionTable<C>,
  iter: IterPoints<'a, Active, C>,
  query_version: u64,
}

impl<'a, C> Iter<'a, C>
where
  C: BytesComparator,
{
  pub(super) fn new(version: u64, table: &'a MultipleVersionTable<C>) -> Self {
    Self {
      iter: table.point_iter(version),
      query_version: version,
      table,
    }
  }
}

impl<'a, C> Iterator for Iter<'a, C>
where
  C: BytesComparator,
{
  type Item = Entry<'a, Active, C>;

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
  table: &'a MultipleVersionTable<C>,
  iter: RangePoints<'a, Active, Q, R, C>,
  query_version: u64,
}

impl<'a, Q, R, C> Range<'a, Q, R, C>
where
  C: BytesComparator + 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
{
  pub(super) fn new(version: u64, table: &'a MultipleVersionTable<C>, r: R) -> Self {
    Self {
      iter: table.point_range(version, r),
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
  type Item = Entry<'a, Active, C>;

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
