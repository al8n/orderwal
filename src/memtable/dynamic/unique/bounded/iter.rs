use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesComparator, Active};

use crate::memtable::dynamic::unique::DynamicMemtable;

use super::{Entry, IterPoints, RangePoints, Table};

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, C>
where
  C: 'static,
{
  table: &'a Table<C>,
  iter: IterPoints<'a, Active, C>,
}

impl<'a, C> Iter<'a, C>
where
  C: BytesComparator,
{
  pub(super) fn new(table: &'a Table<C>) -> Self {
    Self {
      iter: table.iter_points(),
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
      match self.table.validate(next) {
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
      match self.table.validate(prev) {
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
  table: &'a Table<C>,
  iter: RangePoints<'a, Active, Q, R, C>,
}

impl<'a, Q, R, C> Range<'a, Q, R, C>
where
  C: BytesComparator + 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
{
  pub(super) fn new(table: &'a Table<C>, r: R) -> Self {
    Self {
      iter: table.range_points(r),
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
      match self.table.validate(next) {
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
      match self.table.validate(prev) {
        ControlFlow::Break(entry) => return entry,
        ControlFlow::Continue(_) => continue,
      }
    }
  }
}
