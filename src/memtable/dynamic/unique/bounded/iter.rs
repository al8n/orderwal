use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesComparator, Active};

use crate::{memtable::dynamic::unique::DynamicMemtable, types::Kind};

use super::{Entry, IterPoints, RangePoints, Table};

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, C, T>
where
  C: 'static,
  T: Kind,
{
  table: &'a Table<C>,
  iter: IterPoints<'a, Active, C, T>,
}

impl<'a, C, T> Iter<'a, C, T>
where
  C: BytesComparator,
  T: Kind,
{
  pub(super) fn new(table: &'a Table<C>) -> Self {
    Self {
      iter: table.iter_points(),
      table,
    }
  }
}

impl<'a, C, T> Iterator for Iter<'a, C, T>
where
  C: BytesComparator,
  T: Kind,
{
  type Item = Entry<'a, Active, C, T>;

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

impl<C, T> DoubleEndedIterator for Iter<'_, C, T>
where
  C: BytesComparator,
  T: Kind,
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
pub struct Range<'a, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
  T: Kind,
{
  table: &'a Table<C>,
  iter: RangePoints<'a, Active, Q, R, C, T>,
}

impl<'a, Q, R, C, T> Range<'a, Q, R, C, T>
where
  C: BytesComparator + 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
  T: Kind,
{
  pub(super) fn new(table: &'a Table<C>, r: R) -> Self {
    Self {
      iter: table.range_points(r),
      table,
    }
  }
}

impl<'a, Q, R, C, T> Iterator for Range<'a, Q, R, C, T>
where
  C: BytesComparator + 'static,
  R: RangeBounds<Q>,
  Q: ?Sized + Borrow<[u8]>,
  T: Kind,
{
  type Item = Entry<'a, Active, C, T>;

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

impl<Q, R, C, T> DoubleEndedIterator for Range<'_, Q, R, C, T>
where
  R: RangeBounds<Q>,
  Q: ?Sized + Borrow<[u8]>,
  C: BytesComparator + 'static,
  T: Kind,
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
