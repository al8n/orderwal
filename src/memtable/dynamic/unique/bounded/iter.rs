use core::{
  borrow::Borrow,
  ops::{ControlFlow, RangeBounds},
};

use skl::{dynamic::BytesComparator, generic::unique::Map as _, Active};

use super::{Dynamic, Entry, IterPoints, RangePoints, Table};

/// An iterator over the entries of a `Memtable`.
pub struct Iter<'a, C>
where
  C: 'static,
{
  table: &'a Table<C, Dynamic>,
  iter: IterPoints<'a, Active, C, Dynamic>,
}

impl<'a, C> Iter<'a, C>
where
  C: 'static,
{
  pub(super) fn new(table: &'a Table<C, Dynamic>) -> Self {
    Self {
      iter: IterPoints::new(table.skl.iter()),
      table,
    }
  }
}

impl<'a, C> Iterator for Iter<'a, C>
where
  C: BytesComparator + 'static,
{
  type Item = Entry<'a, Active, C, Dynamic>;

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

/// An iterator over the entries of a `Memtable`.
pub struct Range<'a, Q, R, C>
where
  R: RangeBounds<Q>,
  Q: ?Sized,
{
  table: &'a Table<C, Dynamic>,
  iter: RangePoints<'a, Active, Q, R, C, Dynamic>,
}

impl<'a, Q, R, C> Range<'a, Q, R, C>
where
  C: 'static,
  R: RangeBounds<Q> + 'a,
  Q: ?Sized + Borrow<[u8]>,
{
  pub(super) fn new(table: &'a Table<C, Dynamic>, r: R) -> Self {
    Self {
      iter: RangePoints::new(table.skl.range(r)),
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
