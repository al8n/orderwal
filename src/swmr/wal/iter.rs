use core::{borrow::Borrow, ops::RangeBounds};

use crossbeam_skiplist::Comparable;
use dbutils::Comparator;

use super::Pointer;

/// An iterator over the entries in the WAL.
pub struct Iter<'a, C> {
  iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Iter<'a, C> {
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C: Comparator> Iterator for Iter<'a, C> {
  type Item = (&'a [u8], &'a [u8]);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice()))
  }
}

impl<'a, C: Comparator> DoubleEndedIterator for Iter<'a, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice()))
  }
}

/// An iterator over the keys in the WAL.
pub struct Keys<'a, C> {
  iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Keys<'a, C> {
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C: Comparator> Iterator for Keys<'a, C> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_key_slice())
  }
}

impl<'a, C: Comparator> DoubleEndedIterator for Keys<'a, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_key_slice())
  }
}

/// An iterator over the values in the WAL.
pub struct Values<'a, C> {
  iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Values<'a, C> {
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C: Comparator> Iterator for Values<'a, C> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_value_slice())
  }
}

impl<'a, C: Comparator> DoubleEndedIterator for Values<'a, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_value_slice())
  }
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  iter: crossbeam_skiplist::set::Range<'a, Q, R, Pointer<C>>,
}

impl<'a, Q, R, C> Range<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Range<'a, Q, R, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, Q, R, C> Iterator for Range<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  type Item = (&'a [u8], &'a [u8]);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next()
      .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice()))
  }
}

impl<'a, Q, R, C> DoubleEndedIterator for Range<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice()))
  }
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  iter: crossbeam_skiplist::set::Range<'a, Q, R, Pointer<C>>,
}

impl<'a, Q, R, C> RangeKeys<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Range<'a, Q, R, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, Q, R, C> Iterator for RangeKeys<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_key_slice())
  }
}

impl<'a, Q, R, C> DoubleEndedIterator for RangeKeys<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_key_slice())
  }
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  iter: crossbeam_skiplist::set::Range<'a, Q, R, Pointer<C>>,
}

impl<'a, Q, R, C> RangeValues<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  #[inline]
  pub(super) fn new(iter: crossbeam_skiplist::set::Range<'a, Q, R, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, Q, R, C> Iterator for RangeValues<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_value_slice())
  }
}

impl<'a, Q, R, C> DoubleEndedIterator for RangeValues<'a, Q, R, C>
where
  C: Comparator,
  R: RangeBounds<Q>,
  [u8]: Borrow<Q>,
  Q: Ord + ?Sized + Comparable<[u8]>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_value_slice())
  }
}
