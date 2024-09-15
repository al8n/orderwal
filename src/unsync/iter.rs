use core::iter::FusedIterator;
use std::collections::btree_set;

use super::*;

/// Iterator over the entries in the WAL.
pub struct Iter<'a, C> {
  iter: btree_set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Iter<'a, C> {
  #[inline]
  pub(super) fn new(iter: btree_set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C> Iterator for Iter<'a, C> {
  type Item = (&'a [u8], &'a [u8]);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| {
      let k = ptr.as_key_slice();
      let v = ptr.as_value_slice();
      (k, v)
    })
  }
}

impl<C> DoubleEndedIterator for Iter<'_, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| {
      let k = ptr.as_key_slice();
      let v = ptr.as_value_slice();
      (k, v)
    })
  }
}

impl<C> FusedIterator for Iter<'_, C> {}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, C> {
  iter: btree_set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Keys<'a, C> {
  #[inline]
  pub(super) fn new(iter: btree_set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C> Iterator for Keys<'a, C> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_key_slice())
  }
}

impl<C> DoubleEndedIterator for Keys<'_, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_key_slice())
  }
}

impl<C> FusedIterator for Keys<'_, C> {}

/// Iterator over the values in the WAL.
pub struct Values<'a, C> {
  iter: btree_set::Iter<'a, Pointer<C>>,
}

impl<'a, C> Values<'a, C> {
  #[inline]
  pub(super) fn new(iter: btree_set::Iter<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C> Iterator for Values<'a, C> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_value_slice())
  }
}

impl<C> DoubleEndedIterator for Values<'_, C> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_value_slice())
  }
}

impl<C> FusedIterator for Values<'_, C> {}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, C>
where
  C: Comparator,
{
  iter: btree_set::Range<'a, Pointer<C>>,
}

impl<'a, C> Range<'a, C>
where
  C: Comparator,
{
  #[inline]
  pub(super) fn new(iter: btree_set::Range<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C> Iterator for Range<'a, C>
where
  C: Comparator,
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

impl<C> DoubleEndedIterator for Range<'_, C>
where
  C: Comparator,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .iter
      .next_back()
      .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice()))
  }
}

impl<C> FusedIterator for Range<'_, C> where C: Comparator {}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, C>
where
  C: Comparator,
{
  iter: btree_set::Range<'a, Pointer<C>>,
}

impl<'a, C> RangeKeys<'a, C>
where
  C: Comparator,
{
  #[inline]
  pub(super) fn new(iter: btree_set::Range<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C> Iterator for RangeKeys<'a, C>
where
  C: Comparator,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_key_slice())
  }
}

impl<C> DoubleEndedIterator for RangeKeys<'_, C>
where
  C: Comparator,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_key_slice())
  }
}

impl<C> FusedIterator for RangeKeys<'_, C> where C: Comparator {}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, C>
where
  C: Comparator,
{
  iter: btree_set::Range<'a, Pointer<C>>,
}

impl<'a, C> RangeValues<'a, C>
where
  C: Comparator,
{
  #[inline]
  pub(super) fn new(iter: btree_set::Range<'a, Pointer<C>>) -> Self {
    Self { iter }
  }
}

impl<'a, C> Iterator for RangeValues<'a, C>
where
  C: Comparator,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|ptr| ptr.as_value_slice())
  }
}

impl<C> DoubleEndedIterator for RangeValues<'_, C>
where
  C: Comparator,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(|ptr| ptr.as_value_slice())
  }
}

impl<C> FusedIterator for RangeValues<'_, C> where C: Comparator {}
