use core::marker::PhantomData;

use skl::map::sync::{Iter as MapIter, Range as MapRange};

use super::Entry;


pub struct Iter<'a, P> {
  iter: MapIter<'a>,
  _p: PhantomData<P>,
}

impl<'a, P> Iterator for Iter<'a, P> {
  type Item = Entry<'a, P>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(Entry::new)
  }
}

impl<P> DoubleEndedIterator for Iter<'_, P> {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.iter.next_back().map(Entry::new)
  }
}

pub struct Range<'a, Q, R, P>
{
  range: MapRange<'a, Q, R>,
  _p: PhantomData<P>,
}

impl<'a, Q, R, P> Iterator for Range<'a, Q, R, P>
where
  Q: std::borrow::Borrow<[u8]>,
  R: std::ops::RangeBounds<Q>,
{
  type Item = Entry<'a, P>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    self.range.next().map(Entry::new)
  }
}

impl<Q, R, P> DoubleEndedIterator for Range<'_, Q, R, P>
where
  Q: std::borrow::Borrow<[u8]>,
  R: std::ops::RangeBounds<Q>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    self.range.next_back().map(Entry::new)
  }
}

