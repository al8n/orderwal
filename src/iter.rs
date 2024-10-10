use core::iter::FusedIterator;

use super::*;

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, P> {
  iter: I,
  version: Option<u64>,
  _m: PhantomData<&'a P>,
}

impl<I, P> Iter<'_, I, P> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: I) -> Self {
    Self {
      version,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, I, P> Iterator for Iter<'a, I, P>
where
  P: sealed::Pointer,
  I: Iterator<Item = &'a P>,
{
  type Item = (&'a [u8], &'a [u8]);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().map(|ptr| {
        let k = ptr.as_key_slice();
        let v = ptr.as_value_slice();
        (k, v)
      }),
      Some(version) => loop {
        match self.iter.next() {
          Some(ptr) if ptr.version() <= version => {
            let k = ptr.as_key_slice();
            let v = ptr.as_value_slice();
            return Some((k, v));
          }
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, P> DoubleEndedIterator for Iter<'a, I, P>
where
  P: sealed::Pointer,
  I: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().map(|ptr| {
        let k = ptr.as_key_slice();
        let v = ptr.as_value_slice();
        (k, v)
      }),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ptr) if ptr.version() <= version => {
            let k = ptr.as_key_slice();
            let v = ptr.as_value_slice();
            return Some((k, v));
          }
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, P> FusedIterator for Iter<'a, I, P>
where
  P: sealed::Pointer,
  I: FusedIterator<Item = &'a P>,
{
}

/// Iterator over the keys in the WAL.
pub struct Keys<'a, I, P> {
  iter: I,
  version: Option<u64>,
  _m: PhantomData<&'a P>,
}

impl<I, P> Keys<'_, I, P> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: I) -> Self {
    Self {
      version,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, I, P> Iterator for Keys<'a, I, P>
where
  P: sealed::Pointer,
  I: Iterator<Item = &'a P>,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().map(|ptr| ptr.as_key_slice()),
      Some(version) => loop {
        match self.iter.next() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_key_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, P> DoubleEndedIterator for Keys<'a, I, P>
where
  P: sealed::Pointer,
  I: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().map(|ptr| ptr.as_key_slice()),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_key_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, P> FusedIterator for Keys<'a, I, P>
where
  P: sealed::Pointer,
  I: FusedIterator<Item = &'a P>,
{
}

/// Iterator over the values in the WAL.
pub struct Values<'a, I, P> {
  iter: I,
  version: Option<u64>,
  _m: PhantomData<&'a P>,
}

impl<I, P> Values<'_, I, P> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: I) -> Self {
    Self {
      version,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, I, P> Iterator for Values<'a, I, P>
where
  P: sealed::Pointer,
  I: Iterator<Item = &'a P>,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().map(|ptr| ptr.as_value_slice()),
      Some(version) => loop {
        match self.iter.next() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_value_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, P> DoubleEndedIterator for Values<'a, I, P>
where
  P: sealed::Pointer,
  I: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().map(|ptr| ptr.as_value_slice()),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_value_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, P> FusedIterator for Values<'a, I, P>
where
  P: sealed::Pointer,
  I: FusedIterator<Item = &'a P>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, R, P> {
  iter: R,
  version: Option<u64>,
  _m: PhantomData<&'a P>,
}

impl<R, P> Range<'_, R, P> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: R) -> Self {
    Self {
      version,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, R, P> Iterator for Range<'a, R, P>
where
  P: sealed::Pointer,
  R: Iterator<Item = &'a P>,
{
  type Item = (&'a [u8], &'a [u8]);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self
        .iter
        .next()
        .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice())),
      Some(version) => loop {
        match self.iter.next() {
          Some(ptr) if ptr.version() <= version => {
            return Some((ptr.as_key_slice(), ptr.as_value_slice()))
          }
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, P> DoubleEndedIterator for Range<'a, R, P>
where
  P: sealed::Pointer,
  R: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self
        .iter
        .next_back()
        .map(|ptr| (ptr.as_key_slice(), ptr.as_value_slice())),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ptr) if ptr.version() <= version => {
            return Some((ptr.as_key_slice(), ptr.as_value_slice()))
          }
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, P> FusedIterator for Range<'a, R, P>
where
  P: sealed::Pointer,
  R: FusedIterator<Item = &'a P>,
{
}

/// An iterator over the keys in a subset of the entries in the WAL.
pub struct RangeKeys<'a, R, P> {
  iter: R,
  version: Option<u64>,
  _m: PhantomData<&'a P>,
}

impl<R, P> RangeKeys<'_, R, P> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: R) -> Self {
    Self {
      version,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, R, P> Iterator for RangeKeys<'a, R, P>
where
  P: sealed::Pointer,
  R: Iterator<Item = &'a P>,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().map(|ptr| ptr.as_key_slice()),
      Some(version) => loop {
        match self.iter.next() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_key_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, P> DoubleEndedIterator for RangeKeys<'a, R, P>
where
  P: sealed::Pointer,
  R: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().map(|ptr| ptr.as_key_slice()),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_key_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, P> FusedIterator for RangeKeys<'a, R, P>
where
  P: sealed::Pointer,
  R: FusedIterator<Item = &'a P>,
{
}

/// An iterator over the values in a subset of the entries in the WAL.
pub struct RangeValues<'a, R, P> {
  iter: R,
  version: Option<u64>,
  _m: PhantomData<&'a P>,
}

impl<R, P> RangeValues<'_, R, P> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: R) -> Self {
    Self {
      version,
      iter,
      _m: PhantomData,
    }
  }
}

impl<'a, R, P> Iterator for RangeValues<'a, R, P>
where
  P: sealed::Pointer,
  R: Iterator<Item = &'a P>,
{
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().map(|ptr| ptr.as_value_slice()),
      Some(version) => loop {
        match self.iter.next() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_value_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, P> DoubleEndedIterator for RangeValues<'a, R, P>
where
  P: sealed::Pointer,
  R: DoubleEndedIterator<Item = &'a P>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().map(|ptr| ptr.as_value_slice()),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ptr) if ptr.version() <= version => return Some(ptr.as_value_slice()),
          Some(_) => continue,
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, P> FusedIterator for RangeValues<'a, R, P>
where
  P: sealed::Pointer,
  R: FusedIterator<Item = &'a P>,
{
}
