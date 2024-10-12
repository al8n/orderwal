use core::{iter::FusedIterator, marker::PhantomData};

use dbutils::CheapClone;

use super::{
  memtable::{Memtable, MemtableEntry},
  sealed::Pointer,
};

/// Iterator over the entries in the WAL.
pub struct Iter<'a, I, M: Memtable> {
  iter: I,
  version: Option<u64>,
  pointer: Option<M::Pointer>,
  _m: PhantomData<&'a ()>,
}

impl<I, M: Memtable> Iter<'_, I, M> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: I) -> Self {
    Self {
      version,
      iter,
      pointer: None,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub(super) const fn version(&self) -> Option<u64> {
    self.version
  }
}

impl<'a, I, M> Iterator for Iter<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: Iterator<Item = M::Item<'a>>,
{
  type Item = M::Item<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().inspect(|ent| {
        let ptr = ent.pointer();
        self.pointer = Some(ptr.cheap_clone());
      }),
      Some(version) => loop {
        match self.iter.next() {
          Some(ent) => {
            let next_pointer = ent.pointer();
            if let Some(ref pointer) = self.pointer {
              if next_pointer.version() <= version
                && next_pointer.as_key_slice() != pointer.as_key_slice()
              {
                self.pointer = Some(next_pointer.cheap_clone());
                return Some(ent);
              }
            } else if next_pointer.version() <= version {
              self.pointer = Some(next_pointer.cheap_clone());
              return Some(ent);
            }
          }
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, M> DoubleEndedIterator for Iter<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: DoubleEndedIterator<Item = M::Item<'a>>,
{
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().inspect(|ent| {
        let ptr = ent.pointer();
        self.pointer = Some(ptr.cheap_clone());
      }),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ent) => {
            let prev_pointer = ent.pointer();
            if let Some(ref pointer) = self.pointer {
              if prev_pointer.version() <= version
                && prev_pointer.as_key_slice() != pointer.as_key_slice()
              {
                self.pointer = Some(prev_pointer.cheap_clone());
                return Some(ent);
              }
            } else if prev_pointer.version() <= version {
              self.pointer = Some(prev_pointer.cheap_clone());
              return Some(ent);
            }
          }
          None => return None,
        }
      },
    }
  }
}

impl<'a, I, M> FusedIterator for Iter<'a, I, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  I: FusedIterator<Item = M::Item<'a>>,
{
}

/// An iterator over a subset of the entries in the WAL.
pub struct Range<'a, R, M: Memtable> {
  iter: R,
  version: Option<u64>,
  pointer: Option<M::Pointer>,
  _m: PhantomData<&'a ()>,
}

impl<R, M: Memtable> Range<'_, R, M> {
  #[inline]
  pub(super) fn new(version: Option<u64>, iter: R) -> Self {
    Self {
      version,
      iter,
      pointer: None,
      _m: PhantomData,
    }
  }

  /// Returns the query version of the iterator.
  #[inline]
  pub(super) const fn version(&self) -> Option<u64> {
    self.version
  }
}

impl<'a, R, M> Iterator for Range<'a, R, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  R: Iterator<Item = M::Item<'a>>,
{
  type Item = M::Item<'a>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next().inspect(|ent| {
        let ptr = ent.pointer();
        self.pointer = Some(ptr.cheap_clone());
      }),
      Some(version) => loop {
        match self.iter.next() {
          Some(ent) => {
            let next_pointer = ent.pointer();
            if let Some(ref pointer) = self.pointer {
              if next_pointer.version() <= version
                && next_pointer.as_key_slice() != pointer.as_key_slice()
              {
                self.pointer = Some(next_pointer.cheap_clone());
                return Some(ent);
              }
            } else if next_pointer.version() <= version {
              self.pointer = Some(next_pointer.cheap_clone());
              return Some(ent);
            }
          }
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, M> DoubleEndedIterator for Range<'a, R, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  R: DoubleEndedIterator<Item = M::Item<'a>>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.version {
      None => self.iter.next_back().inspect(|ent| {
        let ptr = ent.pointer();
        self.pointer = Some(ptr.cheap_clone());
      }),
      Some(version) => loop {
        match self.iter.next_back() {
          Some(ent) => {
            let prev_pointer = ent.pointer();
            if let Some(ref pointer) = self.pointer {
              if prev_pointer.version() <= version
                && prev_pointer.as_key_slice() != pointer.as_key_slice()
              {
                self.pointer = Some(prev_pointer.cheap_clone());
                return Some(ent);
              }
            } else if prev_pointer.version() <= version {
              self.pointer = Some(prev_pointer.cheap_clone());
              return Some(ent);
            }
          }
          None => return None,
        }
      },
    }
  }
}

impl<'a, R, M> FusedIterator for Range<'a, R, M>
where
  M: Memtable + 'static,
  M::Pointer: Pointer + CheapClone + 'static,
  R: FusedIterator<Item = M::Item<'a>>,
{
}
