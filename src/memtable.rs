use core::ops::{Bound, RangeBounds};
use dbutils::equivalent::Comparable;

use crate::error::Error;

/// Memtable implementation based on linked based [`SkipMap`][`crossbeam_skiplist`].
pub mod linked;

/// Memtable implementation based on ARNEA based [`SkipMap`](skl).
pub mod arena;

/// An entry which is stored in the memory table.
pub trait MemtableEntry<'a>: Sized {
  /// The pointer type.
  type Pointer;

  /// Returns the pointer associated with the entry.
  fn pointer(&self) -> &Self::Pointer;

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait Memtable {
  /// The pointer type.
  type Pointer;
  /// The item returned by the iterator or query methods.
  type Item<'a>: MemtableEntry<'a, Pointer = Self::Pointer>
  where
    Self::Pointer: 'a,
    Self: 'a;
  /// The iterator type.
  type Iterator<'a>: DoubleEndedIterator<Item = Self::Item<'a>>
  where
    Self::Pointer: 'a,
    Self: 'a;
  /// The range iterator type.
  type Range<'a, Q, R>: Iterator<Item = Self::Item<'a>>
  where
    Self::Pointer: 'a,
    Self: 'a,
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;
  
  /// The configuration options for the memtable.
  type Options;
  
  /// Creates a new memtable with the specified options.
  fn new(opts: Self::Options) -> Result<Self, Error> where Self: Sized;

  /// Returns the number of entries in the memtable.
  fn len(&self) -> usize;

  /// Returns `true` if the memtable is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Returns the upper bound of the memtable.
  fn upper_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>;

  /// Returns the lower bound of the memtable.
  fn lower_bound<Q>(&self, bound: Bound<&Q>) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>;

  /// Inserts a pointer into the memtable.
  fn insert(&mut self, ele: Self::Pointer) -> Result<(), Error>
  where
    Self::Pointer: Ord + 'static;

  /// Returns the first pointer in the memtable.
  fn first(&self) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord;

  /// Returns the last pointer in the memtable.
  fn last(&self) -> Option<Self::Item<'_>>
  where
    Self::Pointer: Ord;

  /// Returns the pointer associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<Self::Item<'_>>
  where
    Q: ?Sized + Comparable<Self::Pointer>;

  /// Returns `true` if the memtable contains the specified pointer.
  fn contains<Q>(&self, key: &Q) -> bool
  where
    Q: ?Sized + Comparable<Self::Pointer>;

  /// Returns an iterator over the memtable.
  fn iter(&self) -> Self::Iterator<'_>;

  /// Returns an iterator over a subset of the memtable.
  fn range<'a, Q, R>(&'a self, range: R) -> Self::Range<'a, Q, R>
  where
    R: RangeBounds<Q> + 'a,
    Q: ?Sized + Comparable<Self::Pointer>;
}
