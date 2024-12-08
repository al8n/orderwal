use core::ops::{Bound, RangeBounds};

#[macro_use]
mod bounded;

/// Memtables which only support unique keys.
pub mod unique;

/// Memtables which support multiple versions.
pub mod multiple_version;

mod comparator;
mod range_comparator;

pub(super) use comparator::MemtableComparator;
pub(super) use range_comparator::MemtableRangeComparator;

/// An entry which is stored in the memory table.
pub trait MemtableEntry<'a>
where
  Self: Sized,
{
  /// The value type.
  type Value;

  /// Returns the key in the entry.
  fn key(&self) -> &'a [u8];

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value;

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>;
}

/// An range entry which is stored in the memory table.
pub trait RangeEntry<'a>: Sized {
  /// Returns the start bound of the range entry.
  fn start_bound(&self) -> Bound<&'a [u8]>;

  /// Returns the end bound of the range entry.
  fn end_bound(&self) -> Bound<&'a [u8]>;

  /// Returns the range of the entry.
  fn range(&self) -> impl RangeBounds<[u8]> + 'a {
    (self.start_bound(), self.end_bound())
  }

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>;
}

/// An entry which is stored in the memory table.
pub trait RangeDeletionEntry<'a>: RangeEntry<'a> {}

/// An entry which is stored in the memory table.
pub trait RangeUpdateEntry<'a>
where
  Self: RangeEntry<'a>,
{
  /// The value type.
  type Value;

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value;
}
