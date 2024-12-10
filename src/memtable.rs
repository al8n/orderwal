use core::ops::{Bound, RangeBounds};

use crate::types::{Mode, RecordPointer};

#[macro_use]
pub(crate) mod bounded;

/// Memtables for dynamic(bytes) key-value order WALs.
pub mod dynamic;

/// Memtables for generic(structured) key-value order WALs.
pub mod generic;

#[derive(ref_cast::RefCast)]
#[repr(transparent)]
struct Query<Q: ?Sized>(Q);

/// An entry which is stored in the memory table.
pub trait MemtableEntry<'a>
where
  Self: Sized,
{
  /// The key type.
  type Key: 'a;

  /// The value type.
  type Value: 'a;

  /// Returns the key in the entry.
  fn key(&self) -> Self::Key;

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value;

  /// Returns the next entry in the memory table.
  fn next(&self) -> Option<Self>;

  /// Returns the previous entry in the memory table.
  fn prev(&self) -> Option<Self>;
}

/// An range entry which is stored in the memory table.
pub trait RangeEntry<'a>
where
  Self: Sized,
{
  /// The key type.
  type Key: 'a;

  /// Returns the start bound of the range entry.
  fn start_bound(&self) -> Bound<Self::Key>;

  /// Returns the end bound of the range entry.
  fn end_bound(&self) -> Bound<Self::Key>;

  /// Returns the range of the entry.
  fn range(&self) -> impl RangeBounds<Self::Key> + 'a {
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
  type Value: 'a;

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value;
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait Memtable {
  /// The configuration options for the memtable.
  type Options;

  /// The error type may be returned when constructing the memtable.
  type Error;

  /// Creates a new memtable with the specified options.
  fn new<A>(arena: A, opts: Self::Options) -> Result<Self, Self::Error>
  where
    Self: Sized,
    A: rarena_allocator::Allocator;

  /// Returns the total number of entries in the memtable.
  fn len(&self) -> usize;

  /// Returns `true` if the memtable is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Inserts a pointer into the memtable.
  fn insert(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Removes the pointer associated with the key.
  fn remove(&self, version: Option<u64>, key: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts a range deletion pointer into the memtable, a range deletion is a deletion of a range of keys,
  /// which means that keys in the range are marked as deleted.
  ///
  /// This is not a contra operation to [`range_set`](MultipleVersionMemtable::range_set).
  /// See also [`range_set`](MultipleVersionMemtable::range_set) and [`range_set`](MultipleVersionMemtable::range_unset).
  fn range_remove(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts an range update pointer into the memtable.
  fn range_set(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Unset a range from the memtable, this is a contra operation to [`range_set`](MultipleVersionMemtable::range_set).
  fn range_unset(&self, version: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Returns the kind of the memtable.
  fn mode() -> Mode;
}
