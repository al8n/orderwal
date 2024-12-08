use crate::types::{Mode, RecordPointer};

#[macro_use]
mod bounded;

/// Memtables for dynamic(bytes) key-value order WALs.
pub mod dynamic;

/// Memtables for generic(structured) key-value order WALs.
pub mod generic;

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

mod sealed {
  pub trait ComparatorConstructor<C: ?Sized>: Sized {
    fn new(ptr: *const u8, cmp: triomphe::Arc<C>) -> Self;
  }

  pub trait Sealed {
    type GenericComparator<K: ?Sized>: ComparatorConstructor<Self>;
    type GenericRangeComparator<K: ?Sized>: ComparatorConstructor<Self>;
    type DynamicComparator: ComparatorConstructor<Self>;
    type DynamicRangeComparator: ComparatorConstructor<Self>;
  }

  impl<C> Sealed for C
  where
    C: ?Sized,
  {
    type GenericComparator<K: ?Sized> = crate::memtable::generic::MemtableComparator<K, C>;

    type GenericRangeComparator<K: ?Sized> =
      crate::memtable::generic::MemtableRangeComparator<K, C>;

    type DynamicComparator = crate::memtable::dynamic::MemtableComparator<C>;

    type DynamicRangeComparator = crate::memtable::dynamic::MemtableRangeComparator<C>;
  }
}
