use core::ops::{Bound, RangeBounds};

use crate::types::{Query, RecordPointer};

pub(crate) mod bounded;
pub(crate) mod unbounded;

/// Memtables for dynamic(bytes) key-value order WALs.
pub mod dynamic;

/// Memtables for generic(structured) key-value order WALs.
pub mod generic;

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

trait RangeEntryExt<'a>: RangeEntry<'a> {
  /// Returns the start bound of the range entry.
  fn query_start_bound(&self) -> Bound<Query<Self::Key>> {
    match self.start_bound() {
      Bound::Included(key) => Bound::Included(Query(key)),
      Bound::Excluded(key) => Bound::Excluded(Query(key)),
      Bound::Unbounded => Bound::Unbounded,
    }
  }

  /// Returns the end bound of the range entry.
  fn query_end_bound(&self) -> Bound<Query<Self::Key>> {
    match self.end_bound() {
      Bound::Included(key) => Bound::Included(Query(key)),
      Bound::Excluded(key) => Bound::Excluded(Query(key)),
      Bound::Unbounded => Bound::Unbounded,
    }
  }

  /// Returns the range of the entry.
  fn query_range(&self) -> impl RangeBounds<Query<Self::Key>> + 'a {
    (self.query_start_bound(), self.query_end_bound())
  }
}

impl<'a, T> RangeEntryExt<'a> for T where T: RangeEntry<'a> {}

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
}

/// A memory table which is used to store pointers to the underlying entries.
pub trait MutableMemtable: Memtable {
  /// Inserts a pointer into the memtable.
  fn insert(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Removes the pointer associated with the key.
  fn remove(&self, version: u64, key: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts a range deletion pointer into the memtable, a range deletion is a deletion of a range of keys,
  /// which means that keys in the range are marked as deleted.
  ///
  /// This is not a contra operation to [`range_set`](MultipleVersionMemtable::range_set).
  /// See also [`range_set`](MultipleVersionMemtable::range_set) and [`range_set`](MultipleVersionMemtable::range_unset).
  fn range_remove(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts an range update pointer into the memtable.
  fn range_set(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Unset a range from the memtable, this is a contra operation to [`range_set`](MultipleVersionMemtable::range_set).
  fn range_unset(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;
}

/// Transformable
pub trait Transformable: sealed::Sealed {
  /// The output type of this transform.
  type Output;

  /// Returns the output after transformring.
  fn transform(&self) -> <Self as Transformable>::Output;
}

impl<T> Transformable for T
where
  T: sealed::Sealed,
{
  type Output = <T as sealed::Sealed>::Output;
  
  #[inline]
  fn transform(&self) -> <Self as Transformable>::Output {
    <T as sealed::Sealed>::transform(self)
  }
}

mod sealed {
  pub trait Sealed {
    type Input;
    type Output;

    /// Returns the input state.
    fn input(&self) -> Self::Input;

    /// Converts the input state to the state.
    fn from_input(input: Self::Input) -> Self
    where
      Self: Sized;

    /// Returns the output after transformring.
    fn transform(&self) -> Self::Output;
  }

  const _: () = {
    use dbutils::types::{Type, LazyRef};

    impl Sealed for &[u8] {
      type Input = Option<Self>;
      type Output = Self;
  
      #[inline]
      fn input(&self) -> Self::Input {
        Some(self)
      }
  
      #[inline]
      fn from_input(input: Self::Input) -> Self {
        input.expect("entry in Active state must have value")
      }
  
      #[inline]
      fn transform(&self) -> Self::Output {
        self
      }
    }
  
    impl<'a, T> Sealed for LazyRef<'a, T>
    where
      T: Type + ?Sized,
    {
      type Input = Option<&'a [u8]>;
      type Output = T::Ref<'a>;
  
      #[inline]
      fn input(&self) -> Self::Input {
        Some(self.raw().expect("entry in Active state must have value"))
      }
  
      #[inline]
      fn from_input(input: Self::Input) -> Self {
        unsafe { LazyRef::from_raw(input.expect("entry in Active state must have value")) }
      }
  
      #[inline]
      fn transform(&self) -> Self::Output {
        *self.get()
      }
    }
  
    impl Sealed for Option<&[u8]> {
      type Input = Self;
      type Output = Self;
  
      #[inline]
      fn input(&self) -> Self::Input {
        *self
      }
  
      #[inline]
      fn from_input(input: Self::Input) -> Self {
        input
      }
  
      #[inline]
      fn transform(&self) -> Self::Output {
        self.as_ref().copied()
      }
    }
  
    impl<'a, T> Sealed for Option<LazyRef<'a, T>>
    where
      T: Type + ?Sized,
    {
      type Input = Option<&'a [u8]>;
      type Output = Option<T::Ref<'a>>;
  
      #[inline]
      fn input(&self) -> Self::Input {
        self
          .as_ref()
          .map(|v| v.raw().expect("entry must have a raw value"))
      }
  
      #[inline]
      fn from_input(input: Self::Input) -> Self {
        input.map(|v| unsafe { LazyRef::from_raw(v) })
      }
  
      #[inline]
      fn transform(&self) -> Self::Output {
        self.as_ref().map(|v| *v.get())
      }
    }
  };
}
