use core::ops::{Bound, RangeBounds};

use crate::types::{Query, RecordPointer, WithValue};

#[cfg(feature = "skl")]
pub(crate) mod bounded;
#[cfg(feature = "crossbeam-skiplist-mvcc")]
pub(crate) mod unbounded;

/// Memtables for dynamic(bytes) key-value order WALs.
pub mod dynamic;

/// Memtables for generic(structured) key-value order WALs.
pub mod generic;

/// An entry which is stored in the memory table.
pub trait Entry<'a>
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

  /// Returns the version of the entry.
  fn version(&self) -> u64;
}

/// An entry which means that the entry can return key and value in bytes format.
pub trait RawEntry<'a>
where
  Self: Sized,
{
  /// The raw value type.
  type RawValue: 'a;

  /// Returns the raw key in the entry.
  fn raw_key(&self) -> &'a [u8];

  /// Returns the raw value in the entry.
  fn raw_value(&self) -> Self::RawValue;
}

/// A raw range entry which means that the entry can return start bound and ent bound in bytes format.
pub trait RawRangeEntry<'a, O>
where
  Self: Sized,
{
  /// The raw value type.
  type RawValue: 'a
  where
    O: WithValue;

  /// Returns the start bound of the range entry in bytes.
  fn raw_start_bound(&self) -> Bound<&'a [u8]>;

  /// Returns the end bound of the range entry in bytes.
  fn raw_end_bound(&self) -> Bound<&'a [u8]>;

  /// Returns the raw value in the entry.
  fn raw_value(&self) -> Self::RawValue
  where
    O: WithValue;
}

/// An range entry which is stored in the memory table.
pub trait RangeEntry<'a, O> {
  /// The key type.
  type Key: 'a;
  /// The value type.
  type Value: 'a
  where
    O: WithValue;

  /// Returns the start bound of the range entry.
  fn start_bound(&self) -> Bound<Self::Key>;

  /// Returns the end bound of the range entry.
  fn end_bound(&self) -> Bound<Self::Key>;

  /// Returns the value in the entry.
  fn value(&self) -> Self::Value
  where
    O: WithValue;

  /// Returns the range of the entry.
  fn range(&self) -> (Bound<Self::Key>, Bound<Self::Key>) {
    (self.start_bound(), self.end_bound())
  }

  /// Returns the next entry in the memory table.
  fn next(&mut self) -> Option<Self>
  where
    Self: Sized;

  /// Returns the previous entry in the memory table.
  fn prev(&mut self) -> Option<Self>
  where
    Self: Sized;

  /// Returns the version of the entry.
  fn version(&self) -> u64;
}

trait RangeEntryExt<'a, O>: RangeEntry<'a, O> {
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

impl<'a, O, T> RangeEntryExt<'a, O> for T where T: RangeEntry<'a, O> {}

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
  /// This is not a contra operation to [`range_set`](MutableMemtable::range_set).
  /// See also [`range_set`](MutableMemtable::range_set) and [`range_set`](MutableMemtable::range_unset).
  fn range_remove(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Inserts an range update pointer into the memtable.
  fn range_set(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;

  /// Unset a range from the memtable, this is a contra operation to [`range_set`](MutableMemtable::range_set).
  fn range_unset(&self, version: u64, pointer: RecordPointer) -> Result<(), Self::Error>;
}

/// Transfer trait for converting data between different states.
pub trait Transfer<'a, D>: sealed::Sealed<'a, D> {}

impl<'a, D, T> Transfer<'a, D> for T where T: sealed::Sealed<'a, D> {}

mod sealed {
  use dbutils::types::{LazyRef, Type};

  #[cfg(all(feature = "crossbeam-skiplist-mvcc", not(feature = "skl")))]
  pub trait Sealed<'a, I>:
    crossbeam_skiplist_mvcc::Transfer<
    'a,
    crate::types::RecordPointer,
    To = <Self as dbutils::state::State>::Data<'a, &'a crate::types::RecordPointer>,
  >
  {
    type Value;

    fn input(data: &Self::Data<'a, I>) -> Self::Data<'a, &'a [u8]>;

    fn from_input(input: Option<&'a [u8]>) -> Self::Data<'a, I>
    where
      Self: Sized;

    fn transfer(data: &Self::Data<'a, I>) -> Self::Data<'a, Self::Value>;

    fn leak<T>(data: Self::Data<'a, T>) -> Option<T>;

    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D>;
  }

  #[cfg(all(feature = "skl", not(feature = "crossbeam-skiplist-mvcc")))]
  pub trait Sealed<'a, I>:
    skl::Transfer<'a, LazyRef<'a, crate::types::RecordPointer>, To = crate::types::RecordPointer>
  {
    type Value;

    fn input(data: &Self::Data<'a, I>) -> Self::Data<'a, &'a [u8]>;

    fn from_input(input: Option<&'a [u8]>) -> Self::Data<'a, I>
    where
      Self: Sized;

    fn transfer(data: &Self::Data<'a, I>) -> Self::Data<'a, Self::Value>;

    fn leak<T>(data: Self::Data<'a, T>) -> Option<T>;

    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D>;
  }

  #[cfg(all(feature = "skl", feature = "crossbeam-skiplist-mvcc"))]
  pub trait Sealed<'a, I>:
    skl::Transfer<'a, LazyRef<'a, crate::types::RecordPointer>, To = crate::types::RecordPointer>
    + crossbeam_skiplist_mvcc::Transfer<
      'a,
      crate::types::RecordPointer,
      To = <Self as dbutils::state::State>::Data<'a, &'a crate::types::RecordPointer>,
    >
  {
    type Value;

    fn input(data: &Self::Data<'a, I>) -> Self::Data<'a, &'a [u8]>;

    fn from_input(input: Option<&'a [u8]>) -> Self::Data<'a, I>
    where
      Self: Sized;

    fn raw(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized;

    fn transfer(data: &Self::Data<'a, I>) -> Self::Data<'a, Self::Value>;

    fn leak<T>(data: Self::Data<'a, T>) -> Option<T>;

    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D>;
  }

  #[cfg(not(any(feature = "skl", feature = "crossbeam-skiplist-mvcc")))]
  pub trait Sealed<'a, I>: dbutils::state::State {
    type Output;

    fn input(data: &Self::Data<'a, I>) -> Self::Data<'a, &'a [u8]>;

    fn from_input(input: Option<&'a [u8]>) -> Self::Data<'a, I>
    where
      Self: Sized;

    fn raw(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized;

    fn transfer(data: &Self::Data<'a, I>) -> Self::Data<'a, Self::Output>;

    fn leak<T>(data: Self::Data<'a, T>) -> Option<T>;

    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D>;
  }

  impl<'a, I> Sealed<'a, LazyRef<'a, I>> for dbutils::state::Active
  where
    I: Type + ?Sized,
  {
    type Value = I::Ref<'a>;

    #[inline]
    fn input(data: &Self::Data<'a, LazyRef<'a, I>>) -> Self::Data<'a, &'a [u8]> {
      data.raw().expect("entry in Active state must have value")
    }

    #[inline]
    fn from_input(input: Option<&'a [u8]>) -> LazyRef<'a, I>
    where
      Self: Sized,
    {
      unsafe { LazyRef::from_raw(input.expect("entry in Active state must have value")) }
    }

    #[inline]
    fn raw(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized,
    {
      input.expect("entry in Active state must have value")
    }

    #[inline]
    fn transfer(data: &Self::Data<'a, LazyRef<'a, I>>) -> Self::Data<'a, I::Ref<'a>> {
      *data.get()
    }

    #[inline]
    fn leak<T>(data: Self::Data<'a, T>) -> Option<T> {
      Some(data)
    }

    #[inline]
    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D> {
      data.expect("entry in Active state must have value")
    }
  }

  impl<'a, I> Sealed<'a, LazyRef<'a, I>> for dbutils::state::MaybeTombstone
  where
    I: Type + ?Sized,
  {
    type Value = I::Ref<'a>;

    #[inline]
    fn input(data: &Self::Data<'a, LazyRef<'a, I>>) -> Option<&'a [u8]> {
      data
        .as_ref()
        .map(|v| v.raw().expect("entry in Active state must have value"))
    }

    #[inline]
    fn from_input(input: Option<&'a [u8]>) -> Option<LazyRef<'a, I>>
    where
      Self: Sized,
    {
      unsafe { input.map(|v| LazyRef::from_raw(v)) }
    }

    #[inline]
    fn raw(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized,
    {
      input
    }

    #[inline]
    fn transfer(data: &Self::Data<'a, LazyRef<'a, I>>) -> Self::Data<'a, I::Ref<'a>> {
      data.as_ref().map(|v| *v.get())
    }

    #[inline]
    fn leak<T>(data: Self::Data<'a, T>) -> Option<T> {
      data
    }

    #[inline]
    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D> {
      data.flatten()
    }
  }

  impl<'a> Sealed<'a, &'a [u8]> for dbutils::state::Active {
    type Value = &'a [u8];

    #[inline]
    fn input(data: &Self::Data<'a, &'a [u8]>) -> Self::Data<'a, &'a [u8]> {
      *data
    }

    #[inline]
    fn from_input(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized,
    {
      input.expect("entry in Active state must have value")
    }

    #[inline]
    fn raw(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized,
    {
      input.expect("entry in Active state must have value")
    }

    #[inline]
    fn transfer(data: &Self::Data<'a, &'a [u8]>) -> Self::Data<'a, Self::Value> {
      *data
    }

    #[inline]
    fn leak<T>(data: Self::Data<'a, T>) -> Option<T> {
      Some(data)
    }

    #[inline]
    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D> {
      data.expect("entry in Active state must have value")
    }
  }

  impl<'a> Sealed<'a, &'a [u8]> for dbutils::state::MaybeTombstone {
    type Value = &'a [u8];

    #[inline]
    fn input(data: &Self::Data<'a, &'a [u8]>) -> Option<&'a [u8]> {
      data.as_ref().copied()
    }

    #[inline]
    fn from_input(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized,
    {
      input
    }

    #[inline]
    fn raw(input: Option<&'a [u8]>) -> Self::Data<'a, &'a [u8]>
    where
      Self: Sized,
    {
      input
    }

    #[inline]
    fn transfer(data: &Self::Data<'a, &'a [u8]>) -> Self::Data<'a, Self::Value> {
      data.as_ref().copied()
    }

    #[inline]
    fn leak<T>(data: Self::Data<'a, T>) -> Option<T> {
      data
    }

    #[inline]
    fn into_state<D>(data: Option<Self::Data<'a, D>>) -> Self::Data<'a, D> {
      data.flatten()
    }
  }
}
