use core::borrow::Borrow;

use dbutils::{buffer::VacantBuffer, Comparator};

use super::entry::{
  Entry, EntryWithBuilders, EntryWithKeyBuilder, EntryWithValueBuilder, GenericEntry,
};

/// A batch of keys and values that can be inserted into the [`Wal`](super::Wal).
pub trait Batch {
  /// The key type.
  type Key: Borrow<[u8]>;

  /// The value type.
  type Value: Borrow<[u8]>;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut Entry<Self::Key, Self::Value, Self::Comparator>>
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, V, C, T> Batch for T
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut Entry<K, V, C>>,
{
  type Key = K;
  type Value = V;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`](super::Wal).
/// Comparing to [`Batch`], this trait is used to build
/// the key in place.
pub trait BatchWithKeyBuilder<P: 'static> {
  /// The key builder type.
  type KeyBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::Error>;

  /// The error for the key builder.
  type Error;

  /// The value type.
  type Value;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut EntryWithKeyBuilder<Self::KeyBuilder, Self::Value, P>>
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<KB, E, V, P, T> BatchWithKeyBuilder<P> for T
where
  KB: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithKeyBuilder<KB, V, P>>,
  P: 'static,
{
  type KeyBuilder = KB;
  type Error = E;
  type Value = V;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`](super::Wal).
/// Comparing to [`Batch`], this trait is used to build
/// the value in place.
pub trait BatchWithValueBuilder<P: 'static> {
  /// The value builder type.
  type ValueBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::Error>;

  /// The error for the value builder.
  type Error;

  /// The key type.
  type Key;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut EntryWithValueBuilder<Self::Key, Self::ValueBuilder, P>>
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, VB, E, P, T> BatchWithValueBuilder<P> for T
where
  VB: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithValueBuilder<K, VB, P>>,
  P: 'static,
{
  type Key = K;
  type Error = E;
  type ValueBuilder = VB;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`](super::Wal).
/// Comparing to [`Batch`], this trait is used to build
/// the key and value in place.
pub trait BatchWithBuilders<P: 'static> {
  /// The value builder type.
  type ValueBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::ValueError>;

  /// The error for the value builder.
  type ValueError;

  /// The value builder type.
  type KeyBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::KeyError>;

  /// The error for the value builder.
  type KeyError;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithBuilders<Self::KeyBuilder, Self::ValueBuilder, P>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<KB, KE, VB, VE, P, T> BatchWithBuilders<P> for T
where
  VB: Fn(&mut VacantBuffer<'_>) -> Result<(), VE>,
  KB: Fn(&mut VacantBuffer<'_>) -> Result<(), KE>,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithBuilders<KB, VB, P>>,
  P: 'static,
{
  type KeyBuilder = KB;
  type KeyError = KE;
  type ValueBuilder = VB;
  type ValueError = VE;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// The container for entries in the [`GenericBatch`].
pub trait GenericBatch<'e> {
  /// The key type.
  type Key: 'e;

  /// The value type.
  type Value: 'e;

  /// The mutable iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut GenericEntry<'e, Self::Key, Self::Value>>
  where
    Self: 'e,
    'e: 'a;

  /// Returns an mutable iterator over the keys and values.
  fn iter_mut(&'e mut self) -> Self::IterMut<'e>;
}

impl<'e, K, V, T> GenericBatch<'e> for T
where
  K: 'e,
  V: 'e,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut GenericEntry<'e, K, V>>,
{
  type Key = K;
  type Value = V;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'e,
    'e: 'a;

  fn iter_mut(&'e mut self) -> Self::IterMut<'e> {
    IntoIterator::into_iter(self)
  }
}
