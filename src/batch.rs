use dbutils::buffer::VacantBuffer;

use crate::sealed::Pointer;

use super::entry::{Entry, EntryWithBuilders, EntryWithKeyBuilder, EntryWithValueBuilder};

/// A batch of keys and values that can be inserted into the [`Wal`](super::Wal).
pub trait Batch {
  /// The key type.
  type Key;

  /// The value type.
  type Value;

  /// The [`Pointer`] type.
  type Pointer;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut Entry<Self::Key, Self::Value, Self::Pointer>>
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, V, P, T> Batch for T
where
  P: Pointer,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut Entry<K, V, P>>,
{
  type Key = K;
  type Value = V;
  type Pointer = P;

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a;

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

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a;

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

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a;

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

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}
