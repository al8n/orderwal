use core::borrow::Borrow;

use dbutils::{buffer::VacantBuffer, Comparator};

use super::{entry::{
  Entry, EntryWithBuilders, EntryWithKeyBuilder, EntryWithValueBuilder, GenericEntry,
}, GenericEntryRefMut};

/// A batch of keys and values that can be inserted into the [`Wal`].
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

/// A batch of keys and values that can be inserted into the [`Wal`].
/// Comparing to [`Batch`], this trait is used to build
/// the key in place.
pub trait BatchWithKeyBuilder {
  /// The key builder type.
  type KeyBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::Error>;

  /// The error for the key builder.
  type Error;

  /// The value type.
  type Value: Borrow<[u8]>;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithKeyBuilder<Self::KeyBuilder, Self::Value, Self::Comparator>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<KB, E, V, C, T> BatchWithKeyBuilder for T
where
  KB: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
  V: Borrow<[u8]>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithKeyBuilder<KB, V, C>>,
{
  type KeyBuilder = KB;
  type Error = E;
  type Value = V;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`].
/// Comparing to [`Batch`], this trait is used to build
/// the value in place.
pub trait BatchWithValueBuilder {
  /// The value builder type.
  type ValueBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::Error>;

  /// The error for the value builder.
  type Error;

  /// The key type.
  type Key: Borrow<[u8]>;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithValueBuilder<Self::Key, Self::ValueBuilder, Self::Comparator>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, VB, E, C, T> BatchWithValueBuilder for T
where
  VB: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
  K: Borrow<[u8]>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithValueBuilder<K, VB, C>>,
{
  type Key = K;
  type Error = E;
  type ValueBuilder = VB;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`].
/// Comparing to [`Batch`], this trait is used to build
/// the key and value in place.
pub trait BatchWithBuilders {
  /// The value builder type.
  type ValueBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::ValueError>;

  /// The error for the value builder.
  type ValueError;

  /// The value builder type.
  type KeyBuilder: Fn(&mut VacantBuffer<'_>) -> Result<(), Self::KeyError>;

  /// The error for the value builder.
  type KeyError;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithBuilders<Self::KeyBuilder, Self::ValueBuilder, Self::Comparator>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<KB, KE, VB, VE, C, T> BatchWithBuilders for T
where
  VB: Fn(&mut VacantBuffer<'_>) -> Result<(), VE>,
  KB: Fn(&mut VacantBuffer<'_>) -> Result<(), KE>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithBuilders<KB, VB, C>>,
{
  type KeyBuilder = KB;
  type KeyError = KE;
  type ValueBuilder = VB;
  type ValueError = VE;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// An iterator wrapper for any `&mut T: IntoIterator<Item = &mut GenericEntry<'_, K, V>>`.
pub struct GenericBatchIterMut<'a, K, V, T> {
  iter: T,
  _m: core::marker::PhantomData<&'a (K, V)>,
}

impl<K, V, T> GenericBatchIterMut<'_, K, V, T> {
  /// Creates a new iterator wrapper.
  #[inline]
  const fn new(iter: T) -> Self {
    Self {
      iter,
      _m: core::marker::PhantomData,
    }
  }
}

impl<'a, K, V, T> Iterator for GenericBatchIterMut<'a, K, V, T>
where
  T: Iterator<Item = &'a mut GenericEntry<'a, K, V>>,
{
  type Item = GenericEntryRefMut<'a, K, V>;

  fn next(&mut self) -> Option<Self::Item> {
    self.iter.next().map(|entry| entry.as_ref_mut())
  }
}

/// The container for entries in the [`GenericBatch`].
pub trait GenericBatch {
  /// The key type.
  type Key;

  /// The value type.
  type Value;

  /// The iterator type.
  type IterMut<'e>: Iterator<Item = &'e mut GenericEntry<'e, Self::Key, Self::Value>>
  where
    Self: 'e;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, V, T> GenericBatch for T
where
  for<'a> &'a mut T: IntoIterator<Item = &'a mut GenericEntry<'a, K, V>>,
{
  type Key = K;
  type Value = V;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

// impl<'a, K, V> GenericBatch for Vec<GenericEntry<'a, K, V>>
// {
//   type Key = K;
//   type Value = V;

//   type IterMut<'b> = GenericBatchIterMut<'b, K, V, core::slice::IterMut<'a, GenericEntry<'a, K, V>>>
//   where
//     Self: 'b;

//   fn iter_mut(&mut self) -> Self::IterMut<'_> {
//     GenericBatchIterMut::new(IntoIterator::into_iter(self))
//   }
// }
