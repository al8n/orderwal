use dbutils::{buffer::VacantBuffer, traits::Type};

use crate::{sealed::{Pointer, WithVersion, WithoutVersion}, KeyBuilder, ValueBuilder, VERSION_SIZE};

use super::entry::*;

/// An entry in batch
pub trait BatchEntry {
  /// The [`Pointer`] type.
  type Pointer;

  type KeyError;

  type ValueError;

  /// Returns the key.
  fn key_builder(
    &self,
  ) -> KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::KeyError>>;

  fn encoded_key_len(&self) -> usize;

  fn value_len(&self) -> usize;

  /// Returns the value.
  fn value_builder(
    &self,
  ) -> ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::ValueError>>;

  /// Returns the version.
  fn version(&self) -> Option<u64>;

  fn set_encoded_meta(&mut self, meta: BatchEncodedEntryMeta);

  fn encoded_meta(&self) -> &BatchEncodedEntryMeta;

  /// Takes the pointer.
  fn take_pointer(&mut self) -> Option<Self::Pointer>;

  /// Sets the pointer.
  fn set_pointer(&mut self, pointer: Self::Pointer);
}

pub trait Batch2 {
  type Entry;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut Self::Entry>
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}


impl<E, T> Batch2 for T
where
  for<'a> &'a mut T: IntoIterator<Item = &'a mut E>,
{
  type Entry = E;

  type IterMut<'a>
    = <&'a mut T as IntoIterator>::IntoIter
  where
    Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

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
  type ValueBuilder;

  /// The error for the value builder.
  type ValueError;

  /// The value builder type.
  type KeyBuilder;

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

pub trait BufWriter {
  type Error;

  fn len(&self) -> usize;

  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error>;
}

impl<T: ?Sized + Type> BufWriter for T
where
  T: Type,
{
  type Error = T::Error;

  #[inline]
  fn len(&self) -> usize {
    Type::encoded_len(self)
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.encode_to_buffer(buf).map(|_| ())
  }
}

impl<W, E> BufWriter for ValueBuilder<W>
where
  W: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.builder()(buf)
  }
}

impl<W, E> BufWriter for KeyBuilder<W>
where
  W: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.builder()(buf)
  }
}

/// An entry.
pub struct BatchEntryRef<'a, K: ?Sized, V: ?Sized, P> {
  pub(crate) key: &'a K,
  pub(crate) value: &'a V,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
  pub(crate) version: Option<u64>,
}

impl<'a, K, V, P> BatchEntryRef<'a, K, V, P>
where
  P: WithoutVersion,
  K: ?Sized,
  V: ?Sized,
{
  /// Creates a new entry.
  #[inline]
  pub fn new(key: &'a K, value: &'a V) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: None,
    }
  }
}

impl<'a, K, V, P> BatchEntryRef<'a, K, V, P>
where
  P: WithVersion,
  K: ?Sized,
  V: ?Sized,
{
  /// Creates a new entry.
  #[inline]
  pub fn with_version(
    version: u64,
    key: &'a K,
    value: &'a V,
  ) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: Some(version),
    }
  }
}

impl<K: BufWriter, V: BufWriter, P> BatchEntry for BatchEntryRef<'_, K, V, P> {
  type Pointer = P;

  type KeyError = K::Error;

  type ValueError = V::Error;

  #[inline]
  fn key_builder(
    &self,
  ) -> KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::KeyError>> {
    KeyBuilder::once(self.encoded_key_len() as u32, |buf| {
      self.key.write(buf)
    })
  }

  #[inline]
  fn encoded_key_len(&self) -> usize {
    match self.version {
      Some(_) => self.key.len() + VERSION_SIZE,
      None => self.key.len(),
    }
  }

  #[inline]
  fn value_len(&self) -> usize {
    self.value.len()
  }

  #[inline]
  fn value_builder(
    &self,
  ) -> ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::ValueError>> {
    ValueBuilder::once(self.value_len() as u32, |buf| {
      self.value.write(buf)
    })
  }

  #[inline]
  fn version(&self) -> Option<u64> {
    self.version
  }

  #[inline]
  fn set_encoded_meta(&mut self, meta: BatchEncodedEntryMeta) {
    self.meta = meta;
  }

  #[inline]
  fn encoded_meta(&self) -> &BatchEncodedEntryMeta {
    &self.meta
  }

  #[inline]
  fn take_pointer(&mut self) -> Option<Self::Pointer> {
    self.pointer.take()
  }

  #[inline]
  fn set_pointer(&mut self, pointer: Self::Pointer) {
    self.pointer = Some(pointer);
  }
}

impl<'a, K, V, P> BatchEntryRef<'a, K, V, P> {
  /// Returns the length of the key.
  #[inline]
  pub fn key_len(&self) -> usize
  where
    K: BufWriter,
  {
    self.key.len()
  }

  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize
  where
    V: BufWriter,
  {
    self.value.len()
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    self.key
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (&'a K, &'a V) {
    (self.key, self.value)
  }
}

pub struct BatchEntry2<K, V, P> {
  pub(crate) key: K,
  pub(crate) value: V,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
  pub(crate) version: Option<u64>,
}

impl<K, V, P> BatchEntry2<K, V, P>
where
  P: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: None,
    }
  }
}

impl<K, V, P> BatchEntry2<K, V, P>
where
  P: WithVersion,
{
  /// Creates a new entry.
  #[inline]
  pub fn with_version(
    version: u64,
    key: K,
    value: V,
  ) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: Some(version),
    }
  }
}

impl<K: BufWriter, V: BufWriter, P> BatchEntry for BatchEntry2<K, V, P> {
  type Pointer = P;

  type KeyError = K::Error;

  type ValueError = V::Error;

  #[inline]
  fn key_builder(
    &self,
  ) -> KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::KeyError>> {
    KeyBuilder::once(self.encoded_key_len() as u32, |buf| {
      self.key.write(buf)
    })
  }

  #[inline]
  fn encoded_key_len(&self) -> usize {
    match self.version {
      Some(_) => self.key.len() + VERSION_SIZE,
      None => self.key.len(),
    }
  }

  #[inline]
  fn value_len(&self) -> usize {
    self.value.len()
  }

  #[inline]
  fn value_builder(
    &self,
  ) -> ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::ValueError>> {
    ValueBuilder::once(self.value_len() as u32, |buf| {
      self.value.write(buf)
    })
  }

  #[inline]
  fn version(&self) -> Option<u64> {
    self.version
  }

  #[inline]
  fn set_encoded_meta(&mut self, meta: BatchEncodedEntryMeta) {
    self.meta = meta;
  }

  #[inline]
  fn encoded_meta(&self) -> &BatchEncodedEntryMeta {
    &self.meta
  }

  #[inline]
  fn take_pointer(&mut self) -> Option<Self::Pointer> {
    self.pointer.take()
  }

  #[inline]
  fn set_pointer(&mut self, pointer: Self::Pointer) {
    self.pointer = Some(pointer);
  }
}

impl<K, V, P> BatchEntry2<K, V, P> {
  /// Returns the length of the key.
  #[inline]
  pub fn key_len(&self) -> usize
  where
    K: BufWriter,
  {
    self.key.len()
  }

  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize
  where
    V: BufWriter,
  {
    self.value.len()
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, V) {
    (self.key, self.value)
  }
}