use core::borrow::Borrow;

use dbutils::{buffer::VacantBuffer, error::InsufficientBuffer, traits::Type};

use super::{
  generic::Generic,
  sealed::{Constructable, WithVersion, WithoutVersion},
  KeyBuilder, ValueBuilder, VERSION_SIZE,
};

pub(crate) struct BatchEncodedEntryMeta {
  /// The output of `merge_lengths(klen, vlen)`
  pub(crate) kvlen: u64,
  /// the length of `encoded_u64_varint(merge_lengths(klen, vlen))`
  pub(crate) kvlen_size: usize,
  pub(crate) klen: usize,
  pub(crate) vlen: usize,
}

impl BatchEncodedEntryMeta {
  #[inline]
  pub(crate) const fn new(klen: usize, vlen: usize, kvlen: u64, kvlen_size: usize) -> Self {
    Self {
      klen,
      vlen,
      kvlen,
      kvlen_size,
    }
  }

  #[inline]
  pub(crate) const fn zero() -> Self {
    Self {
      klen: 0,
      vlen: 0,
      kvlen: 0,
      kvlen_size: 0,
    }
  }
}

pub trait BufWriter {
  type Error;

  fn len(&self) -> usize;

  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error>;
}

impl<A: Borrow<[u8]>> BufWriter for A {
  type Error = InsufficientBuffer;

  #[inline]
  fn len(&self) -> usize {
    self.borrow().len()
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    buf.put_slice(self.borrow())
  }
}

impl<T: ?Sized + Type> BufWriter for Generic<'_, T>
where
  T: Type,
{
  type Error = T::Error;

  #[inline]
  fn len(&self) -> usize {
    Generic::encoded_len(self)
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    Generic::encode_to_buffer(self, buf).map(|_| ())
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

pub trait BufWriterOnce {
  type Error;

  fn len(&self) -> usize;

  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error>;
}

impl<A: Borrow<[u8]>> BufWriterOnce for A {
  type Error = InsufficientBuffer;

  #[inline]
  fn len(&self) -> usize {
    self.borrow().len()
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    buf.put_slice(self.borrow())
  }
}

impl<T: ?Sized + Type> BufWriterOnce for Generic<'_, T>
where
  T: Type,
{
  type Error = T::Error;

  #[inline]
  fn len(&self) -> usize {
    Generic::encoded_len(self)
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    Generic::encode_to_buffer(&self, buf).map(|_| ())
  }
}

impl<W, E> BufWriterOnce for ValueBuilder<W>
where
  W: FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.into_components().1(buf)
  }
}

impl<W, E> BufWriterOnce for KeyBuilder<W>
where
  W: FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.into_components().1(buf)
  }
}

/// An entry can be inserted into the WALs through [`Batch`](super::batch::Batch).
pub struct Entry<K, V, C: Constructable> {
  pub(crate) key: K,
  pub(crate) value: V,
  pub(crate) meta: BatchEncodedEntryMeta,
  pointer: Option<C::Pointer>,
  version: Option<u64>,
}

impl<K, V, C> Entry<K, V, C>
where
  C: Constructable,
  C::Pointer: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      meta: BatchEncodedEntryMeta::zero(),
      pointer: None,
      version: None,
    }
  }
}

impl<K, V, C> Entry<K, V, C>
where
  C: Constructable,
  C::Pointer: WithVersion,
{
  /// Creates a new entry.
  #[inline]
  pub fn with_version(version: u64, key: K, value: V) -> Self {
    Self {
      key,
      value,
      meta: BatchEncodedEntryMeta::zero(),
      pointer: None,
      version: Some(version),
    }
  }

  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    match self.version {
      Some(version) => version,
      None => unreachable!(),
    }
  }

  /// Set the version of the entry.
  #[inline]
  pub fn set_version(&mut self, version: u64) {
    self.version = Some(version);
  }
}

impl<K, V, C: Constructable> Entry<K, V, C> {
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

  #[inline]
  pub(crate) fn encoded_key_len(&self) -> usize
  where
    K: BufWriter,
    V: BufWriter,
  {
    match self.version {
      Some(_) => self.key.len() + VERSION_SIZE,
      None => self.key.len(),
    }
  }

  #[inline]
  pub(crate) const fn internal_version(&self) -> Option<u64> {
    self.version
  }

  #[inline]
  pub(crate) fn take_pointer(&mut self) -> Option<C::Pointer> {
    self.pointer.take()
  }

  #[inline]
  pub(crate) fn set_pointer(&mut self, pointer: C::Pointer) {
    self.pointer = Some(pointer);
  }

  #[inline]
  pub(crate) fn set_encoded_meta(&mut self, meta: BatchEncodedEntryMeta) {
    self.meta = meta;
  }

  #[inline]
  pub(crate) fn encoded_meta(&self) -> &BatchEncodedEntryMeta {
    &self.meta
  }
}
