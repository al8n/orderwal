use core::borrow::Borrow;

use among::Among;
use crossbeam_skiplist::set::Entry as SetEntry;
use rarena_allocator::either::Either;

use super::{
  pointer::{GenericPointer, Pointer},
  wal::r#type::{Type, TypeRef},
  KeyBuilder, ValueBuilder,
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
  const fn zero() -> Self {
    Self {
      klen: 0,
      vlen: 0,
      kvlen: 0,
      kvlen_size: 0,
    }
  }
}

/// An entry which can be inserted into the [`Wal`](crate::wal::Wal).
pub struct Entry<K, V, C> {
  pub(crate) key: K,
  pub(crate) value: V,
  pub(crate) pointer: Option<Pointer<C>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<K, V, C> Entry<K, V, C>
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
{
  /// Returns the length of the value.
  #[inline]
  pub fn key_len(&self) -> usize {
    self.key.borrow().len()
  }

  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize {
    self.value.borrow().len()
  }
}

impl<K, V, C> Entry<K, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
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

/// An entry builder which can build an [`Entry`] to be inserted into the [`Wal`](crate::wal::Wal).
pub struct EntryWithKeyBuilder<KB, V, C> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) value: V,
  pub(crate) pointer: Option<Pointer<C>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<KB, V, C> EntryWithKeyBuilder<KB, V, C>
where
  V: Borrow<[u8]>,
{
  /// Returns the length of the value.
  #[inline]
  pub fn value_len(&self) -> usize {
    self.value.borrow().len()
  }
}

impl<KB, V, C> EntryWithKeyBuilder<KB, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, value: V) -> Self {
    Self {
      kb,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Returns the length of the key.
  #[inline]
  pub const fn key_len(&self) -> usize {
    self.kb.size() as usize
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, V) {
    (self.kb, self.value)
  }
}

/// An entry builder which can build an [`Entry`] to be inserted into the [`Wal`](crate::wal::Wal).
pub struct EntryWithValueBuilder<K, VB, C> {
  pub(crate) key: K,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<Pointer<C>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<K, VB, C> EntryWithValueBuilder<K, VB, C>
where
  K: Borrow<[u8]>,
{
  /// Returns the length of the key.
  #[inline]
  pub fn key_len(&self) -> usize {
    self.key.borrow().len()
  }
}

impl<K, VB, C> EntryWithValueBuilder<K, VB, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, vb: ValueBuilder<VB>) -> Self {
    Self {
      key,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the value.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the length of the value.
  #[inline]
  pub const fn value_len(&self) -> usize {
    self.vb.size() as usize
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, ValueBuilder<VB>) {
    (self.key, self.vb)
  }
}

/// An entry builder which can build an [`Entry`] to be inserted into the [`Wal`](crate::wal::Wal).
pub struct EntryWithBuilders<KB, VB, C> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<Pointer<C>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<KB, VB, C> EntryWithBuilders<KB, VB, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, vb: ValueBuilder<VB>) -> Self {
    Self {
      kb,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the value builder.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the key builder.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Returns the length of the key.
  #[inline]
  pub const fn key_len(&self) -> usize {
    self.kb.size() as usize
  }

  /// Returns the length of the value.
  #[inline]
  pub const fn value_len(&self) -> usize {
    self.vb.size() as usize
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, ValueBuilder<VB>) {
    (self.kb, self.vb)
  }
}

/// A wrapper around a generic type that can be used to construct a [`GenericEntry`].
#[repr(transparent)]
pub struct Generic<'a, T> {
  data: Among<T, &'a T, &'a [u8]>,
}

impl<T: Type> Generic<'_, T> {
  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    match &self.data {
      Among::Left(val) => val.encoded_len(),
      Among::Middle(val) => val.encoded_len(),
      Among::Right(val) => val.len(),
    }
  }

  #[inline]
  pub(crate) fn encode(&self, buf: &mut [u8]) -> Result<(), T::Error> {
    match &self.data {
      Among::Left(val) => val.encode(buf),
      Among::Middle(val) => val.encode(buf),
      Among::Right(val) => {
        buf.copy_from_slice(val);
        Ok(())
      }
    }
  }
}

impl<'a, T> Generic<'a, T> {
  /// Returns the value contained in the generic.
  #[inline]
  pub const fn data(&self) -> Either<&T, &'a [u8]> {
    match &self.data {
      Among::Left(val) => Either::Left(val),
      Among::Middle(val) => Either::Left(val),
      Among::Right(val) => Either::Right(val),
    }
  }

  /// Creates a new generic from bytes for querying or inserting into the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
  ///
  /// ## Safety
  /// - the `slice` must the same as the one returned by [`T::encode`](Type::encode).
  #[inline]
  pub const unsafe fn from_slice(slice: &'a [u8]) -> Self {
    Self {
      data: Among::Right(slice),
    }
  }

  #[inline]
  pub(crate) fn into_among(self) -> Among<T, &'a T, &'a [u8]> {
    self.data
  }
}

impl<'a, T> From<&'a T> for Generic<'a, T> {
  #[inline]
  fn from(value: &'a T) -> Self {
    Self {
      data: Among::Middle(value),
    }
  }
}

impl<T> From<T> for Generic<'_, T> {
  #[inline]
  fn from(value: T) -> Self {
    Self {
      data: Among::Left(value),
    }
  }
}

/// An entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
pub struct GenericEntry<'a, K, V> {
  pub(crate) key: Generic<'a, K>,
  pub(crate) value: Generic<'a, V>,
  pub(crate) pointer: Option<GenericPointer<K, V>>,
  pub(crate) meta: BatchEncodedEntryMeta,
}

impl<'a, K, V> GenericEntry<'a, K, V> {
  /// Creates a new entry.
  #[inline]
  pub fn new(key: impl Into<Generic<'a, K>>, value: impl Into<Generic<'a, V>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> Either<&K, &[u8]> {
    self.key.data()
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> Either<&V, &[u8]> {
    self.value.data()
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (Generic<'a, K>, Generic<'a, V>) {
    (self.key, self.value)
  }
}

/// The reference to an entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
#[repr(transparent)]
pub struct GenericEntryRef<'a, K, V> {
  ent: SetEntry<'a, GenericPointer<K, V>>,
}

impl<'a, K, V> core::fmt::Debug for GenericEntryRef<'a, K, V>
where
  K: Type,
  K::Ref<'a>: core::fmt::Debug,
  V: Type,
  V::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("EntryRef")
      .field("key", &self.key())
      .field("value", &self.value())
      .finish()
  }
}

impl<K, V> Clone for GenericEntryRef<'_, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
    }
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V> {
  #[inline]
  pub(super) fn new(ent: SetEntry<'a, GenericPointer<K, V>>) -> Self {
    Self { ent }
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: Type,
  V: Type,
{
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> K::Ref<'a> {
    let p = self.ent.value();
    unsafe { TypeRef::from_slice(p.as_key_slice()) }
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> V::Ref<'a> {
    let p = self.ent.value();
    unsafe { TypeRef::from_slice(p.as_value_slice()) }
  }
}
