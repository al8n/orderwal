use core::borrow::Borrow;

use dbutils::{
  buffer::VacantBuffer,
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
};
use rarena_allocator::either::Either;

use crate::{
  sealed::{Pointer as _, WithVersion, WithoutVersion},
  VERSION_SIZE,
};

use super::{KeyBuilder, ValueBuilder};

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

/// An entry which can be inserted into the [`Wal`](crate::wal::Wal).
pub struct Entry<K, V, P> {
  pub(crate) key: K,
  pub(crate) value: V,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
  pub(crate) version: Option<u64>,
}

impl<K, V, P> Entry<K, V, P>
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

  #[inline]
  pub(crate) fn internal_key_len(&self) -> usize {
    match self.version {
      Some(_) => self.key.borrow().len() + VERSION_SIZE,
      None => self.key.borrow().len(),
    }
  }
}

impl<K, V, P> Entry<K, V, P>
where
  P: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: None,
    }
  }
}

impl<K, V, P> Entry<K, V, P>
where
  P: WithVersion,
{
  /// Creates a new versioned entry.
  #[inline]
  pub const fn with_version(version: u64, key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: Some(version),
    }
  }
}

impl<K, V, P> Entry<K, V, P> {
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
pub struct EntryWithKeyBuilder<KB, V, P> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) value: V,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
  pub(crate) version: Option<u64>,
}

impl<KB, V, P> EntryWithKeyBuilder<KB, V, P>
where
  V: Borrow<[u8]>,
{
  /// Returns the length of the value.
  #[inline]
  pub(crate) fn value_len(&self) -> usize {
    self.value.borrow().len()
  }
}

impl<KB, V, P> EntryWithKeyBuilder<KB, V, P>
where
  P: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, value: V) -> Self {
    Self {
      kb,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: None,
    }
  }
}

impl<KB, V, P> EntryWithKeyBuilder<KB, V, P>
where
  P: WithVersion,
{
  /// Creates a new versioned entry.
  #[inline]
  pub const fn with_version(version: u64, kb: KeyBuilder<KB>, value: V) -> Self {
    Self {
      kb,
      value,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: Some(version),
    }
  }
}

impl<KB, V, P> EntryWithKeyBuilder<KB, V, P> {
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

  #[inline]
  pub(crate) const fn internal_key_len(&self) -> usize {
    match self.version {
      Some(_) => self.kb.size() as usize + VERSION_SIZE,
      None => self.kb.size() as usize,
    }
  }
}

/// An entry builder which can build an [`Entry`] to be inserted into the [`Wal`](crate::wal::Wal).
pub struct EntryWithValueBuilder<K, VB, P> {
  pub(crate) key: K,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
  pub(crate) version: Option<u64>,
}

impl<K, VB, C> EntryWithValueBuilder<K, VB, C>
where
  K: Borrow<[u8]>,
{
  /// Returns the length of the key.
  #[inline]
  pub(crate) fn internal_key_len(&self) -> usize {
    match self.version {
      Some(_) => self.key.borrow().len() + VERSION_SIZE,
      None => self.key.borrow().len(),
    }
  }
}

impl<K, VB, P> EntryWithValueBuilder<K, VB, P>
where
  P: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, vb: ValueBuilder<VB>) -> Self {
    Self {
      key,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: None,
    }
  }
}

impl<K, VB, P> EntryWithValueBuilder<K, VB, P>
where
  P: WithVersion,
{
  /// Creates a new versioned entry.
  #[inline]
  pub const fn with_version(version: u64, key: K, vb: ValueBuilder<VB>) -> Self {
    Self {
      key,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: Some(version),
    }
  }
}

impl<K, VB, P> EntryWithValueBuilder<K, VB, P> {
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

/// An entry builder which can build an entry to be inserted into the WALs.
pub struct EntryWithBuilders<KB, VB, P> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<P>,
  pub(crate) meta: BatchEncodedEntryMeta,
  pub(crate) version: Option<u64>,
}

impl<KB, VB, P> EntryWithBuilders<KB, VB, P>
where
  P: WithoutVersion,
{
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, vb: ValueBuilder<VB>) -> Self {
    Self {
      kb,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: None,
    }
  }
}

impl<KB, VB, P> EntryWithBuilders<KB, VB, P>
where
  P: WithVersion,
{
  /// Creates a new entry.
  #[inline]
  pub const fn with_version(version: u64, kb: KeyBuilder<KB>, vb: ValueBuilder<VB>) -> Self {
    Self {
      kb,
      vb,
      pointer: None,
      meta: BatchEncodedEntryMeta::zero(),
      version: Some(version),
    }
  }
}

impl<KB, VB, P> EntryWithBuilders<KB, VB, P> {
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

  #[inline]
  pub(crate) const fn internal_key_len(&self) -> usize {
    match self.version {
      Some(_) => self.kb.size() as usize + VERSION_SIZE,
      None => self.kb.size() as usize,
    }
  }
}
