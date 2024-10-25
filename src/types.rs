pub use dbutils::{
  buffer::{BufWriter, BufWriterOnce},
  traits::MaybeStructured,
};
use dbutils::{
  leb128::encoded_u64_varint_len,
  traits::{KeyRef, Type},
};

use crate::{
  memtable::MemtableEntry,
  merge_lengths,
  sealed::{Pointer, WithVersion, WithoutVersion},
  ty_ref, CHECKSUM_SIZE, RECORD_FLAG_SIZE, VERSION_SIZE,
};

const ENTRY_FLAGS_SIZE: usize = core::mem::size_of::<EntryFlags>();

bitflags::bitflags! {
  /// The flags for each entry.
  #[derive(Debug, Copy, Clone)]
  pub struct EntryFlags: u8 {
    /// First bit: 1 indicates removed
    const REMOVED = 0b00000001;
    /// Second bit: 1 indicates the key is pointer
    const POINTER = 0b00000010;
    /// Third bit: 1 indicates the entry contains a version
    const VERSIONED = 0b00000100;
  }
}

impl EntryFlags {
  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();
}

pub(crate) struct EncodedEntryMeta {
  pub(crate) packed_kvlen_size: usize,
  pub(crate) packed_kvlen: u64,
  pub(crate) entry_size: u32,
  pub(crate) klen: usize,
  pub(crate) vlen: usize,
  pub(crate) versioned: bool,
  batch: bool,
}

impl EncodedEntryMeta {
  #[inline]
  pub(crate) const fn new(key_len: usize, value_len: usize, versioned: bool) -> Self {
    // Cast to u32 is safe, because we already checked those values before calling this function.

    let len = merge_lengths(key_len as u32, value_len as u32);
    let len_size = encoded_u64_varint_len(len);
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    let elen = RECORD_FLAG_SIZE as u32
      + len_size as u32
      + ENTRY_FLAGS_SIZE as u32
      + version_size as u32
      + key_len as u32
      + value_len as u32
      + CHECKSUM_SIZE as u32;

    Self {
      packed_kvlen_size: len_size,
      batch: false,
      packed_kvlen: len,
      entry_size: elen,
      klen: key_len,
      vlen: value_len,
      versioned,
    }
  }

  #[inline]
  pub(crate) const fn batch(key_len: usize, value_len: usize, versioned: bool) -> Self {
    // Cast to u32 is safe, because we already checked those values before calling this function.

    let len = merge_lengths(key_len as u32, value_len as u32);
    let len_size = encoded_u64_varint_len(len);
    let version_size = if versioned { VERSION_SIZE } else { 0 };
    let elen = len_size as u32
      + EntryFlags::SIZE as u32
      + version_size as u32
      + key_len as u32
      + value_len as u32;

    Self {
      packed_kvlen_size: len_size,
      packed_kvlen: len,
      entry_size: elen,
      klen: key_len,
      vlen: value_len,
      versioned,
      batch: true,
    }
  }

  #[inline]
  pub(crate) const fn batch_zero(versioned: bool) -> Self {
    Self {
      packed_kvlen_size: 0,
      packed_kvlen: 0,
      entry_size: 0,
      klen: 0,
      vlen: 0,
      versioned,
      batch: true,
    }
  }

  #[inline]
  pub(crate) const fn entry_flag_offset(&self) -> usize {
    if self.batch {
      return self.packed_kvlen_size;
    }

    RECORD_FLAG_SIZE + self.packed_kvlen_size
  }

  #[inline]
  pub(crate) const fn version_offset(&self) -> usize {
    self.entry_flag_offset() + ENTRY_FLAGS_SIZE
  }

  #[inline]
  pub(crate) const fn key_offset(&self) -> usize {
    if self.versioned {
      self.version_offset() + VERSION_SIZE
    } else {
      self.version_offset()
    }
  }

  #[inline]
  pub(crate) const fn value_offset(&self) -> usize {
    self.key_offset() + self.klen
  }

  #[inline]
  pub(crate) const fn checksum_offset(&self) -> usize {
    if self.batch {
      self.value_offset() + self.vlen
    } else {
      self.entry_size as usize - CHECKSUM_SIZE
    }
  }
}

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
{
  ent: E,
  pub(crate) raw_key: &'a [u8],
  key: K::Ref<'a>,
  value: V::Ref<'a>,
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<'a, K, V, E> core::fmt::Debug for Entry<'a, K, V, E>
where
  K: Type + ?Sized,
  K::Ref<'a>: core::fmt::Debug,
  V: Type + ?Sized,
  V::Ref<'a>: core::fmt::Debug,
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if let Some(version) = self.version {
      f.debug_struct("Entry")
        .field("key", &self.key())
        .field("value", &self.value())
        .field("version", &version)
        .finish()
    } else {
      f.debug_struct("Entry")
        .field("key", &self.key())
        .field("value", &self.value())
        .finish()
    }
  }
}

impl<'a, K, V, E> Clone for Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  K::Ref<'a>: Clone,
  V: ?Sized + Type,
  V::Ref<'a>: Clone,
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      key: self.key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithoutVersion,
{
  #[inline]
  pub(super) fn new(ent: E) -> Self {
    Self::with_version_in(ent, None)
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithVersion,
{
  #[inline]
  pub(super) fn with_version(ent: E, query_version: u64) -> Self {
    Self::with_version_in(ent, Some(query_version))
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let raw_key = ptr.as_key_slice();
    Self {
      raw_key,
      key: ty_ref::<K>(raw_key),
      value: ty_ref::<V>(ptr.as_value_slice().unwrap()),
      version: if query_version.is_some() {
        Some(ptr.version().unwrap_or(0))
      } else {
        None
      },
      query_version,
      ent,
    }
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: Type + ?Sized,
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: ?Sized + Type,
  V: Type + ?Sized,
{
  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &V::Ref<'a> {
    &self.value
  }
}

impl<'a, K, V, E> Entry<'a, K, V, E>
where
  K: Type + ?Sized,
  V: ?Sized + Type,
{
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    &self.key
  }
}

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, K, E>
where
  K: ?Sized + Type,
{
  ent: E,
  raw_key: &'a [u8],
  key: K::Ref<'a>,
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<'a, K, E> core::fmt::Debug for Key<'a, K, E>
where
  K: Type + ?Sized,
  K::Ref<'a>: core::fmt::Debug,
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if let Some(version) = self.version {
      f.debug_struct("Key")
        .field("key", &self.key())
        .field("version", &version)
        .finish()
    } else {
      f.debug_struct("Key").field("key", &self.key()).finish()
    }
  }
}

impl<'a, K, E> Clone for Key<'a, K, E>
where
  K: ?Sized + Type,
  K::Ref<'a>: Clone,
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      key: self.key,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let raw_key = ptr.as_key_slice();
    Self {
      raw_key,
      key: ty_ref::<K>(raw_key),
      version: if query_version.is_some() {
        Some(ptr.version().unwrap_or(0))
      } else {
        None
      },
      query_version,
      ent,
    }
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: Type + Ord + ?Sized,
  for<'b> K::Ref<'b>: KeyRef<'b, K>,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: Type + ?Sized,
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, K, E> Key<'a, K, E>
where
  K: Type + ?Sized,
{
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &K::Ref<'a> {
    &self.key
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, V, E>
where
  V: ?Sized + Type,
{
  ent: E,
  raw_key: &'a [u8],
  value: V::Ref<'a>,
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<'a, V, E> core::fmt::Debug for Value<'a, V, E>
where
  V: Type + ?Sized,
  V::Ref<'a>: core::fmt::Debug,
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if let Some(version) = self.version {
      f.debug_struct("Value")
        .field("value", &self.value())
        .field("version", &version)
        .finish()
    } else {
      f.debug_struct("Value")
        .field("value", &self.value())
        .finish()
    }
  }
}

impl<'a, V, E> Clone for Value<'a, V, E>
where
  V: ?Sized + Type,
  V::Ref<'a>: Clone,
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: ?Sized + Type,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let raw_key = ptr.as_key_slice();
    Self {
      raw_key,
      value: ty_ref::<V>(ptr.as_value_slice().unwrap()),
      version: if query_version.is_some() {
        Some(ptr.version().unwrap_or(0))
      } else {
        None
      },
      query_version,
      ent,
    }
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: Type + ?Sized,
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.query_version))
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: Type + ?Sized,
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, V, E> Value<'a, V, E>
where
  V: Type + ?Sized,
{
  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &V::Ref<'a> {
    &self.value
  }
}

macro_rules! builder_ext {
  ($($name:ident),+ $(,)?) => {
    $(
      paste::paste! {
        impl<F> $name<F> {
          #[doc = "Creates a new `" $name "` with the given size and builder closure which requires `FnOnce`."]
          #[inline]
          pub const fn once<E>(size: usize, f: F) -> Self
          where
            F: for<'a> FnOnce(&mut dbutils::buffer::VacantBuffer<'a>) -> Result<usize, E>,
          {
            Self { size, f }
          }
        }
      }
    )*
  };
}

dbutils::builder!(
  /// A value builder for the wal, which requires the value size for accurate allocation and a closure to build the value.
  pub ValueBuilder;
  /// A key builder for the wal, which requires the key size for accurate allocation and a closure to build the key.
  pub KeyBuilder;
);

builder_ext!(ValueBuilder, KeyBuilder,);
