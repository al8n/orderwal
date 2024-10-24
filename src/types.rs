use dbutils::traits::{KeyRef, Type};
pub use dbutils::{
  buffer::{BufWriter, BufWriterOnce},
  traits::MaybeStructured,
};

use crate::{
  memtable::MemtableEntry,
  sealed::{Pointer, WithVersion, WithoutVersion},
  ty_ref,
};

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
        Some(ptr.version())
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
        Some(ptr.version())
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
        Some(ptr.version())
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
            F: for<'a> FnOnce(&mut dbutils::buffer::VacantBuffer<'a>) -> Result<(), E>,
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
