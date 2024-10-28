use core::cell::OnceCell;

use dbutils::traits::{KeyRef, Type};

use crate::{memtable::MultipleVersionMemtableEntry, ty_ref};

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  ent: E,
  raw_key: &'a [u8],
  raw_value: &'a [u8],
  key: OnceCell<<E::Key as Type>::Ref<'a>>,
  value: OnceCell<<E::Value as Type>::Ref<'a>>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for Entry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + core::fmt::Debug,
  E::Key: Type,
  E::Value: Type,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key())
      .field("value", &self.value())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, E> Clone for Entry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + Clone,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      raw_value: self.raw_value,
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  pub(crate) fn with_version(ent: E, query_version: u64) -> Self {
    let version = ent.version();
    let raw_key = ent.key().as_slice();
    let raw_value = ent
      .value()
      .expect("value must be present on Entry")
      .as_slice();
    Self {
      raw_key,
      raw_value,
      key: OnceCell::new(),
      value: OnceCell::new(),
      version,
      query_version,
      ent,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type + Ord,
  <E::Key as Type>::Ref<'a>: KeyRef<'a, E::Key>,
  E::Value: Type,
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
      .map(|ent| Self::with_version(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(ent, self.query_version))
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &<E::Key as Type>::Ref<'a> {
    self.key.get_or_init(|| ty_ref::<E::Key>(self.raw_key))
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &<E::Value as Type>::Ref<'a> {
    self
      .value
      .get_or_init(|| ty_ref::<E::Value>(self.raw_value))
  }
}

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
{
  ent: E,
  raw_key: &'a [u8],
  key: OnceCell<<E::Key as Type>::Ref<'a>>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for Key<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + core::fmt::Debug,
  E::Key: Type,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Key")
      .field("key", &self.key())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, E> Clone for Key<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + Clone,
  E::Key: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      key: self.key.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
{
  #[inline]
  pub(super) fn with_version(ent: E, query_version: u64) -> Self {
    let raw_key = ent.key().as_slice();
    let version = ent.version();
    Self {
      raw_key,
      key: OnceCell::new(),
      version,
      query_version,
      ent,
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E::Key: Type + Ord,
  <E::Key as Type>::Ref<'a>: KeyRef<'a, E::Key>,
  E: MultipleVersionMemtableEntry<'a>,
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
      .map(|ent| Self::with_version(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(ent, self.query_version))
  }
}

impl<'a, E> Key<'a, E>
where
  E::Key: Type,
  E: MultipleVersionMemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &<E::Key as Type>::Ref<'a> {
    self.key.get_or_init(|| ty_ref::<E::Key>(self.raw_key))
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, E>
where
  E::Value: Type,
  E: MultipleVersionMemtableEntry<'a>,
{
  ent: E,
  raw_key: &'a [u8],
  raw_value: &'a [u8],
  value: OnceCell<<E::Value as Type>::Ref<'a>>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for Value<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + core::fmt::Debug,
  E::Value: Type,
  <E::Value as Type>::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Value")
      .field("value", &self.value())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, E> Clone for Value<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + Clone,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      raw_value: self.raw_value,
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Value: Type,
{
  #[inline]
  pub(super) fn with_version(ent: E, query_version: u64) -> Self {
    let raw_key = ent.key().as_slice();
    let raw_value = ent
      .value()
      .expect("value must be present on Value")
      .as_slice();
    let version = ent.version();
    Self {
      raw_key,
      raw_value,
      value: OnceCell::new(),
      version,
      query_version,
      ent,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Value: Type,
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
      .map(|ent| Self::with_version(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(ent, self.query_version))
  }
}

impl<'a, E> Value<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Value: Type,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &<E::Value as Type>::Ref<'a> {
    self
      .value
      .get_or_init(|| ty_ref::<E::Value>(self.raw_value))
  }
}

/// The reference to an entry in the generic WALs.
pub struct MultipleVersionEntry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  ent: E,
  raw_key: &'a [u8],
  raw_value: Option<&'a [u8]>,
  key: OnceCell<<E::Key as Type>::Ref<'a>>,
  value: OnceCell<<E::Value as Type>::Ref<'a>>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for MultipleVersionEntry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + core::fmt::Debug,
  E::Key: Type,
  E::Value: Type,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MultipleVersionEntry")
      .field("key", &self.key())
      .field("value", &self.value())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, E> Clone for MultipleVersionEntry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a> + Clone,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      raw_value: self.raw_value,
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> MultipleVersionEntry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  pub(crate) fn with_version(ent: E, query_version: u64) -> Self {
    let raw_key = ent.key().as_slice();
    let raw_value = ent.value().map(|v| v.as_slice());
    let version = ent.version();
    Self {
      raw_key,
      raw_value,
      key: OnceCell::new(),
      value: OnceCell::new(),
      version,
      query_version,
      ent,
    }
  }
}

impl<'a, E> MultipleVersionEntry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Ord + Type,
  for<'b> <E::Key as Type>::Ref<'b>: KeyRef<'b, E::Key>,
  E::Value: Type,
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
      .map(|ent| Self::with_version(ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(ent, self.query_version))
  }
}

impl<'a, E> MultipleVersionEntry<'a, E>
where
  E: MultipleVersionMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &<E::Key as Type>::Ref<'a> {
    self.key.get_or_init(|| ty_ref::<E::Key>(self.raw_key))
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> Option<&<E::Value as Type>::Ref<'a>> {
    self
      .raw_value
      .map(|v| self.value.get_or_init(|| ty_ref::<E::Value>(v)))
  }
}
