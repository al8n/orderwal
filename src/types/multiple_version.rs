use dbutils::types::{KeyRef, Type};
use skl::LazyRef;

use crate::memtable::VersionedMemtableEntry;

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  ent: E,
  key: LazyRef<'a, E::Key>,
  value: LazyRef<'a, E::Value>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for Entry<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
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
  E: VersionedMemtableEntry<'a> + Clone,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
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
    unsafe {
      Self {
        key: LazyRef::from_raw(raw_key),
        value: LazyRef::from_raw(raw_value),
        version,
        query_version,
        ent,
      }
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
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
  E: VersionedMemtableEntry<'a>,
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
    self.key.get()
  }

  /// Returns the raw key of the entry.
  #[inline]
  pub fn raw_key(&self) -> &'a [u8] {
    self.key.raw().expect("Entry's raw key cannot be None")
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &<E::Value as Type>::Ref<'a> {
    self.value.get()
  }

  /// Returns the raw value of the entry.
  #[inline]
  pub fn raw_value(&self) -> &'a [u8] {
    self.value.raw().expect("Entry's raw value cannot be None")
  }
}

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, E>
where
  E: VersionedMemtableEntry<'a>,
  E::Key: Type,
{
  ent: E,
  key: LazyRef<'a, E::Key>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for Key<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
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
  E: VersionedMemtableEntry<'a> + Clone,
  E::Key: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E: VersionedMemtableEntry<'a>,
  E::Key: Type,
{
  #[inline]
  pub(crate) fn with_version(ent: E, query_version: u64) -> Self {
    let raw_key = ent.key().as_slice();
    let version = ent.version();
    Self {
      key: unsafe { LazyRef::from_raw(raw_key) },
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
  E: VersionedMemtableEntry<'a>,
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
  E: VersionedMemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &<E::Key as Type>::Ref<'a> {
    self.key.get()
  }

  /// Returns the raw key of the entry.
  #[inline]
  pub fn raw_key(&self) -> &'a [u8] {
    self.key.raw().expect("Key's raw key cannot be None")
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, E>
where
  E::Value: Type,
  E: VersionedMemtableEntry<'a>,
{
  ent: E,
  raw_key: &'a [u8],
  value: LazyRef<'a, E::Value>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for Value<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
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
  E: VersionedMemtableEntry<'a> + Clone,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: VersionedMemtableEntry<'a>,
  E::Value: Type,
{
  #[inline]
  pub(crate) fn with_version(ent: E, query_version: u64) -> Self {
    let raw_key = ent.key().as_slice();
    let raw_value = ent
      .value()
      .expect("value must be present on Value")
      .as_slice();
    let version = ent.version();
    Self {
      raw_key,
      value: unsafe { LazyRef::from_raw(raw_value) },
      version,
      query_version,
      ent,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: VersionedMemtableEntry<'a>,
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
  E: VersionedMemtableEntry<'a>,
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
    self.value.get()
  }

  /// Returns the raw value of the entry.
  #[inline]
  pub fn raw_value(&self) -> &'a [u8] {
    self.value.raw().expect("Value's raw value cannot be None")
  }
}

/// The reference to an entry in the generic WALs.
pub struct MultipleVersionEntry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  ent: E,
  key: LazyRef<'a, E::Key>,
  value: Option<LazyRef<'a, E::Value>>,
  version: u64,
  query_version: u64,
}

impl<'a, E> core::fmt::Debug for MultipleVersionEntry<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
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
  E: VersionedMemtableEntry<'a> + Clone,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> MultipleVersionEntry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  pub(crate) fn with_version(ent: E, query_version: u64) -> Self {
    let raw_key = ent.key().as_slice();
    let raw_value = ent.value().map(|v| v.as_slice());
    let version = ent.version();
    unsafe {
      Self {
        key: LazyRef::from_raw(raw_key),
        value: raw_value.map(|v| LazyRef::from_raw(v)),
        version,
        query_version,
        ent,
      }
    }
  }
}

impl<'a, E> MultipleVersionEntry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
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
  E: VersionedMemtableEntry<'a>,
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
    self.key.get()
  }

  /// Returns the raw key of the entry.
  #[inline]
  pub fn raw_key(&self) -> &'a [u8] {
    self
      .key
      .raw()
      .expect("MultipleVersionEntry's raw key cannot be None")
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> Option<&<E::Value as Type>::Ref<'a>> {
    self.value.as_deref()
  }

  /// Returns the raw value of the entry.
  #[inline]
  pub fn raw_value(&self) -> Option<&'a [u8]> {
    match self.value.as_ref() {
      None => None,
      Some(v) => Some(
        v.raw()
          .expect("MultipleVersionEntry's raw value cannot be None if value exists"),
      ),
    }
  }
}
