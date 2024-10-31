use dbutils::types::{KeyRef, Type};
use skl::LazyRef;

use crate::{memtable::MemtableEntry, sealed::WithoutVersion};

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  ent: E,
  key: LazyRef<'a, E::Key>,
  value: LazyRef<'a, E::Value>,
}

impl<'a, E> core::fmt::Debug for Entry<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
  E::Key: Type,
  E::Value: Type,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key())
      .field("value", &self.value())
      .finish()
  }
}

impl<'a, E> Clone for Entry<'a, E>
where
  E: MemtableEntry<'a> + Clone,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a> + WithoutVersion,
  E::Key: Type,
  E::Value: Type,
{
  #[inline]
  pub(crate) fn new(ent: E) -> Self {
    let raw_key = ent.key().as_slice();
    let raw_value = ent.value().as_slice();
    unsafe {
      Self {
        key: LazyRef::from_raw(raw_key),
        value: LazyRef::from_raw(raw_value),
        ent,
      }
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a> + WithoutVersion,
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
    self.ent.next().map(Self::new)
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &<E::Key as Type>::Ref<'a> {
    self.key.get()
  }

  /// Returns the raw key of the entry.
  #[inline]
  pub fn raw_key(&self) -> &[u8] {
    self.key.raw().expect("Entry's raw key cannot be None")
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &<E::Value as Type>::Ref<'a> {
    self.value.get()
  }

  /// Returns the raw value of the entry.
  #[inline]
  pub fn raw_value(&self) -> &[u8] {
    self.value.raw().expect("Entry's raw value cannot be None")
  }
}

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, E>
where
  E: MemtableEntry<'a>,
  E::Key: Type,
{
  ent: E,
  key: LazyRef<'a, E::Key>,
}

impl<'a, E> core::fmt::Debug for Key<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
  E::Key: Type,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Key").field("key", &self.key()).finish()
  }
}

impl<'a, E> Clone for Key<'a, E>
where
  E: MemtableEntry<'a> + Clone,
  E::Key: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E::Key: Type + Ord,
  <E::Key as Type>::Ref<'a>: KeyRef<'a, E::Key>,
  E: MemtableEntry<'a>,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }
}

impl<'a, E> Key<'a, E>
where
  E::Key: Type,
  E: MemtableEntry<'a>,
{
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &<E::Key as Type>::Ref<'a> {
    self.key.get()
  }

  /// Returns the raw key of the entry.
  #[inline]
  pub fn raw_key(&self) -> &[u8] {
    self.key.raw().expect("Key's raw key cannot be None")
  }

  #[inline]
  pub(crate) fn new(ent: E) -> Self {
    let raw_key = ent.key().as_slice();
    unsafe {
      Self {
        key: LazyRef::from_raw(raw_key),
        ent,
      }
    }
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, E>
where
  E::Value: Type,
  E: MemtableEntry<'a>,
{
  ent: E,
  raw_key: &'a [u8],
  value: LazyRef<'a, E::Value>,
}

impl<'a, E> core::fmt::Debug for Value<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
  E::Value: Type,
  <E::Value as Type>::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Value")
      .field("value", &self.value())
      .finish()
  }
}

impl<'a, E> Clone for Value<'a, E>
where
  E: MemtableEntry<'a> + Clone,
  E::Value: Type,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      raw_key: self.raw_key,
      value: self.value.clone(),
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: MemtableEntry<'a>,
  E::Value: Type,
{
  #[inline]
  pub(crate) fn new(ent: E) -> Self {
    let raw_key = ent.key().as_slice();
    let raw_value = ent.value().as_slice();
    unsafe {
      Self {
        raw_key,
        value: LazyRef::from_raw(raw_value),
        ent,
      }
    }
  }

  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self.ent.next().map(Self::new)
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(Self::new)
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &<E::Value as Type>::Ref<'a> {
    self.value.get()
  }

  /// Returns the raw value of the entry.
  #[inline]
  pub fn raw_value(&self) -> &[u8] {
    self.value.raw().expect("Value's raw value cannot be None")
  }
}
