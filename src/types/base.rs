use core::cell::OnceCell;

use dbutils::traits::{KeyRef, Type};

use crate::{memtable::MemtableEntry, sealed::WithoutVersion, ty_ref};

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Key: Type,
  E::Value: Type,
{
  ent: E,
  raw_key: &'a [u8],
  raw_value: &'a [u8],
  key: OnceCell<<E::Key as Type>::Ref<'a>>,
  value: OnceCell<<E::Value as Type>::Ref<'a>>,
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
      raw_key: self.raw_key,
      raw_value: self.raw_value,
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
    Self {
      raw_key,
      raw_value,
      key: OnceCell::new(),
      value: OnceCell::new(),
      ent,
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
  E: MemtableEntry<'a>,
  E::Key: Type,
{
  ent: E,
  raw_key: &'a [u8],
  key: OnceCell<<E::Key as Type>::Ref<'a>>,
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
      raw_key: self.raw_key,
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
    self.key.get_or_init(|| ty_ref::<E::Key>(self.raw_key))
  }

  #[inline]
  pub(crate) fn new(ent: E) -> Self {
    let raw_key = ent.key().as_slice();
    Self {
      raw_key,
      key: OnceCell::new(),
      ent,
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
  raw_value: &'a [u8],
  value: OnceCell<<E::Value as Type>::Ref<'a>>,
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
      raw_value: self.raw_value,
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
    Self {
      raw_key,
      raw_value,
      value: OnceCell::new(),
      ent,
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
    self
      .value
      .get_or_init(|| ty_ref::<E::Value>(self.raw_value))
  }
}
