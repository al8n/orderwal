use core::slice;

use crate::{dynamic::memtable::MemtableEntry, WithoutVersion};

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: MemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for Entry<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
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
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
      ptr: self.ptr,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a> + WithoutVersion,
{
  #[inline]
  pub(crate) fn new((ptr, ent): (*const u8, E)) -> Self {
    let kp = ent.key();
    let vp = ent.value();
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        value: slice::from_raw_parts(ptr.add(vp.offset()), vp.len()),
        ent,
        ptr,
      }
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a> + WithoutVersion,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self.ent.next().map(|ent| Self::new((self.ptr, ent)))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(|ent| Self::new((self.ptr, ent)))
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
{
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }
}

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, E>
where
  E: MemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for Key<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Key").field("key", &self.key()).finish()
  }
}

impl<'a, E> Clone for Key<'a, E>
where
  E: MemtableEntry<'a> + Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      ptr: self.ptr,
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E: MemtableEntry<'a>,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self.ent.next().map(|ent| Self::new((self.ptr, ent)))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(|ent| Self::new((self.ptr, ent)))
  }
}

impl<'a, E> Key<'a, E>
where
  E: MemtableEntry<'a>,
{
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  #[inline]
  pub(crate) fn new((ptr, ent): (*const u8, E)) -> Self {
    let kp = ent.key();
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        ent,
        ptr,
      }
    }
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, E>
where
  E: MemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for Value<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
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
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      value: self.value,
      ptr: self.ptr,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: MemtableEntry<'a>,
  E::Value: crate::dynamic::types::Value<'a>,
{
  #[inline]
  pub(crate) fn new((ptr, ent): (*const u8, E)) -> Self {
    let kp = ent.key();
    let vp = ent.value();
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        value: slice::from_raw_parts(ptr.add(vp.offset()), vp.len()),
        ent,
        ptr,
      }
    }
  }

  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    self.ent.next().map(|ent| Self::new((self.ptr, ent)))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self.ent.prev().map(|ent| Self::new((self.ptr, ent)))
  }

  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }
}
