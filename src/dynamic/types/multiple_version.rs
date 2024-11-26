use core::slice;

use crate::dynamic::memtable::VersionedMemtableEntry;

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  version: u64,
  query_version: u64,
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for Entry<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
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
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
      ptr: self.ptr,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  #[inline]
  pub(crate) fn with_version(ptr: *const u8, ent: E, query_version: u64) -> Self {
    let version = ent.version();
    let kp = ent.key();
    let vp = ent
      .value()
      .expect("value must be present on Entry");
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        value: slice::from_raw_parts(ptr.add(vp.offset()), vp.len()),
        version,
        query_version,
        ent,
        ptr,
      }
    }
  }
}

impl<'a, E> Entry<'a, E>
where
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
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }
}

impl<'a, E> Entry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

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
  E: VersionedMemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  version: u64,
  query_version: u64,
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for Key<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
  
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
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      version: self.version,
      query_version: self.query_version,
      ptr: self.ptr
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  #[inline]
  pub(crate) fn with_version(ptr: *const u8, ent: E, query_version: u64) -> Self {
    let kp = ent.key();
    let version = ent.version();
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        version,
        query_version,
        ent,
        ptr,
      }
    }
  }
}

impl<'a, E> Key<'a, E>
where
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
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }
}

impl<'a, E> Key<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  version: u64,
  query_version: u64,
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for Value<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
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
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
      ptr: self.ptr,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  #[inline]
  pub(crate) fn with_version(ptr: *const u8, ent: E, query_version: u64) -> Self {
    let kp = ent.key();
    let vp = ent
      .value()
      .expect("value must be present on Value");
    let version = ent.version();
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        value: slice::from_raw_parts(ptr.add(vp.offset()), vp.len()),
        version,
        query_version,
        ent,
        ptr,
      }
    }
  }
}

impl<'a, E> Value<'a, E>
where
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
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }
}

impl<'a, E> Value<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }
}

/// The reference to an entry in the generic WALs.
pub struct VersionedEntry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  ent: E,
  key: &'a [u8],
  value: Option<&'a [u8]>,
  version: u64,
  query_version: u64,
  ptr: *const u8,
}

impl<'a, E> core::fmt::Debug for VersionedEntry<'a, E>
where
  E: VersionedMemtableEntry<'a> + core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("VersionedEntry")
      .field("key", &self.key())
      .field("value", &self.value())
      .field("version", &self.version)
      .finish()
  }
}

impl<'a, E> Clone for VersionedEntry<'a, E>
where
  E: VersionedMemtableEntry<'a> + Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key.clone(),
      value: self.value.clone(),
      version: self.version,
      query_version: self.query_version,
      ptr: self.ptr,
    }
  }
}

impl<'a, E> VersionedEntry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  #[inline]
  pub(crate) fn with_version(ptr: *const u8, ent: E, query_version: u64) -> Self {
    let kp = ent.key();
    let vp = ent.value();
    let version = ent.version();
    unsafe {
      Self {
        key: slice::from_raw_parts(ptr.add(kp.offset()), kp.len()),
        value: vp.map(|vp| slice::from_raw_parts(ptr.add(vp.offset()), vp.len())),
        version,
        query_version,
        ent,
        ptr,
      }
    }
  }
}

impl<'a, E> VersionedEntry<'a, E>
where
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
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version(self.ptr, ent, self.query_version))
  }
}

impl<'a, E> VersionedEntry<'a, E>
where
  E: VersionedMemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub const fn version(&self) -> u64 {
    self.version
  }

  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> Option<&'a [u8]> {
    self.value
  }
}
