use crate::{
  memtable::MemtableEntry,
  sealed::{Pointer, WithVersion, WithoutVersion},
};

/// The reference to an entry in the WALs.
pub struct Entry<'a, E> {
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  query_version: Option<u64>,
  version: Option<u64>,
}

impl<E> core::fmt::Debug for Entry<'_, E>
where
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

impl<E> Clone for Entry<'_, E>
where
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithoutVersion,
{
  #[inline]
  pub(super) fn new(ent: E) -> Self {
    Self::with_version_in(ent, None)
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithVersion,
{
  #[inline]
  pub(super) fn with_version(ent: E, query_version: u64) -> Self {
    Self::with_version_in(ent, Some(query_version))
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();

    Self {
      key: ptr.as_key_slice(),
      value: ptr.as_value_slice(),
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

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    if let Some(query_version) = self.query_version {
      let mut curr = self.ent.next();

      while let Some(mut ent) = curr {
        let p = ent.pointer();
        let version = p.version();
        let k = p.as_key_slice();

        // Do not yield the same key twice and check if the version is less than or equal to the query version.
        if version <= query_version && k != self.key {
          return Some(Self {
            key: k,
            value: p.as_value_slice(),
            version: Some(version),
            query_version: self.query_version,
            ent,
          });
        }

        curr = ent.next();
      }

      None
    } else {
      self
        .ent
        .next()
        .map(|ent| Self::with_version_in(ent, self.query_version))
    }
  }

  /// Returns the previous entry in the WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    if let Some(query_version) = self.query_version {
      let mut curr = self.ent.prev();

      while let Some(mut ent) = curr {
        let p = ent.pointer();
        let version = p.version();
        let k = p.as_key_slice();

        // Do not yield the same key twice and check if the version is less than or equal to the query version.
        if version <= query_version && k != self.key {
          return Some(Self {
            key: k,
            value: p.as_value_slice(),
            version: Some(version),
            query_version: self.query_version,
            ent,
          });
        }

        curr = ent.prev();
      }

      None
    } else {
      self
        .ent
        .prev()
        .map(|ent| Self::with_version_in(ent, self.query_version))
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, E> Entry<'a, E> {
  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }

  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }
}

/// The reference to a key of the entry in the generic WALs.
pub struct Key<'a, E> {
  ent: E,
  key: &'a [u8],
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<E> core::fmt::Debug for Key<'_, E>
where
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

impl<E> Clone for Key<'_, E>
where
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let key = ptr.as_key_slice();
    Self {
      key,
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

impl<'a, E> Key<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    if let Some(query_version) = self.query_version {
      let mut curr = self.ent.next();

      while let Some(mut ent) = curr {
        let p = ent.pointer();
        let version = p.version();
        let key = p.as_key_slice();
        // Do not yield the same key twice and check if the version is less than or equal to the query version.
        if version <= query_version && key != self.key {
          return Some(Self {
            key,
            version: Some(version),
            query_version: self.query_version,
            ent,
          });
        }

        curr = ent.next();
      }

      None
    } else {
      self
        .ent
        .next()
        .map(|ent| Self::with_version_in(ent, self.query_version))
    }
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    if let Some(query_version) = self.query_version {
      let mut curr = self.ent.prev();

      while let Some(mut ent) = curr {
        let p = ent.pointer();
        let version = p.version();
        let key = p.as_key_slice();
        // Do not yield the same key twice and check if the version is less than or equal to the query version.
        if version <= query_version && key != self.key {
          return Some(Self {
            key,
            version: Some(version),
            query_version: self.query_version,
            ent,
          });
        }

        curr = ent.prev();
      }

      None
    } else {
      self
        .ent
        .prev()
        .map(|ent| Self::with_version_in(ent, self.query_version))
    }
  }
}

impl<'a, E> Key<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, E> Key<'a, E> {
  /// Returns the key of the entry.
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }
}

/// The reference to a value of the entry in the generic WALs.
pub struct Value<'a, E> {
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  version: Option<u64>,
  query_version: Option<u64>,
}

impl<E> core::fmt::Debug for Value<'_, E>
where
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

impl<E> Clone for Value<'_, E>
where
  E: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      key: self.key,
      value: self.value,
      version: self.version,
      query_version: self.query_version,
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  pub(super) fn with_version_in(ent: E, query_version: Option<u64>) -> Self {
    let ptr = ent.pointer();
    let key = ptr.as_key_slice();
    Self {
      key,
      value: ptr.as_value_slice(),
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

impl<'a, E> Value<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  /// Returns the next entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  #[allow(clippy::should_implement_trait)]
  pub fn next(&mut self) -> Option<Self> {
    if let Some(query_version) = self.query_version {
      let mut curr = self.ent.next();

      while let Some(mut ent) = curr {
        let p = ent.pointer();
        let version = p.version();
        let key = p.as_key_slice();
        // Do not yield the same key twice and check if the version is less than or equal to the query version.
        if version <= query_version && key != self.key {
          return Some(Self {
            key,
            value: p.as_value_slice(),
            version: Some(version),
            query_version: self.query_version,
            ent,
          });
        }

        curr = ent.next();
      }

      None
    } else {
      self
        .ent
        .next()
        .map(|ent| Self::with_version_in(ent, self.query_version))
    }
  }

  /// Returns the previous entry in the generic WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    if let Some(query_version) = self.query_version {
      let mut curr = self.ent.prev();

      while let Some(mut ent) = curr {
        let p = ent.pointer();
        let version = p.version();
        let key = p.as_key_slice();
        // Do not yield the same key twice and check if the version is less than or equal to the query version.
        if version <= query_version && key != self.key {
          return Some(Self {
            key,
            value: p.as_value_slice(),
            version: Some(version),
            query_version: self.query_version,
            ent,
          });
        }

        curr = ent.prev();
      }

      None
    } else {
      self
        .ent
        .prev()
        .map(|ent| Self::with_version_in(ent, self.query_version))
    }
  }
}

impl<'a, E> Value<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: WithVersion,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64 {
    self.version.expect("version must be set")
  }
}

impl<'a, E> Value<'a, E> {
  /// Returns the value of the entry.
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }
}
