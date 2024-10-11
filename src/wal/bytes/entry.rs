use crate::sealed::{MemtableEntry, Pointer, WithVersion, WithoutVersion};

/// The reference to an entry in the WALs.
pub struct Entry<'a, E> {
  ent: E,
  key: &'a [u8],
  value: &'a [u8],
  version: Option<u64>,
}

impl<E> core::fmt::Debug for Entry<'_, E>
where
  E: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Entry")
      .field("key", &self.key())
      .field("value", &self.value())
      .finish()
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
    Self::with_version_in(ent, false)
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer + WithVersion,
{
  #[inline]
  pub(super) fn with_version(ent: E) -> Self {
    Self::with_version_in(ent, true)
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
  E::Pointer: Pointer,
{
  #[inline]
  fn with_version_in(ent: E, version: bool) -> Self {
    let ptr = ent.pointer();
    Self {
      key: ptr.as_key_slice(),
      value: ptr.as_value_slice(),
      version: if version { Some(ptr.version()) } else { None },
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
    self
      .ent
      .next()
      .map(|ent| Self::with_version_in(ent, self.version.is_some()))
  }

  /// Returns the previous entry in the WALs.
  ///
  /// This does not move the cursor.
  #[inline]
  pub fn prev(&mut self) -> Option<Self> {
    self
      .ent
      .prev()
      .map(|ent| Self::with_version_in(ent, self.version.is_some()))
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
