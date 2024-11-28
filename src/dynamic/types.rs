pub use dbutils::{
  buffer::{BufWriter, BufWriterOnce, VacantBuffer},
  types::*,
};
use sealed::Sealed as _;

use core::marker::PhantomData;

use crate::{dynamic::memtable::MemtableEntry, WithVersion};

/// The reference to an entry in the generic WALs.
pub struct Entry<'a, E>
where
  E: MemtableEntry<'a>,
{
  ent: E,
  query_version: u64,
  _m: PhantomData<&'a ()>,
}

impl<'a, E> core::fmt::Debug for Entry<'a, E>
where
  E: MemtableEntry<'a> + core::fmt::Debug,
  E::Value: crate::dynamic::types::Value<'a>,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if self.ent.value().all_versions() {
      f.debug_struct("Entry")
        .field("query_version", &self.query_version)
        .field("entry", &self.ent)
        .finish()
    } else {
      self.ent.fmt(f)
    }
  }
}

impl<'a, E> Clone for Entry<'a, E>
where
  E: MemtableEntry<'a> + Clone,
  E::Value: Clone,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
      query_version: self.query_version,
      _m: PhantomData,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
{
  #[inline]
  pub(crate) fn new(ent: E) -> Self {
    Self {
      query_version: 0,
      ent,
      _m: PhantomData,
    }
  }

  #[inline]
  pub(crate) fn with_version(ent: E, query_version: u64) -> Self {
    Self {
      query_version,
      ent,
      _m: PhantomData,
    }
  }
}

impl<'a, E> Entry<'a, E>
where
  E: MemtableEntry<'a>,
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
  E: MemtableEntry<'a>,
{
  /// Returns the version of the entry.
  #[inline]
  pub fn version(&self) -> u64
  where
    E: WithVersion,
  {
    self.ent.version()
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &'a [u8] {
    self.ent.key()
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> <E::Value as Value<'a>>::Ref
  where
    E::Value: Value<'a>,
  {
    self.ent.value().as_ref()
  }
}


/// Value that can be converted from a byte slice.
pub trait Value<'a>: sealed::Sealed<'a, Self::Ref> {
  /// The reference type.
  type Ref;
}

impl<'a> Value<'a> for &'a [u8] {
  type Ref = &'a [u8];
}

impl<'a> Value<'a> for Option<&'a [u8]> {
  type Ref = Option<&'a [u8]>;
}

mod sealed {
  pub trait Sealed<'a, R> {
    fn as_ref(&self) -> R;

    fn from_value_bytes(src: Option<&'a [u8]>) -> Self
    where
      Self: 'a;

    fn is_removed(&self) -> bool;

    fn all_versions(&self) -> bool;
  }

  impl<'a> Sealed<'a, Option<&'a [u8]>> for Option<&'a [u8]> {
    #[inline]
    fn as_ref(&self) -> Option<&'a [u8]> {
      self.as_ref().copied()
    }

    #[inline]
    fn from_value_bytes(src: Option<&'a [u8]>) -> Self {
      src
    }

    #[inline]
    fn is_removed(&self) -> bool {
      self.is_none()
    }

    #[inline]
    fn all_versions(&self) -> bool {
      true
    }
  }

  impl<'a> Sealed<'a, &'a [u8]> for &'a [u8] {
    #[inline]
    fn as_ref(&self) -> &'a [u8] {
      self
    }

    #[inline]
    fn from_value_bytes(src: Option<&'a [u8]>) -> Self {
      match src {
        Some(v) => v,
        None => panic!("cannot convert None to Value"),
      }
    }

    #[inline]
    fn is_removed(&self) -> bool {
      false
    }

    #[inline]
    fn all_versions(&self) -> bool {
      false
    }
  }
}
