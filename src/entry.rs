use among::Among;
use crossbeam_skiplist::set::Entry as SetEntry;
use rarena_allocator::either::Either;

use super::{
  pointer::{GenericPointer, Pointer},
  wal::r#type::{Type, TypeRef},
  KeyBuilder, ValueBuilder,
};

/// An entry in the write-ahead log.
pub struct Entry<K, V, C> {
  pub(crate) key: K,
  pub(crate) value: V,
  pub(crate) pointer: Option<Pointer<C>>,
}

impl<K, V, C> Entry<K, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, value: V) -> Self {
    Self {
      key,
      value,
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, V) {
    (self.key, self.value)
  }
}

/// An entry in the write-ahead log.
pub struct EntryWithKeyBuilder<KB, V, C> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) value: V,
  pub(crate) pointer: Option<Pointer<C>>,
}

impl<KB, V, C> EntryWithKeyBuilder<KB, V, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, value: V) -> Self {
    Self {
      kb,
      value,
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> &V {
    &self.value
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, V) {
    (self.kb, self.value)
  }
}

/// An entry in the write-ahead log.
pub struct EntryWithValueBuilder<K, VB, C> {
  pub(crate) key: K,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<Pointer<C>>,
}

impl<K, VB, C> EntryWithValueBuilder<K, VB, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(key: K, vb: ValueBuilder<VB>) -> Self {
    Self {
      key,
      vb,
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the value.
  #[inline]
  pub const fn key(&self) -> &K {
    &self.key
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (K, ValueBuilder<VB>) {
    (self.key, self.vb)
  }
}

/// An entry in the write-ahead log.
pub struct EntryWithBuilders<KB, VB, C> {
  pub(crate) kb: KeyBuilder<KB>,
  pub(crate) vb: ValueBuilder<VB>,
  pub(crate) pointer: Option<Pointer<C>>,
}

impl<KB, VB, C> EntryWithBuilders<KB, VB, C> {
  /// Creates a new entry.
  #[inline]
  pub const fn new(kb: KeyBuilder<KB>, vb: ValueBuilder<VB>) -> Self {
    Self {
      kb,
      vb,
      pointer: None,
    }
  }

  /// Returns the value builder.
  #[inline]
  pub const fn value_builder(&self) -> &ValueBuilder<VB> {
    &self.vb
  }

  /// Returns the key builder.
  #[inline]
  pub const fn key_builder(&self) -> &KeyBuilder<KB> {
    &self.kb
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (KeyBuilder<KB>, ValueBuilder<VB>) {
    (self.kb, self.vb)
  }
}

/// An entry in the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
#[repr(transparent)]
pub struct Generic<'a, T> {
  value: Either<GenericVariant<'a, T>, &'a [u8]>,
}

impl<'a, T: Type> Generic<'a, T> {
  /// Returns the value contained in the generic.
  #[inline]
  pub const fn value(&self) -> Either<&GenericVariant<'a, T>, &'a [u8]> {
    match &self.value {
      Either::Left(v) => Either::Left(v),
      Either::Right(v) => Either::Right(v),
    }
  }

  /// Creates a new generic from bytes for querying or inserting into the [`GenericOrderWal`](crate::swmr::GenericOrderWal).
  ///
  /// ## Safety
  /// - the `slice` must the same as the one returned by [`T::encode`](Type::encode).
  #[inline]
  pub const unsafe fn from_slice(slice: &'a [u8]) -> Self {
    Self {
      value: Either::Right(slice),
    }
  }

  #[inline]
  pub(crate) fn into_among(self) -> Among<T, &'a T, &'a [u8]> {
    match self.value {
      Either::Left(val) => match val {
        GenericVariant::Owned(val) => Among::Left(val),
        GenericVariant::Ref(val) => Among::Middle(val),
      },
      Either::Right(val) => Among::Right(val),
    }
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    match &self.value {
      Either::Left(val) => val.encoded_len(),
      Either::Right(val) => val.len(),
    }
  }

  #[inline]
  pub(crate) fn encode(&self, buf: &mut [u8]) -> Result<(), T::Error> {
    match &self.value {
      Either::Left(val) => match val {
        GenericVariant::Owned(val) => val.encode(buf),
        GenericVariant::Ref(val) => val.encode(buf),
      },
      Either::Right(val) => {
        buf.copy_from_slice(val);
        Ok(())
      }
    }
  }
}

impl<'a, T> From<GenericVariant<'a, T>> for Generic<'a, T> {
  #[inline]
  fn from(value: GenericVariant<'a, T>) -> Self {
    Self {
      value: Either::Left(value),
    }
  }
}

impl<'a, T> From<&'a T> for Generic<'a, T> {
  #[inline]
  fn from(value: &'a T) -> Self {
    Self {
      value: Either::Left(GenericVariant::Ref(value)),
    }
  }
}

impl<T> From<T> for Generic<'_, T> {
  #[inline]
  fn from(value: T) -> Self {
    Self {
      value: Either::Left(GenericVariant::Owned(value)),
    }
  }
}

/// The kind of a generic type.
pub enum GenericVariant<'a, T> {
  /// An owned `T`.
  Owned(T),
  /// A reference of `T`.
  Ref(&'a T),
}

impl<T: Type> GenericVariant<'_, T> {
  #[inline]
  fn encoded_len(&self) -> usize {
    match self {
      Self::Owned(val) => val.encoded_len(),
      Self::Ref(val) => val.encoded_len(),
    }
  }
}

impl<T> From<T> for GenericVariant<'_, T> {
  fn from(value: T) -> Self {
    Self::Owned(value)
  }
}

impl<'a, T> From<&'a T> for GenericVariant<'a, T> {
  fn from(value: &'a T) -> Self {
    Self::Ref(value)
  }
}

/// An entry in the generic write-ahead log.
pub struct GenericEntryRefMut<'a, K, V> {
  pub(crate) key: Generic<'a, K>,
  pub(crate) value: Generic<'a, V>,
  pub(crate) pointer: Option<&'a mut GenericPointer<K, V>>,
}

/// An entry in the generic write-ahead log.
pub struct GenericEntry<'a, K, V> {
  pub(crate) key: Generic<'a, K>,
  pub(crate) value: Generic<'a, V>,
  pub(crate) pointer: Option<GenericPointer<K, V>>,
}

impl<'a, K, V> GenericEntry<'a, K, V> {
  /// Creates a new entry.
  #[inline]
  pub fn new(key: impl Into<Generic<'a, K>>, value: impl Into<Generic<'a, V>>) -> Self {
    Self {
      key: key.into(),
      value: value.into(),
      pointer: None,
    }
  }

  /// Returns the key.
  #[inline]
  pub const fn key(&self) -> Either<&GenericVariant<'a, K>, &'a [u8]> {
    match &self.key.value {
      Either::Left(v) => Either::Left(v),
      Either::Right(v) => Either::Right(v),
    }
  }

  /// Returns the value.
  #[inline]
  pub const fn value(&self) -> Either<&GenericVariant<'a, V>, &'a [u8]> {
    match &self.value.value {
      Either::Left(v) => Either::Left(v),
      Either::Right(v) => Either::Right(v),
    }
  }

  /// Consumes the entry and returns the key and value.
  #[inline]
  pub fn into_components(self) -> (Generic<'a, K>, Generic<'a, V>) {
    (self.key, self.value)
  }
 
  #[inline]
  pub(crate) fn as_ref_mut(&mut self) -> GenericEntryRefMut<'_, K, V> {
    let k = match &self.key.value {
      Either::Left(v) => match v {
        GenericVariant::Owned(k) => Generic { value: Either::Left(GenericVariant::Ref(k)) },
        GenericVariant::Ref(k) => Generic { value: Either::Left(GenericVariant::Ref(*k)) },
      },
      Either::Right(v) => Generic { value: Either::Right(v) },
    };

    let v = match &self.value.value {
      Either::Left(v) => match v {
        GenericVariant::Owned(v) => Generic { value: Either::Left(GenericVariant::Ref(v)) },
        GenericVariant::Ref(v) => Generic { value: Either::Left(GenericVariant::Ref(*v)) },
      },
      Either::Right(v) => Generic { value: Either::Right(v) },
    };

    GenericEntryRefMut {
      key: k,
      value: v,
      pointer: self.pointer.as_mut(),
    }
  }
}

/// The reference to an entry in the [`GenericOrderWal`](super::GenericOrderWal).
#[repr(transparent)]
pub struct GenericEntryRef<'a, K, V> {
  ent: SetEntry<'a, GenericPointer<K, V>>,
}

impl<'a, K, V> core::fmt::Debug for GenericEntryRef<'a, K, V>
where
  K: Type,
  K::Ref<'a>: core::fmt::Debug,
  V: Type,
  V::Ref<'a>: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("EntryRef")
      .field("key", &self.key())
      .field("value", &self.value())
      .finish()
  }
}

impl<K, V> Clone for GenericEntryRef<'_, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
    }
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V> {
  #[inline]
  pub(super) fn new(ent: SetEntry<'a, GenericPointer<K, V>>) -> Self {
    Self { ent }
  }
}

impl<'a, K, V> GenericEntryRef<'a, K, V>
where
  K: Type,
  V: Type,
{
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> K::Ref<'a> {
    let p = self.ent.value();
    unsafe { TypeRef::from_slice(p.as_key_slice()) }
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> V::Ref<'a> {
    let p = self.ent.value();
    unsafe { TypeRef::from_slice(p.as_value_slice()) }
  }
}
