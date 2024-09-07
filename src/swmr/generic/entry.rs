use crossbeam_skiplist::set::Entry;
use rarena_allocator::either::Either;

use super::{Pointer, Type, TypeRef};

/// The reference to an entry in the [`GenericOrderWal`](super::GenericOrderWal).
pub struct EntryRef<'a, K: Type, V: Type> {
  ent: Entry<'a, Pointer<K, V>>,
}

impl<'a, K: Type, V: Type> Clone for EntryRef<'a, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
    }
  }
}

impl<'a, K: Type, V: Type> EntryRef<'a, K, V> {
  #[inline]
  pub(super) fn new(ent: Entry<'a, Pointer<K, V>>) -> Self {
    Self { ent }
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> Either<&K, K::Ref<'a>> {
    let p = self.ent.value();
    if let Some(k) = &p.cached_key {
      Either::Left(k)
    } else {
      Either::Right(TypeRef::from_slice(p.as_key_slice()))
    }
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> Either<&V, V::Ref<'a>> {
    let p = self.ent.value();
    if let Some(v) = &p.cached_value {
      Either::Left(v)
    } else {
      Either::Right(TypeRef::from_slice(p.as_value_slice()))
    }
  }
}
