use crossbeam_skiplist::set::Entry;

use super::{Pointer, Type, TypeRef};

/// The reference to an entry in the [`GenericOrderWal`](super::GenericOrderWal).
pub struct EntryRef<'a, K, V> {
  ent: Entry<'a, Pointer<K, V>>,
}

impl<'a, K, V> core::fmt::Debug for EntryRef<'a, K, V>
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

impl<K, V> Clone for EntryRef<'_, K, V> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
    }
  }
}

impl<'a, K, V> EntryRef<'a, K, V> {
  #[inline]
  pub(super) fn new(ent: Entry<'a, Pointer<K, V>>) -> Self {
    Self { ent }
  }
}

impl<'a, K, V> EntryRef<'a, K, V>
where
  K: Type,
  V: Type,
{
  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> K::Ref<'a> {
    let p = self.ent.value();
    TypeRef::from_slice(p.as_key_slice())
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> V::Ref<'a> {
    let p = self.ent.value();
    TypeRef::from_slice(p.as_value_slice())
  }
}
