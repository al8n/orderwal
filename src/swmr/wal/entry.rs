use crossbeam_skiplist::set::Entry;

use super::Pointer;

/// The reference to an entry in the [`GenericOrderWal`](super::GenericOrderWal).
pub struct EntryRef<'a, C> {
  ent: Entry<'a, Pointer<C>>,
}

impl<'a, C> Clone for EntryRef<'a, C> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ent: self.ent.clone(),
    }
  }
}

impl<'a, C> EntryRef<'a, C> {
  #[inline]
  pub(super) fn new(ent: Entry<'a, Pointer<C>>) -> Self {
    Self { ent }
  }

  /// Returns the key of the entry.
  #[inline]
  pub fn key(&self) -> &[u8] {
    let p = self.ent.value();
    p.as_key_slice()
  }

  /// Returns the value of the entry.
  #[inline]
  pub fn value(&self) -> &[u8] {
    let p = self.ent.value();
    p.as_value_slice()
  }
}
