use std::sync::Arc;

use rarena_allocator::sync::Arena;

use crate::{
  memtable::BaseTable,
  sealed::{Constructable, Immutable},
  swmr::wal::OrderCore,
};

use super::writer::GenericOrderWal;

/// An [`GenericOrderWal`] reader.
pub struct GenericOrderWalReader<K: ?Sized, V: ?Sized, P, S>(GenericOrderWal<K, V, P, S>);

impl<K, V, M, S> core::fmt::Debug for GenericOrderWalReader<K, V, M, S>
where
  K: ?Sized,
  V: ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("GenericOrderWalReader")
      .field(&self.0.core)
      .finish()
  }
}

impl<K: ?Sized, V: ?Sized, P, S> Immutable for GenericOrderWalReader<K, V, P, S> {}

impl<K, V, P, S> GenericOrderWalReader<K, V, P, S>
where
  K: ?Sized,
  V: ?Sized,
{
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<OrderCore<K, V, P, S>>) -> Self {
    Self(GenericOrderWal::construct(wal))
  }
}

impl<K, V, M, S> Constructable for GenericOrderWalReader<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: BaseTable<Key = K, Value = V> + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<K, V, Self::Memtable, Self::Checksumer>;
  type Memtable = M;
  type Checksumer = S;
  type Reader = GenericOrderWalReader<K, V, M, S>;

  #[inline]
  fn as_wal(&self) -> &Self::Wal {
    self.0.as_wal()
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self(GenericOrderWal {
      core: Arc::new(core),
    })
  }
}
