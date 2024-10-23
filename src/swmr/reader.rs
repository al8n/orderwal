use core::cell::UnsafeCell;
use std::sync::Arc;

use rarena_allocator::sync::Arena;

use crate::{
  memtable::BaseTable,
  sealed::{self, Constructable, Immutable},
  swmr::wal::OrderCore,
};

use super::writer::GenericOrderWal;

/// An [`GenericOrderWal`] reader.
pub struct GenericOrderWalReader<K: ?Sized, V: ?Sized, P, S>(GenericOrderWal<K, V, P, S>);

impl<K: ?Sized, V: ?Sized, P, S> Immutable for GenericOrderWalReader<K, V, P, S> {}

impl<K, V, P, S> GenericOrderWalReader<K, V, P, S>
where
  K: ?Sized,
  V: ?Sized,
{
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<UnsafeCell<OrderCore<K, V, P, S>>>) -> Self {
    Self(GenericOrderWal::construct(wal))
  }
}

impl<K, V, M, S> Constructable<K, V> for GenericOrderWalReader<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: BaseTable + 'static,
  M::Pointer: sealed::Pointer + Ord + Send + 'static,
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
  fn as_wal_mut(&mut self) -> &mut Self::Wal {
    self.0.as_wal_mut()
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self(GenericOrderWal {
      core: Arc::new(UnsafeCell::new(core)),
    })
  }
}
