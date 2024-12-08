use super::{
  super::{sealed::Constructable, swmr::wal::OrderCore},
  writer::OrderWal,
};
use crate::{memtable::Memtable, Immutable};
use rarena_allocator::sync::Arena;
use std::sync::Arc;

/// An [`OrderWal`] reader.
pub struct OrderWalReader<P, S>(OrderWal<P, S>);

impl<M, S> core::fmt::Debug for OrderWalReader<M, S> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_tuple("OrderWalReader").field(&self.0.core).finish()
  }
}

impl<P, S> Immutable for OrderWalReader<P, S> {}

impl<P, S> OrderWalReader<P, S> {
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<OrderCore<P, S>>) -> Self {
    Self(OrderWal::construct(wal))
  }
}

impl<M, S> Constructable for OrderWalReader<M, S>
where
  S: 'static,
  M: Memtable + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<Self::Memtable, Self::Checksumer>;
  type Memtable = M;
  type Checksumer = S;
  type Reader = OrderWalReader<M, S>;

  #[inline]
  fn as_wal(&self) -> &Self::Wal {
    self.0.as_wal()
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self(OrderWal {
      core: Arc::new(core),
    })
  }
}
