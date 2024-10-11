use core::{cell::UnsafeCell, marker::PhantomData};
use std::sync::Arc;

use rarena_allocator::sync::Arena;

use crate::{
  sealed::{self, Constructable, Immutable},
  swmr::wal::OrderCore,
  wal::generic::GenericComparator,
};

use super::writer::{GenericOrderWal, OrderWal};

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
  pub(super) fn new(wal: Arc<UnsafeCell<OrderCore<P, GenericComparator<K>, S>>>) -> Self {
    Self(GenericOrderWal::construct(wal))
  }
}

impl<K, V, P, S> Constructable for GenericOrderWalReader<K, V, P, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  P: sealed::Pointer<Comparator = GenericComparator<K>> + Ord + Send + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<Self::Pointer, Self::Comparator, Self::Checksumer>;
  type Pointer = P;
  type Checksumer = S;
  type Comparator = GenericComparator<K>;
  type Reader = GenericOrderWalReader<K, V, P, S>;

  #[inline]
  fn as_core(&self) -> &Self::Wal {
    self.0.as_core()
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Wal {
    self.0.as_core_mut()
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self(GenericOrderWal {
      core: Arc::new(UnsafeCell::new(core)),
      _s: PhantomData,
      _v: PhantomData,
    })
  }
}

/// An [`OrderWal`] reader.
pub struct OrderWalReader<P, C, S>(OrderWal<P, C, S>);

impl<P, C, S> OrderWalReader<P, C, S> {
  /// Creates a new read-only WAL reader.
  #[inline]
  pub(super) fn new(wal: Arc<UnsafeCell<OrderCore<P, C, S>>>) -> Self {
    Self(OrderWal::construct(wal))
  }
}

impl<P, C, S> Immutable for OrderWalReader<P, C, S> {}

impl<P, C, S> Constructable for OrderWalReader<P, C, S>
where
  C: 'static,
  S: 'static,
  P: sealed::Pointer<Comparator = C> + Ord + Send + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<P, C, S>;
  type Pointer = P;
  type Checksumer = S;
  type Comparator = C;
  type Reader = Self;

  #[inline]
  fn as_core(&self) -> &Self::Wal {
    self.0.as_core()
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Wal {
    self.0.as_core_mut()
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self(OrderWal::construct(Arc::new(UnsafeCell::new(core))))
  }
}
