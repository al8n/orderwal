use crate::{
  sealed::{self, Constructable, Memtable},
  wal::{
    bytes::pointer::{Pointer, VersionPointer},
    generic::{GenericComparator, GenericPointer, GenericVersionPointer},
  },
  Ascend,
};
use dbutils::{checksum::Crc32, traits::Type, Comparator};
use rarena_allocator::Allocator;

use core::{cell::UnsafeCell, marker::PhantomData};
use rarena_allocator::sync::Arena;
use std::sync::Arc;

use super::{
  reader::{GenericOrderWalReader, OrderWalReader},
  wal::OrderCore,
};

#[cfg(all(
  test,
  any(
    all_tests,
    test_swmr_constructor,
    test_swmr_insert,
    test_swmr_get,
    test_swmr_iters,
  )
))]
mod tests;

/// A ordered write-ahead log implementation for concurrent thread environments.
pub struct GenericOrderWal<K: ?Sized, V: ?Sized, M, S = Crc32> {
  pub(super) core: Arc<UnsafeCell<OrderCore<M, GenericComparator<K>, S>>>,
  pub(super) _s: PhantomData<S>,
  pub(super) _v: PhantomData<V>,
}

impl<K: ?Sized, V: ?Sized, P, S> GenericOrderWal<K, V, P, S> {
  #[inline]
  pub(super) const fn construct(
    core: Arc<UnsafeCell<OrderCore<P, GenericComparator<K>, S>>>,
  ) -> Self {
    Self {
      core,
      _s: PhantomData,
      _v: PhantomData,
    }
  }
}

impl<K, V, M, S> Constructable for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: Memtable + 'static,
  M::Pointer: sealed::Pointer<Comparator = GenericComparator<K>> + Ord + Send + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<Self::Memtable, Self::Comparator, Self::Checksumer>;
  type Memtable = M;
  type Checksumer = S;
  type Comparator = GenericComparator<K>;
  type Reader = GenericOrderWalReader<K, V, M, S>;

  #[inline]
  fn as_core(&self) -> &Self::Wal {
    unsafe { &*self.core.get() }
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Wal {
    unsafe { &mut *self.core.get() }
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self {
      core: Arc::new(UnsafeCell::new(core)),
      _s: PhantomData,
      _v: PhantomData,
    }
  }
}

impl<K, V, M, S> GenericOrderWal<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: Memtable + 'static,
  M::Pointer: sealed::Pointer<Comparator = GenericComparator<K>> + Ord + Send + 'static,
{
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{unsync::GenericOrderWal, Wal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = Builder::new().with_capacity(100).alloc::<GenericOrderWal>().unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
  pub fn path_buf(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.as_core().arena.path()
  }
}

impl<K, V, M, S> crate::wal::generic::base::Writer<K, V> for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: Memtable<Pointer = GenericPointer<K, V>> + 'static,
  GenericPointer<K, V>: Ord,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    GenericOrderWalReader::new(self.core.clone())
  }
}

impl<K, V, M, S> crate::wal::generic::mvcc::Writer<K, V> for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: Memtable<Pointer = GenericVersionPointer<K, V>> + 'static,
  GenericVersionPointer<K, V>: Ord,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    GenericOrderWalReader::new(self.core.clone())
  }
}

/// An ordered write-ahead log implementation for single thread environments.
pub struct OrderWal<P, C = Ascend, S = Crc32> {
  core: Arc<UnsafeCell<OrderCore<P, C, S>>>,
  _s: PhantomData<S>,
}

impl<P, C, S> OrderWal<P, C, S> {
  #[inline]
  pub(super) const fn construct(core: Arc<UnsafeCell<OrderCore<P, C, S>>>) -> Self {
    Self {
      core,
      _s: PhantomData,
    }
  }
}

impl<M, C, S> Constructable for OrderWal<M, C, S>
where
  C: 'static,
  S: 'static,
  M: Memtable + 'static,
  M::Pointer: sealed::Pointer<Comparator = C> + Ord + Send + 'static,
{
  type Allocator = Arena;
  type Wal = OrderCore<Self::Memtable, C, S>;
  type Memtable = M;
  type Checksumer = S;
  type Comparator = C;
  type Reader = OrderWalReader<M, C, S>;

  #[inline]
  fn as_core(&self) -> &Self::Wal {
    unsafe { &*self.core.get() }
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Wal {
    unsafe { &mut *self.core.get() }
  }

  #[inline]
  fn from_core(core: Self::Wal) -> Self {
    Self {
      core: Arc::new(UnsafeCell::new(core)),
      _s: PhantomData,
    }
  }
}

impl<M, C, S> OrderWal<M, C, S>
where
  M: Memtable + 'static,
  M::Pointer: sealed::Pointer<Comparator = C> + Ord + Send + 'static,
  C: 'static,
  S: 'static,
{
  /// Returns the path of the WAL if it is backed by a file.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{unsync::OrderWal, Wal, Builder};
  ///
  /// // A in-memory WAL
  /// let wal = Builder::new().with_capacity(100).alloc::<OrderWal>().unwrap();
  ///
  /// assert!(wal.path_buf().is_none());
  /// ```
  pub fn path_buf(&self) -> Option<&std::sync::Arc<std::path::PathBuf>> {
    self.as_core().arena.path()
  }
}

impl<M, C, S> crate::wal::bytes::base::Writer for OrderWal<M, C, S>
where
  M: Memtable<Pointer = Pointer<C>> + 'static,
  C: Comparator + Send + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}

impl<M, C, S> crate::wal::bytes::mvcc::Writer for OrderWal<M, C, S>
where
  M: Memtable<Pointer = VersionPointer<C>> + 'static,
  C: Comparator + Send + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}
