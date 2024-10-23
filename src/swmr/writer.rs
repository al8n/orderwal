use crate::{
  memtable::{BaseTable, Memtable, MultipleVersionMemtable},
  sealed::{self, Constructable, WithVersion},
  wal::{GenericPointer, GenericVersionPointer},
};
use dbutils::{checksum::Crc32, traits::Type};
use rarena_allocator::{sync::Arena, Allocator};

use core::cell::UnsafeCell;
use std::sync::Arc;

use super::{reader::GenericOrderWalReader, wal::OrderCore};

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
  pub(super) core: Arc<UnsafeCell<OrderCore<K, V, M, S>>>,
}

impl<K: ?Sized, V: ?Sized, P, S> GenericOrderWal<K, V, P, S> {
  #[inline]
  pub(super) const fn construct(core: Arc<UnsafeCell<OrderCore<K, V, P, S>>>) -> Self {
    Self { core }
  }
}

impl<K, V, M, S> Constructable<K, V> for GenericOrderWal<K, V, M, S>
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
    }
  }
}

impl<K, V, M, S> GenericOrderWal<K, V, M, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  M: BaseTable + 'static,
  M::Pointer: sealed::Pointer + Ord + Send + 'static,
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

impl<K, V, M, S> crate::wal::base::Writer<K, V> for GenericOrderWal<K, V, M, S>
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

impl<K, V, M, S> crate::wal::multiple_version::Writer<K, V> for GenericOrderWal<K, V, M, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  M: MultipleVersionMemtable<Pointer = GenericVersionPointer<K, V>> + WithVersion + 'static,
  GenericVersionPointer<K, V>: Ord,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    GenericOrderWalReader::new(self.core.clone())
  }
}
