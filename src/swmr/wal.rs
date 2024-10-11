use crate::{
  generic::{GenericComparator, GenericPointer, GenericVersionPointer},
  pointer::{Pointer, VersionPointer},
  sealed::{self, Constructable},
  Ascend,
};
use dbutils::{checksum::Crc32, traits::Type, Comparator};
use rarena_allocator::Allocator;

// pub use crate::{
//   batch::{Batch, BatchWithBuilders, BatchWithKeyBuilder, BatchWithValueBuilder},
//   builder::Builder,
//   wal::{Reader, Wal},
//   Comparator, KeyBuilder, VacantBuffer, ValueBuilder,
// };

use core::{cell::UnsafeCell, marker::PhantomData};
use rarena_allocator::sync::Arena;
use std::sync::Arc;

mod reader;
pub use reader::*;

use super::c::OrderCore;

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
pub struct GenericOrderWal<K: ?Sized, V: ?Sized, P, S = Crc32> {
  core: Arc<UnsafeCell<OrderCore<P, GenericComparator<K>, S>>>,
  _s: PhantomData<S>,
  _v: PhantomData<V>,
}

impl<K, V, P, S> Constructable for GenericOrderWal<K, V, P, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  P: sealed::Pointer<Comparator = GenericComparator<K>> + Ord + Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderCore<Self::Pointer, Self::Comparator, Self::Checksumer>;
  type Pointer = P;
  type Checksumer = S;
  type Comparator = GenericComparator<K>;
  type Reader = GenericOrderWalReader<K, V, P, S>;

  #[inline]
  fn as_core(&self) -> &Self::Core {
    unsafe { &*self.core.get() }
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Core {
    unsafe { &mut *self.core.get() }
  }

  #[inline]
  fn from_core(core: Self::Core) -> Self {
    Self {
      core: Arc::new(UnsafeCell::new(core)),
      _s: PhantomData,
      _v: PhantomData,
    }
  }
}

impl<K, V, P, S> GenericOrderWal<K, V, P, S>
where
  K: ?Sized + 'static,
  V: ?Sized + 'static,
  S: 'static,
  P: sealed::Pointer<Comparator = GenericComparator<K>> + Ord + Send + 'static,
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

impl<K, V, S> crate::generic::base::Writer<K, V> for GenericOrderWal<K, V, GenericPointer<K, V>, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  GenericPointer<K, V>: Ord,
  S: 'static,
{
  // type Reader = GenericOrderWalReader<K, V, GenericPointer<K, V>, S>;

  #[inline]
  fn reader(&self) -> Self::Reader {
    GenericOrderWalReader::new(self.core.clone())
  }
}

impl<K, V, S> crate::generic::mvcc::Writer<K, V>
  for GenericOrderWal<K, V, GenericVersionPointer<K, V>, S>
where
  K: ?Sized + Type + Ord + 'static,
  V: ?Sized + Type + 'static,
  GenericVersionPointer<K, V>: Ord,
  S: 'static,
{
  // type Reader = GenericOrderWalReader<K, V, GenericVersionPointer<K, V>, S>;

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

impl<P, C, S> Constructable for OrderWal<P, C, S>
where
  C: 'static,
  S: 'static,
  P: sealed::Pointer<Comparator = C> + Ord + Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderCore<P, C, S>;
  type Pointer = P;
  type Checksumer = S;
  type Comparator = C;
  type Reader = OrderWalReader<P, C, S>;

  #[inline]
  fn as_core(&self) -> &Self::Core {
    unsafe { &*self.core.get() }
  }

  #[inline]
  fn as_core_mut(&mut self) -> &mut Self::Core {
    unsafe { &mut *self.core.get() }
  }

  #[inline]
  fn from_core(core: Self::Core) -> Self {
    Self {
      core: Arc::new(UnsafeCell::new(core)),
      _s: PhantomData,
    }
  }
}

impl<P, C, S> OrderWal<P, C, S>
where
  P: sealed::Pointer<Comparator = C> + Ord + Send + 'static,
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

impl<C, S> crate::base::Writer for OrderWal<Pointer<C>, C, S>
where
  C: Comparator + Send + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}

impl<C, S> crate::mvcc::Writer for OrderWal<VersionPointer<C>, C, S>
where
  C: Comparator + Send + 'static,
  S: 'static,
{
  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}
