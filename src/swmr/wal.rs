use crate::{
  pointer::{MvccPointer, Pointer},
  sealed::Constructable,
  Ascend,
};
use dbutils::{checksum::Crc32, Comparator};
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

/// An ordered write-ahead log implementation for single thread environments.
pub struct OrderWal<P, C = Ascend, S = Crc32> {
  core: Arc<UnsafeCell<OrderCore<P, C, S>>>,
  _s: PhantomData<S>,
}

impl<P, C, S> Constructable<C, S> for OrderWal<P, C, S>
where
  C: 'static,
  S: 'static,
  P: Ord + Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderCore<P, C, S>;
  type Pointer = P;

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

impl<P: Ord + Send + 'static, C: 'static, S: 'static> OrderWal<P, C, S> {
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

impl<C, S> crate::base::Writer<C, S> for OrderWal<Pointer<C>, C, S>
where
  C: Comparator + Send + 'static,
  S: 'static,
{
  type Reader = OrderWalReader<Pointer<C>, C, S>;

  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}

impl<C, S> crate::mvcc::Writer<C, S> for OrderWal<MvccPointer<C>, C, S>
where
  C: Comparator + Send + 'static,
  S: 'static,
{
  type Reader = OrderWalReader<MvccPointer<C>, C, S>;

  #[inline]
  fn reader(&self) -> Self::Reader {
    OrderWalReader::new(self.core.clone())
  }
}
