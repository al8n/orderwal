use super::super::*;

use among::Among;
use either::Either;
use error::Error;

use core::ptr::NonNull;
use rarena_allocator::{sync::Arena, ArenaPosition, Error as ArenaError};
use std::sync::Arc;

mod reader;
pub use reader::*;

struct OrderWalCore<C, S> {
  arena: Arena,
  map: SkipSet<Pointer<C>>,
  opts: Options,
  cmp: C,
  cks: UnsafeCellChecksumer<S>,
}

walcore!(SkipSet: Send);

impl<C, S> OrderWalCore<C, S> {
  #[inline]
  fn construct(
    arena: Arena,
    set: SkipSet<Pointer<C>>,
    opts: Options,
    cmp: C,
    checksumer: S,
  ) -> Self {
    Self {
      arena,
      map: set,
      cmp,
      opts,
      cks: UnsafeCellChecksumer::new(checksumer),
    }
  }
}

/// A single writer multiple readers ordered write-ahead log implementation.
///
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
// ```text
// +----------------------+-------------------------+--------------------+
// | magic text (6 bytes) | magic version (2 bytes) |  header (8 bytes)  |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------|--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// ```
pub struct OrderWal<C = Ascend, S = Crc32> {
  core: Arc<OrderWalCore<C, S>>,
  ro: bool,
  _s: PhantomData<S>,
}

impl<C, S> OrderWal<C, S> {
  /// Returns the number of entries in the WAL.
  #[inline]
  pub fn len(&self) -> usize {
    self.core.map.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.core.map.is_empty()
  }

  /// Returns a read-only view of the WAL.
  #[inline]
  pub fn reader(&self) -> OrderWalReader<C, S> {
    OrderWalReader::new(self.core.clone())
  }

  #[inline]
  fn from_core(core: OrderWalCore<C, S>, ro: bool) -> Self {
    Self {
      core: Arc::new(core),
      ro,
      _s: PhantomData,
    }
  }
}

impl_common_methods!();

impl_common_methods!(<C: Comparator, S>);

impl_common_methods!(<S: Checksumer>);

impl_common_methods!(
  where
    C: Comparator + CheapClone + Send + 'static,
);

impl_common_methods!(
  Self: where
  C: Comparator + CheapClone + Send + 'static,
  S: Checksumer,
);

impl_common_methods!(swmr <C, S>);

impl_common_methods!(tests swmr);
