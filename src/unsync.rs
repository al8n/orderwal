use super::*;

use among::Among;
use either::Either;
use error::Error;

use core::ptr::NonNull;
use rarena_allocator::{unsync::Arena, ArenaPosition, Error as ArenaError};
use std::collections::BTreeSet;

mod iter;
pub use iter::*;

struct OrderWalCore<C, S> {
  arena: Arena,
  map: BTreeSet<Pointer<C>>,
  opts: Options,
  cmp: C,
  cks: S,
}

walcore!(BTreeSet);

impl<C, S> OrderWalCore<C, S> {
  #[inline]
  fn construct(
    arena: Arena,
    set: BTreeSet<Pointer<C>>,
    opts: Options,
    cmp: C,
    checksumer: S,
  ) -> Self {
    Self {
      arena,
      map: set,
      cmp,
      opts,
      cks: checksumer,
    }
  }
}

/// An ordered write-ahead log implementation for single thread environments.
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
  core: OrderWalCore<C, S>,
  ro: bool,
  _s: PhantomData<S>,
}

impl<C, S> OrderWal<C, S> {
  /// Gets an iterator that visits the elements in the `OrderWal` in ascending
  /// order.
  #[inline]
  pub fn iter(&self) -> Iter<C> {
    Iter::new(self.core.map.iter())
  }

  /// Gets an iterator over the keys of the `OrderWal`, in sorted order.
  #[inline]
  pub fn keys(&self) -> Keys<C> {
    Keys::new(self.core.map.iter())
  }

  /// Gets an iterator over the values of the `OrderWal`, in sorted order.
  #[inline]
  pub fn values(&self) -> Values<C> {
    Values::new(self.core.map.iter())
  }

  #[inline]
  const fn from_core(core: OrderWalCore<C, S>, ro: bool) -> Self {
    Self {
      core,
      ro,
      _s: PhantomData,
    }
  }
}

impl_common_methods!();

impl_common_methods!(<S: Checksumer>);

impl_common_methods!(<C: Comparator, S>);

impl_common_methods!(
  where
    C: Comparator + CheapClone + 'static,
);

impl_common_methods!(
  Self mut: where
  C: Comparator + CheapClone + 'static,
  S: Checksumer,
);

impl_common_methods!(unsync <C, S>);

impl_common_methods!(tests unsync);
