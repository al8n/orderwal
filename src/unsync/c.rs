use wal::sealed::{Base, WalCore};

use super::*;

pub struct OrderWalCore<C, S> {
  pub(super) arena: Arena,
  pub(super) map: BTreeSet<Pointer<C>>,
  pub(super) opts: Options,
  pub(super) cmp: C,
  pub(super) cks: S,
}

impl<C> Base<C> for BTreeSet<Pointer<C>> {
  fn insert(&mut self, ele: Pointer<C>)
  where
    C: Comparator,
  {
    BTreeSet::insert(self, ele);
  }
}

impl<C, S> WalCore<C, S> for OrderWalCore<C, S> {
  type Allocator = Arena;
  type Base = BTreeSet<Pointer<C>>;

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
