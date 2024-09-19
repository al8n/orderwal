use wal::sealed::{Base, WalCore};

use super::*;

pub struct OrderWalCore<C, S> {
  pub(super) arena: Arena,
  pub(super) map: BTreeSet<Pointer<C>>,
  pub(super) opts: Options,
  pub(super) cmp: C,
  pub(super) cks: S,
}

impl<C> Base for BTreeSet<Pointer<C>>
where
  C: Comparator,
{
  type Pointer = Pointer<C>;

  fn insert(&mut self, ele: Self::Pointer) {
    BTreeSet::insert(self, ele);
  }
}

impl<C, S> WalCore<C, S> for OrderWalCore<C, S>
where
  C: Comparator,
{
  type Allocator = Arena;
  type Base = BTreeSet<Pointer<C>>;
  type Pointer = Pointer<C>;

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
