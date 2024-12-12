use core::{borrow::Borrow, cmp};

use skl::{
  dynamic::{BytesComparator, BytesEquivalentor},
  generic::{
    Comparator, Equivalentor, TypeRefComparator, TypeRefEquivalentor, TypeRefQueryComparator,
    TypeRefQueryEquivalentor,
  },
};
use triomphe::Arc;

use crate::types::{fetch_entry, fetch_raw_key, Query, RawEntryRef, RecordPointer, TypeMode};

pub struct MemtableComparator<C: ?Sized> {
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: Arc<C>,
}

impl<C: ?Sized> crate::types::sealed::ComparatorConstructor<C> for MemtableComparator<C> {
  #[inline]
  fn new(ptr: *const u8, cmp: Arc<C>) -> Self {
    Self { ptr, cmp }
  }
}

impl<C: ?Sized> crate::types::sealed::PointComparator<C> for MemtableComparator<C> {
  #[inline]
  fn fetch_entry<'a, T>(&self, kp: &RecordPointer) -> RawEntryRef<'a, T>
  where
    T: TypeMode,
    T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
    T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
  {
    unsafe { fetch_entry::<T>(self.ptr, kp) }
  }
}

impl<C: ?Sized> MemtableComparator<C> {
  #[inline]
  fn equivalent_key(&self, a: &RecordPointer, b: &[u8]) -> bool
  where
    C: BytesEquivalentor,
  {
    unsafe {
      let (_, ak) = fetch_raw_key(self.ptr, a);
      self.cmp.equivalent(ak, b)
    }
  }

  #[inline]
  fn equivalent_in(&self, a: &RecordPointer, b: &RecordPointer) -> bool
  where
    C: BytesEquivalentor,
  {
    unsafe {
      let (av, ak) = fetch_raw_key(self.ptr, a);
      let (bv, bk) = fetch_raw_key(self.ptr, b);

      match (av, bv) {
        (Some(av), Some(bv)) => self.cmp.equivalent(ak, bk) && av == bv,
        (None, None) => self.cmp.equivalent(ak, bk),
        _ => unreachable!("trying to compare versioned and non-versioned keys"),
      }
    }
  }

  #[inline]
  fn compare_key(&self, a: &RecordPointer, b: &[u8]) -> cmp::Ordering
  where
    C: BytesComparator,
  {
    unsafe {
      let (_, ak) = fetch_raw_key(self.ptr, a);
      self.cmp.compare(ak, b)
    }
  }

  #[inline]
  fn compare_in(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering
  where
    C: BytesComparator,
  {
    unsafe {
      let (av, ak) = fetch_raw_key(self.ptr, a);
      let (bv, bk) = fetch_raw_key(self.ptr, b);

      match (av, bv) {
        (Some(av), Some(bv)) => self.cmp.compare(ak, bk).then_with(|| bv.cmp(&av)),
        (None, None) => self.cmp.compare(ak, bk),
        _ => unreachable!("trying to compare versioned and non-versioned keys"),
      }
    }
  }
}

impl<C: ?Sized> Clone for MemtableComparator<C> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      cmp: self.cmp.clone(),
    }
  }
}

impl<C> core::fmt::Debug for MemtableComparator<C>
where
  C: core::fmt::Debug + ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MemtableComparator")
      .field("ptr", &self.ptr)
      .field("cmp", &self.cmp)
      .finish()
  }
}

impl<C> Equivalentor<Query<&[u8]>> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &Query<&[u8]>, b: &Query<&[u8]>) -> bool {
    self.cmp.equivalent(a.0, b.0)
  }
}

impl<C> Comparator<Query<&[u8]>> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare(&self, a: &Query<&[u8]>, b: &Query<&[u8]>) -> cmp::Ordering {
    self.cmp.compare(a.0, b.0)
  }
}

impl<C> Equivalentor<RecordPointer> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<C> TypeRefEquivalentor<RecordPointer> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  #[inline]
  fn equivalent_ref(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }

  #[inline]
  fn equivalent_refs(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<Q, C> TypeRefQueryEquivalentor<RecordPointer, Q> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &Q) -> bool {
    self.equivalent_key(a, b.borrow())
  }
}

impl<C> Comparator<RecordPointer> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<C> TypeRefComparator<RecordPointer> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare_ref(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }

  #[inline]
  fn compare_refs(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<Q, C> TypeRefQueryEquivalentor<RecordPointer, Query<Q>> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &Query<Q>) -> bool {
    self.equivalent_key(a, b.0.borrow())
  }
}

impl<Q, C> TypeRefQueryComparator<RecordPointer, Query<Q>> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &Query<Q>) -> cmp::Ordering {
    self.compare_key(a, b.0.borrow())
  }
}
