use core::{borrow::Borrow, cmp};

use skl::{
  dynamic::{BytesComparator, BytesEquivalentor},
  generic::{
    Comparator, Equivalentor, TypeRefComparator, TypeRefEquivalentor, TypeRefQueryComparator,
    TypeRefQueryEquivalentor,
  },
};
use triomphe::Arc;

use crate::types::{fetch_entry, fetch_raw_key, Kind, RawEntryRef, RecordPointer};

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
    T: Kind,
    T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
    T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
  {
    unsafe { fetch_entry::<T>(self.ptr, kp) }
  }
}

impl<C: ?Sized> MemtableComparator<C> {
  #[inline]
  pub fn fetch_entry<'a, T>(&self, kp: &RecordPointer) -> RawEntryRef<'a, T>
  where
    T: Kind,
    T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
    T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
  {
    unsafe { fetch_entry::<T>(self.ptr, kp) }
  }

  #[inline]
  fn equivalent_key(&self, a: &RecordPointer, b: &[u8]) -> bool
  where
    C: BytesEquivalentor,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      self.cmp.equivalent(ak, b)
    }
  }

  #[inline]
  fn equivalent_in(&self, a: &RecordPointer, b: &RecordPointer) -> bool
  where
    C: BytesEquivalentor,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let bk = fetch_raw_key(self.ptr, b);

      self.cmp.equivalent(ak, bk)
    }
  }

  #[inline]
  fn compare_key(&self, a: &RecordPointer, b: &[u8]) -> cmp::Ordering
  where
    C: BytesComparator,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      self.cmp.compare(ak, b)
    }
  }

  #[inline]
  fn compare_in(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering
  where
    C: BytesComparator,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let bk = fetch_raw_key(self.ptr, b);

      self.cmp.compare(ak, bk)
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

impl<C> Equivalentor<RecordPointer> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<C> TypeRefEquivalentor<'_, RecordPointer> for MemtableComparator<C>
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

impl<Q, C> TypeRefQueryEquivalentor<'_, RecordPointer, Q> for MemtableComparator<C>
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

impl<C> TypeRefComparator<'_, RecordPointer> for MemtableComparator<C>
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

impl<Q, C> TypeRefQueryComparator<'_, RecordPointer, Q> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &Q) -> cmp::Ordering {
    self.compare_key(a, b.borrow())
  }
}

impl<C> TypeRefQueryEquivalentor<'_, RecordPointer, RecordPointer> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<C> TypeRefQueryComparator<'_, RecordPointer, RecordPointer> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}
