use core::{borrow::Borrow, cmp, ops::Bound};

use skl::{
  dynamic::{BytesComparator, BytesEquivalentor},
  generic::{
    Comparator, Equivalentor, TypeRefComparator, TypeRefEquivalentor, TypeRefQueryComparator,
    TypeRefQueryEquivalentor,
  },
};
use triomphe::Arc;

use crate::types::{
  fetch_raw_range_deletion_entry, fetch_raw_range_key_start_bound, fetch_raw_range_update_entry, Dynamic, RawRangeDeletionRef, RawRangeUpdateRef, RecordPointer
};

pub struct MemtableRangeComparator<C: ?Sized> {
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: Arc<C>,
}

impl<C: ?Sized> super::super::sealed::ComparatorConstructor<C> for MemtableRangeComparator<C> {
  #[inline]
  fn new(ptr: *const u8, cmp: Arc<C>) -> Self {
    Self { ptr, cmp }
  }
}

impl<C: ?Sized> crate::types::sealed::ComparatorConstructor<C> for MemtableRangeComparator<C> {
  #[inline]
  fn new(ptr: *const u8, cmp: Arc<C>) -> Self {
    Self { ptr, cmp }
  }
}

impl<C: ?Sized> crate::types::sealed::RangeComparator<C> for MemtableRangeComparator<C> {
  fn fetch_range_update<'a, T>(&self, kp: &RecordPointer) -> RawRangeUpdateRef<'a, T>
  where
    T: crate::types::Kind,
    T::Key<'a>: crate::types::sealed::Pointee<Input = &'a [u8]>,
    T::Value<'a>: crate::types::sealed::Pointee<Input = &'a [u8]> {
    unsafe { fetch_raw_range_update_entry::<T>(self.ptr, kp) }
  }

  fn fetch_range_deletion<'a, T>(&self, kp: &RecordPointer) -> RawRangeDeletionRef<'a, T>
  where
    T: crate::types::Kind,
    T::Key<'a>: crate::types::sealed::Pointee<Input = &'a [u8]> {
    unsafe { fetch_raw_range_deletion_entry::<T>(self.ptr, kp) }
  }
}

impl<C: ?Sized> MemtableRangeComparator<C> {
  #[inline]
  pub fn fetch_range_update<'a>(&self, kp: &RecordPointer) -> RawRangeUpdateRef<'a, Dynamic>
  {
    unsafe { fetch_raw_range_update_entry::<Dynamic>(self.ptr, kp) }
  }

  #[inline]
  pub fn fetch_range_deletion<'a>(&self, kp: &RecordPointer) -> RawRangeDeletionRef<'a, Dynamic>
  {
    unsafe { fetch_raw_range_deletion_entry::<Dynamic>(self.ptr, kp) }
  }

  #[inline]
  fn equivalent_start_key<'a>(&self, a: &RecordPointer, b: &[u8]) -> bool
  where
    C: BytesEquivalentor,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<&[u8]>(self.ptr, a);
      match ak {
        Bound::Included(k) => self.cmp.equivalent(k, b),
        Bound::Excluded(k) => self.cmp.equivalent(k, b),
        Bound::Unbounded => false,
      }
    }
  }

  #[inline]
  fn equivalent_in<'a>(&self, a: &RecordPointer, b: &RecordPointer) -> bool
  where
    C: BytesEquivalentor,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<&[u8]>(self.ptr, a);
      let bk = fetch_raw_range_key_start_bound::<&[u8]>(self.ptr, b);

      match (ak, bk) {
        (Bound::Unbounded, Bound::Unbounded) => true,
        (Bound::Included(_), Bound::Unbounded) => false,
        (Bound::Excluded(_), Bound::Unbounded) => false,
        (Bound::Unbounded, Bound::Included(_)) => false,
        (Bound::Unbounded, Bound::Excluded(_)) => false,

        (Bound::Included(a), Bound::Included(b)) => self.cmp.equivalent(a, b),
        (Bound::Included(a), Bound::Excluded(b)) => self.cmp.equivalent(a, b),
        (Bound::Excluded(a), Bound::Included(b)) => self.cmp.equivalent(a, b),
        (Bound::Excluded(a), Bound::Excluded(b)) => self.cmp.equivalent(a, b),
      }
    }
  }

  #[inline]
  fn compare_start_key(&self, a: &RecordPointer, b: &[u8]) -> cmp::Ordering
  where
    C: BytesComparator,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<&[u8]>(self.ptr, a);
      match ak {
        Bound::Included(k) => self.cmp.compare(k, b),
        Bound::Excluded(k) => self.cmp.compare(k, b).then(cmp::Ordering::Greater),
        Bound::Unbounded => cmp::Ordering::Less,
      }
    }
  }

  #[inline]
  fn compare_in(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering
  where
    C: BytesComparator,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<&[u8]>(self.ptr, a);
      let bk = fetch_raw_range_key_start_bound::<&[u8]>(self.ptr, b);

      match (ak, bk) {
        (Bound::Included(_), Bound::Unbounded) => cmp::Ordering::Greater,
        (Bound::Excluded(_), Bound::Unbounded) => cmp::Ordering::Greater,
        (Bound::Unbounded, Bound::Included(_)) => cmp::Ordering::Less,
        (Bound::Unbounded, Bound::Excluded(_)) => cmp::Ordering::Less,
        (Bound::Unbounded, Bound::Unbounded) => cmp::Ordering::Equal,

        (Bound::Included(a), Bound::Included(b)) => self.cmp.compare(a, b),
        (Bound::Included(a), Bound::Excluded(b)) => self.cmp.compare(a, b),
        (Bound::Excluded(a), Bound::Included(b)) => self.cmp.compare(a, b),
        (Bound::Excluded(a), Bound::Excluded(b)) => self.cmp.compare(a, b),
      }
    }
  }
}

impl<C: ?Sized> Clone for MemtableRangeComparator<C> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      cmp: self.cmp.clone(),
    }
  }
}

impl<C> core::fmt::Debug for MemtableRangeComparator<C>
where
  C: core::fmt::Debug + ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MemtableRangeComparator")
      .field("ptr", &self.ptr)
      .field("cmp", &self.cmp)
      .finish()
  }
}

impl<C> Equivalentor<RecordPointer> for MemtableRangeComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<C> TypeRefEquivalentor<'_, RecordPointer> for MemtableRangeComparator<C>
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

impl<Q, C> TypeRefQueryEquivalentor<'_, RecordPointer, Q> for MemtableRangeComparator<C>
where
  C: BytesEquivalentor + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &Q) -> bool {
    self.equivalent_start_key(a, b.borrow())
  }
}

impl<C> Comparator<RecordPointer> for MemtableRangeComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<C> TypeRefComparator<'_, RecordPointer> for MemtableRangeComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare_ref(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }

  fn compare_refs(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<Q, C> TypeRefQueryComparator<'_, RecordPointer, Q> for MemtableRangeComparator<C>
where
  C: BytesComparator + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &Q) -> cmp::Ordering {
    self.compare_start_key(a, b.borrow())
  }
}

impl<C> TypeRefQueryEquivalentor<'_, RecordPointer, RecordPointer> for MemtableRangeComparator<C>
where
  C: BytesComparator + ?Sized,
{
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<C> TypeRefQueryComparator<'_, RecordPointer, RecordPointer> for MemtableRangeComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}