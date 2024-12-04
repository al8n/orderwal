use core::{borrow::Borrow, cmp};

use skl::{dynamic::{BytesComparator, BytesEquivalentor}, generic::{Comparator, Equivalentor, Type, TypeRefComparator, TypeRefEquivalentor, TypeRefQueryComparator, TypeRefQueryEquivalentor}};
use triomphe::Arc;

use crate::types::{RawEntryRef, RecordPointer};

use super::{fetch_entry, fetch_raw_key};


pub(super) struct MemtableComparator<C: ?Sized> {
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: Arc<C>,
}

impl<C: ?Sized> MemtableComparator<C> {
  #[inline]
  pub const fn new(ptr: *const u8, cmp: Arc<C>) -> Self {
    Self { ptr, cmp }
  }

  #[inline]
  pub fn fetch_entry<'a>(&self, kp: &RecordPointer) -> RawEntryRef<'a> {
    unsafe {
      fetch_entry(self.ptr, kp)
    }
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

impl<C> Equivalentor for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  type Type = RecordPointer;

  #[inline]
  fn equivalent(&self, a: &Self::Type, b: &Self::Type) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<'a, C> TypeRefEquivalentor<'a> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
{
  #[inline]
  fn equivalent_ref(&self, a: &Self::Type, b: &<Self::Type as Type>::Ref<'a>) -> bool {
    self.equivalent_in(a, b)
  }

  #[inline]
  fn equivalent_refs(
    &self,
    a: &<Self::Type as Type>::Ref<'a>,
    b: &<Self::Type as Type>::Ref<'a>,
  ) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<'a, Q, C> TypeRefQueryEquivalentor<'a, Q> for MemtableComparator<C>
where
  C: BytesEquivalentor + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &<Self::Type as Type>::Ref<'a>, b: &Q) -> bool {
    self.equivalent_key(a, b.borrow())
  }
}


impl<C> Comparator for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare(&self, a: &Self::Type, b: &Self::Type) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<'a, C> TypeRefComparator<'a> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn compare_ref(&self, a: &Self::Type, b: &<Self::Type as Type>::Ref<'a>) -> cmp::Ordering {
    self.compare_in(a, b)
  }

  #[inline]
  fn compare_refs(
    &self,
    a: &<Self::Type as Type>::Ref<'a>,
    b: &<Self::Type as Type>::Ref<'a>,
  ) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<'a, Q, C> TypeRefQueryComparator<'a, Q> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
  Q: ?Sized + Borrow<[u8]>,
{
  #[inline]
  fn query_compare_ref(&self, a: &<Self::Type as Type>::Ref<'a>, b: &Q) -> cmp::Ordering {
    self.compare_key(a, b.borrow())
  }
}

impl<'a, C> TypeRefQueryEquivalentor<'a, RecordPointer> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  fn query_equivalent_ref(&self, a: &<Self::Type as Type>::Ref<'a>, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<'a, C> TypeRefQueryComparator<'a, RecordPointer> for MemtableComparator<C>
where
  C: BytesComparator + ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &<Self::Type as Type>::Ref<'a>, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}