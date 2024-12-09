use core::{cmp, marker::PhantomData};

use skl::generic::{
  Comparator, Equivalentor, Type, TypeRef, TypeRefComparator, TypeRefEquivalentor,
  TypeRefQueryComparator, TypeRefQueryEquivalentor,
};
use triomphe::Arc;

use crate::types::{fetch_entry, fetch_raw_key, Generic, RawEntryRef, RecordPointer};

use super::super::Query;

pub struct MemtableComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: Arc<C>,
  _k: PhantomData<K>,
}

impl<K, C> crate::types::sealed::ComparatorConstructor<C> for MemtableComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  #[inline]
  fn new(ptr: *const u8, cmp: Arc<C>) -> Self {
    Self {
      ptr,
      cmp,
      _k: PhantomData,
    }
  }
}

impl<K, C> MemtableComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  #[inline]
  pub fn fetch_entry<'a, V>(&self, kp: &RecordPointer) -> RawEntryRef<'a, Generic<K, V>>
  where
    V: Type + ?Sized,
    K: Type,
  {
    unsafe { fetch_entry(self.ptr, kp) }
  }

  #[inline]
  fn query_equivalent_key<'a, Q>(&self, a: &RecordPointer, b: &Q) -> bool
  where
    C: TypeRefQueryEquivalentor<'a, K, Q>,
    K: Type,
    Q: ?Sized,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let ak = <K::Ref<'_> as TypeRef<'_>>::from_slice(ak);
      self.cmp.query_equivalent_ref(&ak, b)
    }
  }

  #[inline]
  fn equivalent_in<'a>(&self, a: &RecordPointer, b: &RecordPointer) -> bool
  where
    C: TypeRefEquivalentor<'a, K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let bk = fetch_raw_key(self.ptr, b);

      ak == bk
    }
  }

  #[inline]
  fn compare_key<'a, Q>(&self, a: &RecordPointer, b: &Q) -> cmp::Ordering
  where
    C: TypeRefQueryComparator<'a, K, Q>,
    K: Type,
    Q: ?Sized,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let ak = <K::Ref<'_> as TypeRef<'_>>::from_slice(ak);
      self.cmp.query_compare_ref(&ak, b)
    }
  }

  #[inline]
  fn compare_in<'a>(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering
  where
    C: TypeRefComparator<'a, K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let ak = <K::Ref<'_> as TypeRef<'_>>::from_slice(ak);
      let bk = fetch_raw_key(self.ptr, b);
      let bk = <K::Ref<'_> as TypeRef<'_>>::from_slice(bk);

      self.cmp.compare_refs(&ak, &bk)
    }
  }
}

impl<K, C> Clone for MemtableComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      cmp: self.cmp.clone(),
      _k: PhantomData,
    }
  }
}

impl<K, C> core::fmt::Debug for MemtableComparator<K, C>
where
  C: core::fmt::Debug + ?Sized,
  K: ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MemtableComparator")
      .field("ptr", &self.ptr)
      .field("cmp", &self.cmp)
      .finish()
  }
}

impl<'a, K, C> Equivalentor<RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefEquivalentor<'a, K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<'a, K, C> TypeRefEquivalentor<'a, RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefEquivalentor<'a, K> + ?Sized,
  K: Type + ?Sized,
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

impl<'a, K, C> Comparator<RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefComparator<'a, K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn compare(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<'a, K, C> TypeRefComparator<'a, RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefComparator<'a, K> + ?Sized,
  K: Type + ?Sized,
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

impl<'a, K, Q, C> TypeRefQueryEquivalentor<'a, RecordPointer, Query<Q>> for MemtableComparator<K, C>
where
  C: TypeRefQueryEquivalentor<'a, K, Q> + ?Sized,
  Q: ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &Query<Q>) -> bool {
    self.query_equivalent_key(a, &b.0)
  }
}

impl<'a, K, Q, C> TypeRefQueryComparator<'a, RecordPointer, Query<Q>> for MemtableComparator<K, C>
where
  C: TypeRefQueryComparator<'a, K, Q> + ?Sized,
  K: Type + ?Sized,
  Q: ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &Query<Q>) -> cmp::Ordering {
    self.compare_key(a, &b.0)
  }
}

impl<'a, K, C> TypeRefQueryEquivalentor<'a, RecordPointer, RecordPointer>
  for MemtableComparator<K, C>
where
  C: TypeRefEquivalentor<'a, K> + ?Sized,
  K: Type + ?Sized,
{
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<'a, K, C> TypeRefQueryComparator<'a, RecordPointer, RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefComparator<'a, K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}
