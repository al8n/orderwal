use core::{cmp, marker::PhantomData};

use skl::generic::{
  Comparator, Equivalentor, Type, TypeRef, TypeRefComparator, TypeRefEquivalentor,
  TypeRefQueryComparator, TypeRefQueryEquivalentor,
};
use triomphe::Arc;

use crate::types::{
  fetch_entry, fetch_raw_key, Query, RawEntryRef, RecordPointer, TypeMode,
};

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

impl<K: ?Sized, C: ?Sized> crate::types::sealed::PointComparator<C> for MemtableComparator<K, C> {
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

impl<K, C> MemtableComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  #[inline]
  fn query_equivalent_key<Q>(&self, a: &RecordPointer, b: &Q) -> bool
  where
    C: TypeRefQueryEquivalentor<K, Q>,
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
  fn equivalent_in(&self, a: &RecordPointer, b: &RecordPointer) -> bool
  where
    C: TypeRefEquivalentor<K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_key(self.ptr, a);
      let bk = fetch_raw_key(self.ptr, b);

      ak == bk
    }
  }

  #[inline]
  fn compare_key<Q>(&self, a: &RecordPointer, b: &Q) -> cmp::Ordering
  where
    C: TypeRefQueryComparator<K, Q>,
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
  fn compare_in(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering
  where
    C: TypeRefComparator<K>,
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

impl<K, C> Equivalentor<RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefEquivalentor<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<K, C> TypeRefEquivalentor<RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefEquivalentor<K> + ?Sized,
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

impl<K, C> Comparator<RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefComparator<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn compare(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<'a, K, C> Equivalentor<Query<K::Ref<'a>>> for MemtableComparator<K, C>
where
  C: TypeRefEquivalentor<K> + ?Sized,
  K: Type + ?Sized,
{
  fn equivalent(&self, a: &Query<K::Ref<'a>>, b: &Query<K::Ref<'a>>) -> bool {
    self.cmp.equivalent_refs(&a.0, &b.0)
  }
}

impl<'a, K, C> Comparator<Query<K::Ref<'a>>> for MemtableComparator<K, C>
where
  C: TypeRefComparator<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn compare(&self, a: &Query<K::Ref<'a>>, b: &Query<K::Ref<'a>>) -> cmp::Ordering {
    self.cmp.compare_refs(&a.0, &b.0)
  }
}

impl<K, C> TypeRefComparator<RecordPointer> for MemtableComparator<K, C>
where
  C: TypeRefComparator<K> + ?Sized,
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

impl<K, Q, C> TypeRefQueryEquivalentor<RecordPointer, Query<Q>> for MemtableComparator<K, C>
where
  C: TypeRefQueryEquivalentor<K, Q> + ?Sized,
  Q: ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &Query<Q>) -> bool {
    self.query_equivalent_key(a, &b.0)
  }
}

impl<K, Q, C> TypeRefQueryComparator<RecordPointer, Query<Q>> for MemtableComparator<K, C>
where
  C: TypeRefQueryComparator<K, Q> + ?Sized,
  Q: ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &Query<Q>) -> cmp::Ordering {
    self.compare_key(a, &b.0)
  }
}
