use core::{cmp, marker::PhantomData, ops::Bound};

use skl::generic::{
  Comparator, Equivalentor, LazyRef, Type, TypeRefComparator, TypeRefEquivalentor,
  TypeRefQueryComparator, TypeRefQueryEquivalentor,
};
use triomphe::Arc;

use crate::types::{
  fetch_raw_range_deletion_entry, fetch_raw_range_key_start_bound, fetch_raw_range_update_entry,
  sealed::Pointee, Query, RawRangeDeletionRef, RawRangeUpdateRef, RecordPointer, RefQuery,
};

pub struct MemtableRangeComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: Arc<C>,
  _k: PhantomData<K>,
}

impl<K, C> crate::types::sealed::ComparatorConstructor<C> for MemtableRangeComparator<K, C>
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

impl<K: ?Sized, C: ?Sized> crate::types::sealed::RangeComparator<C>
  for MemtableRangeComparator<K, C>
{
  fn fetch_range_update<'a, T>(&self, kp: &RecordPointer) -> RawRangeUpdateRef<'a, T>
  where
    T: crate::types::TypeMode,
    T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
    T::Value<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
  {
    unsafe { fetch_raw_range_update_entry::<T>(self.ptr, kp) }
  }

  fn fetch_range_deletion<'a, T>(&self, kp: &RecordPointer) -> RawRangeDeletionRef<'a, T>
  where
    T: crate::types::TypeMode,
    T::Key<'a>: crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
  {
    unsafe { fetch_raw_range_deletion_entry::<T>(self.ptr, kp) }
  }
}

impl<K, C> MemtableRangeComparator<K, C>
where
  K: ?Sized,
  C: ?Sized,
{
  #[inline]
  fn equivalent_start_key<Q>(&self, a: &RecordPointer, b: &Q) -> bool
  where
    C: TypeRefQueryEquivalentor<K, Q>,
    K: Type,
    Q: ?Sized,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, a).map(|k| k.output());
      match &ak {
        Bound::Included(k) => self.cmp.query_equivalent_ref(k, b),
        Bound::Excluded(k) => self.cmp.query_equivalent_ref(k, b),
        Bound::Unbounded => false,
      }
    }
  }

  #[inline]
  fn equivalent_start_key_with_ref(&self, a: &RecordPointer, b: &K::Ref<'_>) -> bool
  where
    C: TypeRefEquivalentor<K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, a).map(|k| k.output());
      match &ak {
        Bound::Included(k) => self.cmp.equivalent_refs(k, b),
        Bound::Excluded(k) => self.cmp.equivalent_refs(k, b),
        Bound::Unbounded => false,
      }
    }
  }

  #[inline]
  fn equivalent_in(&self, a: &RecordPointer, b: &RecordPointer) -> bool
  where
    C: TypeRefEquivalentor<K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, a).map(|k| k.input());
      let bk = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, b).map(|k| k.input());

      match (ak, bk) {
        (Bound::Unbounded, Bound::Unbounded) => true,
        (Bound::Included(_), Bound::Unbounded) => false,
        (Bound::Excluded(_), Bound::Unbounded) => false,
        (Bound::Unbounded, Bound::Included(_)) => false,
        (Bound::Unbounded, Bound::Excluded(_)) => false,

        (Bound::Included(a), Bound::Included(b)) => a == b,
        (Bound::Included(a), Bound::Excluded(b)) => a == b,
        (Bound::Excluded(a), Bound::Included(b)) => a == b,
        (Bound::Excluded(a), Bound::Excluded(b)) => a == b,
      }
    }
  }

  #[inline]
  fn compare_start_key<Q>(&self, a: &RecordPointer, b: &Q) -> cmp::Ordering
  where
    C: TypeRefQueryComparator<K, Q>,
    K: Type,
    Q: ?Sized,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, a).map(|k| k.output());
      match &ak {
        Bound::Included(k) => self.cmp.query_compare_ref(k, b),
        Bound::Excluded(k) => self
          .cmp
          .query_compare_ref(k, b)
          .then(cmp::Ordering::Greater),
        Bound::Unbounded => cmp::Ordering::Less,
      }
    }
  }

  #[inline]
  fn compare_start_key_with_ref(&self, a: &RecordPointer, b: &K::Ref<'_>) -> cmp::Ordering
  where
    C: TypeRefComparator<K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, a).map(|k| k.output());
      match &ak {
        Bound::Included(k) => self.cmp.compare_refs(k, b),
        Bound::Excluded(k) => self.cmp.compare_refs(k, b).then(cmp::Ordering::Greater),
        Bound::Unbounded => cmp::Ordering::Less,
      }
    }
  }

  #[inline]
  fn compare_in(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering
  where
    C: TypeRefComparator<K>,
    K: Type,
  {
    unsafe {
      let ak = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, a).map(|k| k.output());
      let bk = fetch_raw_range_key_start_bound::<LazyRef<'_, K>>(self.ptr, b).map(|k| k.output());

      match (&ak, &bk) {
        (Bound::Included(_), Bound::Unbounded) => cmp::Ordering::Greater,
        (Bound::Excluded(_), Bound::Unbounded) => cmp::Ordering::Greater,
        (Bound::Unbounded, Bound::Included(_)) => cmp::Ordering::Less,
        (Bound::Unbounded, Bound::Excluded(_)) => cmp::Ordering::Less,
        (Bound::Unbounded, Bound::Unbounded) => cmp::Ordering::Equal,

        (Bound::Included(a), Bound::Included(b)) => self.cmp.compare_refs(a, b),
        (Bound::Included(a), Bound::Excluded(b)) => self.cmp.compare_refs(a, b),
        (Bound::Excluded(a), Bound::Included(b)) => self.cmp.compare_refs(a, b),
        (Bound::Excluded(a), Bound::Excluded(b)) => self.cmp.compare_refs(a, b),
      }
    }
  }
}

impl<K, C> Clone for MemtableRangeComparator<K, C>
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

impl<K, C> core::fmt::Debug for MemtableRangeComparator<K, C>
where
  C: core::fmt::Debug + ?Sized,
  K: ?Sized,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MemtableRangeComparator")
      .field("ptr", &self.ptr)
      .field("cmp", &self.cmp)
      .finish()
  }
}

impl<K, C> Equivalentor<RecordPointer> for MemtableRangeComparator<K, C>
where
  C: TypeRefEquivalentor<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn equivalent(&self, a: &RecordPointer, b: &RecordPointer) -> bool {
    self.equivalent_in(a, b)
  }
}

impl<K, C> TypeRefEquivalentor<RecordPointer> for MemtableRangeComparator<K, C>
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

impl<K, C> Comparator<RecordPointer> for MemtableRangeComparator<K, C>
where
  C: TypeRefComparator<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn compare(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<K, C> TypeRefComparator<RecordPointer> for MemtableRangeComparator<K, C>
where
  C: TypeRefComparator<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn compare_ref(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }

  fn compare_refs(&self, a: &RecordPointer, b: &RecordPointer) -> cmp::Ordering {
    self.compare_in(a, b)
  }
}

impl<K, Q, C> TypeRefQueryEquivalentor<RecordPointer, Query<Q>> for MemtableRangeComparator<K, C>
where
  C: TypeRefQueryEquivalentor<K, Q> + ?Sized,
  Q: ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &Query<Q>) -> bool {
    self.equivalent_start_key(a, &b.0)
  }
}

impl<K, Q, C> TypeRefQueryComparator<RecordPointer, Query<Q>> for MemtableRangeComparator<K, C>
where
  C: TypeRefQueryComparator<K, Q> + ?Sized,
  Q: ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_compare_ref(&self, a: &RecordPointer, b: &Query<Q>) -> cmp::Ordering {
    self.compare_start_key(a, &b.0)
  }
}

impl<'a, K, C> TypeRefQueryEquivalentor<RecordPointer, RefQuery<K::Ref<'a>>>
  for MemtableRangeComparator<K, C>
where
  C: TypeRefEquivalentor<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_equivalent_ref(&self, a: &RecordPointer, b: &RefQuery<K::Ref<'a>>) -> bool {
    self.equivalent_start_key_with_ref(a, &b.query)
  }
}

impl<'a, K, C> TypeRefQueryComparator<RecordPointer, RefQuery<K::Ref<'a>>>
  for MemtableRangeComparator<K, C>
where
  C: TypeRefComparator<K> + ?Sized,
  K: Type + ?Sized,
{
  #[inline]
  fn query_compare_ref(
    &self,
    a: &<RecordPointer as Type>::Ref<'_>,
    b: &RefQuery<K::Ref<'a>>,
  ) -> cmp::Ordering {
    self.compare_start_key_with_ref(a, &b.query)
  }
}
