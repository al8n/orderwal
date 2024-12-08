use core::marker::PhantomData;

use triomphe::Arc;

pub struct MemtableComparator<K: ?Sized, C: ?Sized> {
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: Arc<C>,
  _k: PhantomData<K>,
}

impl<K: ?Sized, C: ?Sized> super::super::sealed::ComparatorConstructor<C>
  for MemtableComparator<K, C>
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

// impl<K: ?Sized, C: ?Sized> MemtableComparator<K, C>{
//   #[inline]
//   pub fn fetch_entry<'a>(&self, kp: &RecordPointer) -> RawEntryRef<'a> {
//     unsafe { fetch_entry(self.ptr, kp) }
//   }

//   #[inline]
//   fn equivalent_key<'a>(&self, a: &'a RecordPointer, b: K::Ref<'a>) -> bool
//   where
//     C: TypeRefEquivalentor<'a, K>,
//     K: Type,
//   {
//     unsafe {
//       let ak = fetch_raw_key(self.ptr, a);
//       let ak = <K as TypeRef<'_>>::from_slice(ak);
//       self.cmp.equivalent(&ak, b)
//     }
//   }

//   #[inline]
//   fn equivalent_in<'a>(&self, a: &'a RecordPointer, b: &'a RecordPointer) -> bool
//   where
//     C: TypeRefEquivalentor<'a, K>,
//     K: Type,
//   {
//     unsafe {
//       let ak = fetch_raw_key(self.ptr, a);
//       let ak = <K as TypeRef<'_>>::from_slice(ak);
//       let bk = fetch_raw_key(self.ptr, b);
//       let bk = <K as TypeRef<'_>>::from_slice(bk);

//       self.cmp.equivalent(&ak, &bk)
//     }
//   }

//   #[inline]
//   fn compare_key<'a>(&self, a: &'a RecordPointer, b: K::Ref<'a>) -> cmp::Ordering
//   where
//     C: TypeRefComparator<'a, K>,
//     K: Type,
//   {
//     unsafe {
//       let ak = fetch_raw_key(self.ptr, a);
//       let ak = <K as TypeRef<'_>>::from_slice(ak);
//       self.cmp.compare(&ak, b)
//     }
//   }

//   #[inline]
//   fn compare_in<'a>(&self, a: &'a RecordPointer, b: &'a RecordPointer) -> cmp::Ordering
//   where
//     C: TypeRefComparator<'a, K>,
//     K: Type,
//   {
//     unsafe {
//       let ak = fetch_raw_key(self.ptr, a);
//       let ak = <K as TypeRef<'_>>::from_slice(ak);
//       let bk = fetch_raw_key(self.ptr, b);
//       let bk = <K as TypeRef<'_>>::from_slice(bk);

//       self.cmp.compare(&ak, &bk)
//     }
//   }
// }

impl<K: ?Sized, C: ?Sized> Clone for MemtableComparator<K, C> {
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

// impl<K, C> Equivalentor<K> for MemtableComparator<K, C>
// where
//   C: Equivalentor<K> + ?Sized,
//   K: ?Sized,
// {
//   #[inline]
//   fn equivalent(&self, a: &K, b: &K) -> bool {
//     self.equivalent_in(a, b)
//   }
// }

// impl<'a, K, C> TypeRefEquivalentor<'a, K> for MemtableComparator<K, C>
// where
//   C: TypeRefEquivalentor<'a, K> + ?Sized,
//   K: Type + ?Sized,
// {
//   #[inline]
//   fn equivalent_ref(&self, a: &K, b: &K::Ref<'a>) -> bool {
//     self.equivalent_in(a, b)
//   }

//   #[inline]
//   fn equivalent_refs(
//     &self,
//     a: &K::Ref<'a>,
//     b: &K::Ref<'a>,
//   ) -> bool {
//     self.equivalent_in(a, b)
//   }
// }

// impl<'a, K, Q, C> TypeRefQueryEquivalentor<'a, K, Q> for MemtableComparator<K, C>
// where
//   C: TypeRefEquivalentor<'a, K> + ?Sized,
//   Q: ?Sized,
//   K: Type + ?Sized,
// {
//   #[inline]
//   fn query_equivalent_ref(&self, a: &K::Ref<'a>, b: &Q) -> bool {
//     self.equivalent_key(a, b.borrow())
//   }
// }

// impl<K, C> Comparator<K> for MemtableComparator<K, C>
// where
//   C: Comparator<K> + ?Sized,
//   K: ?Sized,
// {
//   #[inline]
//   fn compare(&self, a: &K, b: &K) -> cmp::Ordering {
//     self.compare_in(a, b)
//   }
// }

// impl<'a, K, C> TypeRefComparator<'a, K> for MemtableComparator<K, C>
// where
//   C: TypeRefComparator<'a, K> + ?Sized,
//   K: Type + ?Sized,
// {
//   #[inline]
//   fn compare_ref(&self, a: &K, b: &K::Ref<'a>) -> cmp::Ordering {
//     self.compare_in(a, b)
//   }

//   #[inline]
//   fn compare_refs(
//     &self,
//     a: &K::Ref<'a>,
//     b: &K::Ref<'a>,
//   ) -> cmp::Ordering {
//     self.compare_in(a, b)
//   }
// }

// impl<'a, K, Q, C> TypeRefQueryComparator<'a, K, Q> for MemtableComparator<K, C>
// where
//   C: TypeRefQueryComparator<'a, K, Q> + ?Sized,
//   Q: ?Sized + Borrow<[u8]>,
//   K: Type + ?Sized,
// {
//   #[inline]
//   fn query_compare_ref(&self, a: &K::Ref<'a>, b: &Q) -> cmp::Ordering {
//     self.compare_key(a, b.borrow())
//   }
// }

// impl<'a, K, C> TypeRefQueryEquivalentor<'a, RecordPointer, RecordPointer> for MemtableComparator<K, C>
// where
//   C: TypeRefQueryEquivalentor<'a, RecordPointer, RecordPointer> + ?Sized,
//   K: Type + ?Sized,
// {
//   fn query_equivalent_ref(&self, a: &K::Ref<'a>, b: &RecordPointer) -> bool {
//     self.equivalent_in(a, b)
//   }
// }

// impl<'a, K, C> TypeRefQueryComparator<'a, RecordPointer, RecordPointer> for MemtableComparator<K, C>
// where
//   C: BytesComparator + ?Sized,
// {
//   #[inline]
//   fn query_compare_ref(
//     &self,
//     a: &K::Ref<'a>,
//     b: &RecordPointer,
//   ) -> cmp::Ordering {
//     self.compare_in(a, b)
//   }
// }
