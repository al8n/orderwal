use core::{
  cmp,
  marker::PhantomData,
  ops::{Bound, RangeBounds},
};

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
};

use crate::sealed::Pointer as _;

use super::{GenericPointer, GenericVersionPointer};

#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct Slice<K: ?Sized> {
  _k: PhantomData<K>,
  data: [u8],
}

impl<K, V> Equivalent<GenericPointer<K, V>> for Slice<K>
where
  K: Type + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn equivalent(&self, key: &GenericPointer<K, V>) -> bool {
    self.data.eq(key.as_key_slice())
  }
}

impl<K, V> Comparable<GenericPointer<K, V>> for Slice<K>
where
  K: Type + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
  V: ?Sized,
{
  fn compare(&self, p: &GenericPointer<K, V>) -> cmp::Ordering {
    unsafe {
      let kr: K::Ref<'_> = TypeRef::from_slice(p.as_key_slice());
      let or: K::Ref<'_> = TypeRef::from_slice(&self.data);
      KeyRef::compare(&kr, &or).reverse()
    }
  }
}

pub struct GenericQueryRange<'a, K: ?Sized, Q: ?Sized, R>
where
  R: RangeBounds<Q>,
{
  r: R,
  _q: PhantomData<(&'a Q, &'a K)>,
}

impl<K: ?Sized, Q: ?Sized, R> GenericQueryRange<'_, K, Q, R>
where
  R: RangeBounds<Q>,
{
  #[inline]
  pub(super) const fn new(r: R) -> Self {
    Self { r, _q: PhantomData }
  }
}

impl<'a, K: ?Sized, Q: ?Sized, R> RangeBounds<Query<'a, K, Q>> for GenericQueryRange<'a, K, Q, R>
where
  R: RangeBounds<Q>,
{
  #[inline]
  fn start_bound(&self) -> Bound<&Query<'a, K, Q>> {
    self.r.start_bound().map(Query::ref_cast)
  }

  fn end_bound(&self) -> Bound<&Query<'a, K, Q>> {
    self.r.end_bound().map(Query::ref_cast)
  }
}

#[repr(transparent)]
pub struct Query<'a, K, Q>
where
  K: ?Sized,
  Q: ?Sized,
{
  key: &'a Q,
  _k: PhantomData<K>,
}

impl<'a, K, Q> Query<'a, K, Q>
where
  K: ?Sized,
  Q: ?Sized,
{
  #[inline]
  pub(super) const fn new(key: &'a Q) -> Self {
    Self {
      key,
      _k: PhantomData,
    }
  }

  #[inline]
  pub(super) fn ref_cast(from: &Q) -> &Self {
    if false {
      ::ref_cast::__private::assert_trivial::<PhantomData<K>>();
    }
    #[cfg(debug_assertions)]
    {
      #[allow(unused_imports)]
      use ::ref_cast::__private::LayoutUnsized;
      ::ref_cast::__private::assert_layout::<Self, Q>(
        "Query",
        ::ref_cast::__private::Layout::<Self>::SIZE,
        ::ref_cast::__private::Layout::<Q>::SIZE,
        ::ref_cast::__private::Layout::<Self>::ALIGN,
        ::ref_cast::__private::Layout::<Q>::ALIGN,
      );
    }
    // We can do this because of Query is transparent.
    unsafe { &*(from as *const Q as *const Self) }
  }
}

impl<'a, 'b: 'a, K, Q, V> Equivalent<GenericPointer<K, V>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  V: ?Sized,
  Q: ?Sized + Equivalent<K::Ref<'b>>,
{
  #[inline]
  fn equivalent(&self, p: &GenericPointer<K, V>) -> bool {
    let kr = unsafe { <K::Ref<'b> as TypeRef<'b>>::from_slice(p.as_key_slice()) };
    Equivalent::equivalent(self.key, &kr)
  }
}

impl<'a, 'b: 'a, K, Q, V> Comparable<GenericPointer<K, V>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'b>>,
{
  #[inline]
  fn compare(&self, p: &GenericPointer<K, V>) -> cmp::Ordering {
    let kr = unsafe { <K::Ref<'b> as TypeRef<'b>>::from_slice(p.as_key_slice()) };
    Comparable::compare(self.key, &kr)
  }
}

impl<'a, 'b: 'a, K, Q, V> Equivalent<GenericVersionPointer<K, V>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  V: ?Sized,
  Q: ?Sized + Equivalent<K::Ref<'b>>,
{
  #[inline]
  fn equivalent(&self, p: &GenericVersionPointer<K, V>) -> bool {
    let kr = unsafe { <K::Ref<'b> as TypeRef<'b>>::from_slice(p.as_key_slice()) };
    Equivalent::equivalent(self.key, &kr)
  }
}

impl<'a, 'b: 'a, K, Q, V> Comparable<GenericVersionPointer<K, V>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  V: ?Sized,
  Q: ?Sized + Comparable<K::Ref<'b>>,
{
  #[inline]
  fn compare(&self, p: &GenericVersionPointer<K, V>) -> cmp::Ordering {
    let kr = unsafe { <K::Ref<'b> as TypeRef<'b>>::from_slice(p.as_key_slice()) };
    Comparable::compare(self.key, &kr)
  }
}
