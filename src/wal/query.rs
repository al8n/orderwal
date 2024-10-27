use core::{
  cmp,
  marker::PhantomData,
  ops::{Bound, RangeBounds},
};

use dbutils::{
  equivalent::{Comparable, Equivalent},
  traits::{KeyRef, Type, TypeRef},
};
use ref_cast::RefCast;

use super::KeyPointer;

#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct Slice<K: ?Sized> {
  _k: PhantomData<K>,
  data: [u8],
}

impl<K> Equivalent<KeyPointer<K>> for Slice<K>
where
  K: Type + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn equivalent(&self, key: &KeyPointer<K>) -> bool {
    self.data.eq(key.as_slice())
  }
}

impl<K> Comparable<KeyPointer<K>> for Slice<K>
where
  K: Type + ?Sized,
  for<'a> K::Ref<'a>: KeyRef<'a, K>,
{
  fn compare(&self, p: &KeyPointer<K>) -> cmp::Ordering {
    unsafe {
      let kr: K::Ref<'_> = TypeRef::from_slice(p.as_slice());
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
    self.r.start_bound().map(RefCast::ref_cast)
  }

  fn end_bound(&self) -> Bound<&Query<'a, K, Q>> {
    self.r.end_bound().map(RefCast::ref_cast)
  }
}

#[derive(ref_cast::RefCast)]
#[repr(transparent)]
pub struct Query<'a, K, Q>
where
  K: ?Sized,
  Q: ?Sized,
{
  _k: PhantomData<&'a K>,
  key: Q,
}

impl<'a, K, Q> Equivalent<KeyPointer<K>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  Q: ?Sized + Equivalent<K::Ref<'a>>,
{
  #[inline]
  fn equivalent(&self, p: &KeyPointer<K>) -> bool {
    let kr = unsafe { <K::Ref<'_> as TypeRef<'_>>::from_slice(p.as_slice()) };
    Equivalent::equivalent(&self.key, &kr)
  }
}

impl<'a, K, Q> Comparable<KeyPointer<K>> for Query<'a, K, Q>
where
  K: Type + ?Sized,
  Q: ?Sized + Comparable<K::Ref<'a>>,
{
  #[inline]
  fn compare(&self, p: &KeyPointer<K>) -> cmp::Ordering {
    let kr = unsafe { <K::Ref<'_> as TypeRef<'_>>::from_slice(p.as_slice()) };
    Comparable::compare(&self.key, &kr)
  }
}
