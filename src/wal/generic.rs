mod iter;
use dbutils::traits::{Type, TypeRef};
pub use iter::{
  GenericIter, GenericKeys, GenericRange, GenericRangeKeys, GenericRangeValues, GenericValues,
};

pub(crate) mod base;

pub(crate) mod mvcc;

mod entry;
pub use entry::*;

mod query;
pub(crate) use query::GenericComparator;
use query::*;

mod pointer;
pub use pointer::*;

#[inline]
fn ty_ref<T: ?Sized + Type>(src: &[u8]) -> T::Ref<'_> {
  unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(src) }
}

#[inline]
fn kv_ref<'a, K, V>((k, v): (&'a [u8], &'a [u8])) -> (K::Ref<'a>, V::Ref<'a>)
where
  K: Type + ?Sized,
  V: Type + ?Sized,
{
  (ty_ref::<K>(k), ty_ref::<V>(v))
}
