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
