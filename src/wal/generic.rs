pub(crate) mod iter;
pub(crate) mod base;
pub(crate) mod mvcc;
pub(crate) mod entry;

mod query;
pub(crate) use query::GenericComparator;
use query::*;

mod pointer;
pub use pointer::*;

use dbutils::traits::{Type, TypeRef};

#[inline]
fn ty_ref<T: ?Sized + Type>(src: &[u8]) -> T::Ref<'_> {
  
  unsafe { <T::Ref<'_> as TypeRef<'_>>::from_slice(src) }
}
