/// Memtables which only support unique keys.
pub mod unique;

/// Memtables which support multiple versions.
pub mod multiple_version;

mod comparator;
mod range_comparator;

pub(crate) use comparator::MemtableComparator;
pub(crate) use range_comparator::MemtableRangeComparator;
use skl::generic::{Type, TypeRef};


unsafe fn ty_ref<T: ?Sized + Type>(src: &[u8]) -> T::Ref<'_> {
  <T::Ref<'_> as TypeRef<'_>>::from_slice(src)
}