mod comparator;
mod range_comparator;

pub(crate) use comparator::MemtableComparator;
pub(crate) use range_comparator::MemtableRangeComparator;

#[derive(ref_cast::RefCast)]
#[repr(transparent)]
struct Query<Q: ?Sized>(Q);
