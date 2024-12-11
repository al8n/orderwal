/// Memtables which only support unique keys.
pub mod unique;

/// Memtables which support multiple versions.
pub mod multiple_version;

mod comparator;
mod range_comparator;

pub(crate) use comparator::MemtableComparator;
pub(crate) use range_comparator::MemtableRangeComparator;
