use skl::generic::multiple_version::sync::{Entry, Iter, Range};

range_update_wrapper!(
  /// Range update entry.
  RangeUpdateEntry(Entry)::version
);

iter_wrapper!(
  /// The iterator for point entries.
  IterBulkUpdates(Iter) yield RangeUpdateEntry by MemtableRangeComparator
);

range_wrapper!(
  /// The iterator over a subset of point entries.
  RangeBulkUpdates(Range) yield RangeUpdateEntry by MemtableRangeComparator
);
