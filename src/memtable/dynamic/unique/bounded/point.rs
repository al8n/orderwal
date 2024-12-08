use skl::generic::unique::sync::{Entry, Iter, Range};

point_entry_wrapper!(
  /// Point entry.
  PointEntry(Entry)
);

iter_wrapper!(
  /// The iterator for point entries.
  IterPoints(Iter) yield PointEntry by MemtableComparator
);

range_wrapper!(
  /// The iterator over a subset of point entries.
  RangePoints(Range) yield PointEntry by MemtableComparator
);
