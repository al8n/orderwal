use skl::generic::multiple_version::sync::{Entry, Iter, Range};

range_deletion_wrapper!(
  /// Range deletion entry.
  RangeDeletionEntry(Entry)::version
);

iter_wrapper!(
  /// The iterator for point entries.
  IterBulkDeletions(Iter) yield RangeDeletionEntry by RangeComparator
);

range_wrapper!(
  /// The iterator over a subset of point entries.
  RangeBulkDeletions(Range) yield RangeDeletionEntry by RangeComparator
);
