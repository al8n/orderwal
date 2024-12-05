use skl::generic::multiple_version::sync::{Entry, Iter, Range};

point_entry_wrapper!(
  /// Point entry.
  PointEntry(Entry)::version
);

iter_wrapper!(
  /// The iterator for point entries.
  PointIter(Iter) yield PointEntry
);

range_wrapper!(
  /// The iterator over a subset of point entries.
  PointRange(Range) yield PointEntry
);
