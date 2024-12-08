macro_rules! point_entry_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    $(#[$meta])*
    pub struct $ent<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::memtable::dynamic::MemtableComparator<C>>,
      data: core::cell::OnceCell<$crate::types::RawEntryRef<'a>>,
    }

    impl<'a, S, C> Clone for $ent<'a, S, C>
    where
      S: $crate::State<'a>,

    {
      #[inline]
      fn clone(&self) -> Self {
        Self {
          ent: self.ent.clone(),
          data: self.data.clone(),
        }
      }
    }

    impl<'a, S, C> $ent<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      #[inline]
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::memtable::dynamic::MemtableComparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, C> $crate::memtable::dynamic::MemtableEntry<'a> for $ent<'a, $crate::Active, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Value = &'a [u8];

      #[inline]
      fn key(&self) -> &'a [u8] {
        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        }).key()
      }

      #[inline]
      fn value(&self) -> Self::Value {
        let ent = self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        });

        ent.value().expect("entry in Active state must have a value")
      }

      #[inline]
      fn next(&mut self) -> Option<Self> {
        self.ent.next().map(Self::new)
      }

      #[inline]
      fn prev(&mut self) -> Option<Self> {
        self.ent.prev().map(Self::new)
      }
    }

    impl<'a, C> $crate::memtable::dynamic::MemtableEntry<'a> for $ent<'a, $crate::MaybeTombstone, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Value = Option<&'a [u8]>;

      #[inline]
      fn key(&self) -> &'a [u8] {
        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        }).key()
      }

      #[inline]
      fn value(&self) -> Self::Value {
        let ent = self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        });

        ent.value()
      }

      #[inline]
      fn next(&mut self) -> Option<Self> {
        self.ent.next().map(Self::new)
      }

      #[inline]
      fn prev(&mut self) -> Option<Self> {
        self.ent.prev().map(Self::new)
      }
    }

    $(
      impl<'a, S, C> $crate::WithVersion for $ent<'a, S, C>
      where
        C: dbutils::equivalentor::BytesComparator,
        S: $crate::State<'a>,
      {
        #[inline]
        fn $version(&self) -> u64 {
          self.ent.$version()
        }
      }
    )?
  };
}

macro_rules! range_entry_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident => $raw:ident.$fetch:ident) $(::$version:ident)?
  ) => {
    $(#[$meta])*
    pub struct $ent<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::memtable::dynamic::MemtableRangeComparator<C>>,
      data: core::cell::OnceCell<$crate::types::$raw<'a>>,
    }

    impl<'a, S, C> Clone for $ent<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      #[inline]
      fn clone(&self) -> Self {
        Self {
          ent: self.ent.clone(),
          data: self.data.clone(),
        }
      }
    }

    impl<'a, S, C> $ent<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::memtable::dynamic::MemtableRangeComparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, S, C> $crate::memtable::dynamic::RangeEntry<'a> for $ent<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::State<'a>,
    {
      #[inline]
      fn start_bound(&self) -> core::ops::Bound<&'a [u8]> {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.start_bound()
      }

      #[inline]
      fn end_bound(&self) -> core::ops::Bound<&'a [u8]> {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.end_bound()
      }

      #[inline]
      fn next(&mut self) -> Option<Self> {
        self.ent.next().map(Self::new)
      }

      #[inline]
      fn prev(&mut self) -> Option<Self> {
        self.ent.prev().map(Self::new)
      }
    }

    $(
      impl<'a, S, C> $crate::WithVersion for $ent<'a, S, C>
      where
        C: dbutils::equivalentor::BytesComparator,
        S: $crate::State<'a>,
      {
        #[inline]
        fn $version(&self) -> u64 {
          self.ent.$version()
        }
      }
    )?
  };
}

macro_rules! range_deletion_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    range_entry_wrapper! {
      $(#[$meta])*
      $ent($inner => RawRangeDeletionRef.fetch_range_deletion) $(::$version)?
    }

    impl<'a, S, C> crate::memtable::dynamic::RangeDeletionEntry<'a>
      for $ent<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::State<'a>,
    {
    }
  };
}

macro_rules! range_update_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    range_entry_wrapper! {
      $(#[$meta])*
      $ent($inner => RawRangeUpdateRef.fetch_range_update) $(::$version)?
    }

    impl<'a, C> crate::memtable::dynamic::RangeUpdateEntry<'a>
      for $ent<'a, $crate::MaybeTombstone, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Value = Option<&'a [u8]>;

      #[inline]
      fn value(&self) -> Self::Value {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
        ent.value()
      }
    }

    impl<'a, C> crate::memtable::dynamic::RangeUpdateEntry<'a>
      for $ent<'a, $crate::Active, C>
    where
      C: dbutils::equivalentor::BytesComparator,
    {
      type Value = &'a [u8];

      #[inline]
      fn value(&self) -> Self::Value {
        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
        ent.value().expect("entry in Active state must have a value")
      }
    }
  };
}

macro_rules! iter_wrapper {
  (
    $(#[$meta:meta])*
    $iter:ident($inner:ident) yield $ent:ident by $cmp:ident
  ) => {
    $(#[$meta])*
    pub struct $iter<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      iter: $inner<'a, $crate::types::RecordPointer, (), S, $crate::memtable::dynamic::$cmp<C>>,
    }

    impl<'a, S, C> $iter<'a, S, C>
    where
      S: $crate::State<'a>,
    {
      #[inline]
      pub(super) const fn new(iter: $inner<'a, $crate::types::RecordPointer, (), S, $crate::memtable::dynamic::$cmp<C>>) -> Self {
        Self { iter }
      }
    }

    impl<'a, S, C> Iterator for $iter<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::State<'a>,

    {
      type Item = $ent<'a, S, C>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map($ent::new)
      }
    }

    impl<'a, S, C> DoubleEndedIterator for $iter<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::State<'a>,

    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map($ent::new)
      }
    }
  };
}

macro_rules! range_wrapper {
  (
    $(#[$meta:meta])*
    $iter:ident($inner:ident) yield $ent:ident by $cmp:ident
  ) => {
    $(#[$meta])*
    pub struct $iter<'a, S, Q, R, C>
    where
      S: $crate::State<'a>,
      Q: ?Sized,
    {
      range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, $crate::memtable::dynamic::$cmp<C>>,
    }

    impl<'a, S, Q, R, C> $iter<'a, S, Q, R, C>
    where
      S: $crate::State<'a>,
      Q: ?Sized,
    {
      #[inline]
      pub(super) const fn new(range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, $crate::memtable::dynamic::$cmp<C>>) -> Self {
        Self { range }
      }
    }

    impl<'a, S, Q, R, C> Iterator for $iter<'a, S, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::State<'a>,
      R: core::ops::RangeBounds<Q>,
      Q: ?Sized + core::borrow::Borrow<[u8]>,
    {
      type Item = $ent<'a, S, C>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map($ent::new)
      }
    }

    impl<'a, S, Q, R, C> DoubleEndedIterator for $iter<'a, S, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::State<'a>,

      R: core::ops::RangeBounds<Q>,
      Q: ?Sized + core::borrow::Borrow<[u8]>,
    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map($ent::new)
      }
    }
  };
}
