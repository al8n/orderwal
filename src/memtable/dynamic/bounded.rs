macro_rules! point_entry_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    $(#[$meta])*
    pub struct $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind
    {
      ent: $inner<'a, $crate::types::RecordPointer, (), S, T::Comparator<C>>,
      data: core::cell::OnceCell<$crate::types::RawEntryRef<'a, T>>,
    }

    impl<'a, S, C, T> Clone for $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
      T::Value<'a>: Clone,
      T::Key<'a>: Clone,
    {
      #[inline]
      fn clone(&self) -> Self {
        Self {
          ent: self.ent.clone(),
          data: self.data.clone(),
        }
      }
    }

    impl<'a, S, C, T> $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
    {
      #[inline]
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, T::Comparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, C, T> $crate::memtable::MemtableEntry<'a> for $ent<'a, $crate::Active, C, T>
    where
      C: 'static,
      T: $crate::types::Kind,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Comparator<C>: $crate::types::sealed::PointComparator<C> + dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>,
      <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
    {
      type Key = <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output;
      type Value = <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output;

      #[inline]
      fn key(&self) -> Self::Key {
        use crate::types::sealed::{Pointee, PointComparator};

        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        })
        .key()
        .output()
      }

      #[inline]
      fn value(&self) -> Self::Value {
        use crate::types::sealed::{Pointee, PointComparator};

        let ent = self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        });

        ent.value().expect("entry in Active state must have a value").output()
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

    impl<'a, C, T> $crate::memtable::MemtableEntry<'a> for $ent<'a, $crate::MaybeTombstone, C, T>
    where
      C: 'static,
      T: $crate::types::Kind,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Comparator<C>: $crate::types::sealed::PointComparator<C> + dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>,
      <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
    {
      type Key = <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output;
      type Value = Option<<T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output>;

      #[inline]
      fn key(&self) -> Self::Key {
        use crate::types::sealed::{Pointee, PointComparator};

        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        })
        .key()
        .output()
      }

      #[inline]
      fn value(&self) -> Self::Value {
        use crate::types::sealed::{Pointee, PointComparator};

        let ent = self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        });

        ent.value().map(|v| v.output())
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
      impl<'a, S, C, T> $crate::WithVersion for $ent<'a, S, C, T>
      where
        C: 'static,
        S: $crate::State<'a>,
        T: $crate::types::Kind,
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
    pub struct $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
    {
      pub(crate) ent: $inner<'a, $crate::types::RecordPointer, (), S, T::RangeComparator<C>>,
      data: core::cell::OnceCell<$crate::types::$raw<'a, T>>,
    }

    impl<'a, S, C, T> Clone for $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
      T::Value<'a>: Clone,
      T::Key<'a>: Clone,
    {
      #[inline]
      fn clone(&self) -> Self {
        Self {
          ent: self.ent.clone(),
          data: self.data.clone(),
        }
      }
    }

    impl<'a, S, C, T> $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
    {
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, T::RangeComparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, S, C, T> $crate::memtable::RangeEntry<'a> for $ent<'a, S, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      T: $crate::types::Kind,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, crate::types::RecordPointer>
        + crate::types::sealed::RangeComparator<C>,
    {
      type Key = <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output;

      #[inline]
      fn start_bound(&self) -> core::ops::Bound<Self::Key> {
        use crate::types::sealed::{Pointee, RangeComparator};

        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.start_bound().map(|k| k.output())
      }

      #[inline]
      fn end_bound(&self) -> core::ops::Bound<Self::Key> {
        use crate::types::sealed::{Pointee, RangeComparator};

        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.end_bound().map(|k| k.output())
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
      impl<'a, S, C, T> $crate::WithVersion for $ent<'a, S, C, T>
      where
        C: 'static,
        S: $crate::State<'a>,
        T: $crate::types::Kind,
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

    impl<'a, S, C, T> crate::memtable::RangeDeletionEntry<'a>
      for $ent<'a, S, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      T: $crate::types::Kind,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, crate::types::RecordPointer>
        + crate::types::sealed::RangeComparator<C>,
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

    impl<'a, C, T> crate::memtable::RangeUpdateEntry<'a>
      for $ent<'a, $crate::MaybeTombstone, C, T>
    where
      C: 'static,
      T: $crate::types::Kind,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, crate::types::RecordPointer>
        + crate::types::sealed::RangeComparator<C>,
    {
      type Value = Option<<T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output>;

      #[inline]
      fn value(&self) -> Self::Value {
        use crate::types::sealed::{RangeComparator, Pointee};

        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
        ent.value().map(|v| v.output())
      }
    }

    impl<'a, C, T> crate::memtable::RangeUpdateEntry<'a>
      for $ent<'a, $crate::Active, C, T>
    where
      C: 'static,
      T: $crate::types::Kind,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, crate::types::RecordPointer>
        + crate::types::sealed::RangeComparator<C>,
    {
      type Value = <T::Value<'a> as crate::types::sealed::Pointee<'a>>::Output;

      #[inline]
      fn value(&self) -> Self::Value {
        use crate::types::sealed::{RangeComparator, Pointee};

        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().fetch_range_update(self.ent.key()));
        ent.value().expect("entry in Active state must have a value").output()
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
    pub struct $iter<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
    {
      iter: $inner<'a, $crate::types::RecordPointer, (), S, T::$cmp<C>>,
    }

    impl<'a, S, C, T> $iter<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::Kind,
    {
      #[inline]
      pub(super) const fn new(iter: $inner<'a, $crate::types::RecordPointer, (), S, T::$cmp<C>>) -> Self {
        Self { iter }
      }
    }

    impl<'a, S, C, T> Iterator for $iter<'a, S, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      T: $crate::types::Kind,
      T::$cmp<C>: dbutils::equivalentor::TypeRefQueryComparator<'a, $crate::types::RecordPointer, $crate::types::RecordPointer> + 'a,
    {
      type Item = $ent<'a, S, C, T>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map($ent::new)
      }
    }

    impl<'a, S, C, T> DoubleEndedIterator for $iter<'a, S, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      T: $crate::types::Kind,
      T::$cmp<C>: dbutils::equivalentor::TypeRefQueryComparator<'a, $crate::types::RecordPointer, $crate::types::RecordPointer> + 'a,
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
    pub struct $iter<'a, S, Q, R, C, T>
    where
      S: $crate::State<'a>,
      Q: ?Sized,
      T: $crate::types::Kind,
    {
      range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, T::$cmp<C>>,
    }

    impl<'a, S, Q, R, C, T> $iter<'a, S, Q, R, C, T>
    where
      S: $crate::State<'a>,
      Q: ?Sized,
      T: $crate::types::Kind,
    {
      #[inline]
      pub(super) const fn new(range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, T::$cmp<C>>) -> Self {
        Self { range }
      }
    }

    impl<'a, S, Q, R, C, T> Iterator for $iter<'a, S, Q, R, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      R: core::ops::RangeBounds<Q>,
      Q: ?Sized,
      T: $crate::types::Kind,
      T::$cmp<C>: dbutils::equivalentor::TypeRefQueryComparator<'a, $crate::types::RecordPointer, Q> + 'a,
    {
      type Item = $ent<'a, S, C, T>;

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.range.next().map($ent::new)
      }
    }

    impl<'a, S, Q, R, C, T> DoubleEndedIterator for $iter<'a, S, Q, R, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      R: core::ops::RangeBounds<Q>,
      Q: ?Sized,
      T: $crate::types::Kind,
      T::$cmp<C>: dbutils::equivalentor::TypeRefQueryComparator<'a, $crate::types::RecordPointer, Q> + 'a,
    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map($ent::new)
      }
    }
  };
}
