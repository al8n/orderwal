macro_rules! construct_skl {
  ($builder:ident) => {{
    $builder.alloc()
  }};
  ($builder:ident($mmap:ident)) => {{
    if $mmap {
      $builder.map_anon().map_err(skl::error::Error::IO)
    } else {
      $builder.alloc()
    }
  }};
}

macro_rules! memmap_or_not {
  ($prefix: ident => $opts:ident($arena:ident)) => {{
    paste::paste! {
      use skl::Arena;

      let arena_opts = skl::Options::new()
      .with_capacity($opts.capacity())
      .with_freelist(skl::options::Freelist::None)
      .with_unify(false)
      .with_max_height($opts.max_height());

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      let mmap = $opts.map_anon();
      let cmp = Arc::new($opts.cmp);
      let ptr = $arena.raw_ptr();
      let points_cmp = <T::Comparator<C> as $crate::types::sealed::ComparatorConstructor<_>>::new(ptr, cmp.clone());
      let range_del_cmp = <T::RangeComparator<C> as $crate::types::sealed::ComparatorConstructor<_>>::new(ptr, cmp.clone());
      let range_update_cmp = <T::RangeComparator<C> as $crate::types::sealed::ComparatorConstructor<_>>::new(ptr, cmp.clone());

      let b = skl::generic::Builder::with(points_cmp).with_options(arena_opts);

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      let points: SkipMap<_, _, _> = construct_skl!(b(mmap))?;
      #[cfg(not(all(feature = "memmap", not(target_family = "wasm"))))]
      let points: SkipMap<_, _, C::[< $prefix:camel Comparator >]> = construct_skl!(b)?;

      let allocator = points.allocator().clone();
      let range_del_skl = SkipMap::<_, _, _>::create_from_allocator(
        allocator.clone(),
        range_del_cmp,
      )?;
      let range_key_skl =
        SkipMap::<_, _, _>::create_from_allocator(allocator, range_update_cmp)?;

      Ok(Self {
        skl: points,
        range_updates_skl: range_key_skl,
        range_deletions_skl: range_del_skl,
        cmp,
      })
    }
  }};
}

macro_rules! memtable {
  ($mode:ident($($version:ident)?)) => {
    paste::paste! {
      use among::Among;
      use skl::{
        either::Either, generic::$mode::{sync::SkipMap, Map}
      };
      use triomphe::Arc;

      use crate::{
        memtable::{Memtable, bounded::TableOptions},
        types::{Mode, RecordPointer},
      };

      /// A memory table implementation based on ARENA [`SkipMap`](skl).
      pub struct Table<C, T>
      where
        T: $crate::types::TypeMode,
      {
        pub(in crate::memtable) cmp: Arc<C>,
        pub(in crate::memtable) skl: SkipMap<RecordPointer, (), T::Comparator<C>>,
        pub(in crate::memtable) range_deletions_skl:
          SkipMap<RecordPointer, (), T::RangeComparator<C>>,
        pub(in crate::memtable) range_updates_skl: SkipMap<RecordPointer, (), T::RangeComparator<C>>,
      }

      impl<C, T> Memtable for Table<C, T>
      where
        C: 'static,
        T: $crate::types::TypeMode,
        T::Comparator<C>: for<'a> dbutils::equivalentor::TypeRefComparator<'a, RecordPointer> + 'static,
        T::RangeComparator<C>: for<'a> dbutils::equivalentor::TypeRefComparator<'a, RecordPointer> + 'static,
      {
        type Options = TableOptions<C>;

        type Error = skl::error::Error;

        #[inline]
        fn new<A>(arena: A, opts: Self::Options) -> Result<Self, Self::Error>
        where
          Self: Sized,
          A: rarena_allocator::Allocator,
        {
          memmap_or_not!(dynamic => opts(arena))
        }

        #[inline]
        fn len(&self) -> usize {
          self.skl.len() + self.range_deletions_skl.len() + self.range_updates_skl.len()
        }

        #[inline]
        fn insert(&self, [< _ $($version)? >]: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
          self
            .skl
            .insert($([< _ $version >].unwrap(),)? &pointer, &())
            .map(|_| ())
            .map_err(Among::unwrap_right)
        }

        #[inline]
        fn remove(&self, [< _ $($version)? >]: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
          self
            .skl
            .get_or_remove($([< _ $version >].unwrap(),)? &key)
            .map(|_| ())
            .map_err(Either::unwrap_right)
        }

        #[inline]
        fn range_remove(&self, [< _ $($version)? >]: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
          self
            .range_deletions_skl
            .insert($([< _ $version >].unwrap(),)? &pointer, &())
            .map(|_| ())
            .map_err(Among::unwrap_right)
        }

        #[inline]
        fn range_set(&self, [< _ $($version)? >]: Option<u64>, pointer: RecordPointer) -> Result<(), Self::Error> {
          self
            .range_updates_skl
            .insert($([< _ $version >].unwrap(),)? &pointer, &())
            .map(|_| ())
            .map_err(Among::unwrap_right)
        }

        #[inline]
        fn range_unset(&self, [< _ $($version)? >]: Option<u64>, key: RecordPointer) -> Result<(), Self::Error> {
          self
            .range_updates_skl
            .get_or_remove($([< _ $version >].unwrap(),)? &key)
            .map(|_| ())
            .map_err(Either::unwrap_right)
        }

        #[inline]
        fn mode() -> Mode {
          Mode::[< $mode:camel >]
        }
      }
    }
  };
}

macro_rules! point_entry_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    $(#[$meta])*
    pub struct $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::TypeMode
    {
      pub(in crate::memtable) ent: $inner<'a, $crate::types::RecordPointer, (), S, T::Comparator<C>>,
      data: core::cell::OnceCell<$crate::types::RawEntryRef<'a, T>>,
    }

    impl<'a, S, C, T> core::fmt::Debug for $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Comparator<C>: $crate::types::sealed::PointComparator<C>,
    {
      fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        use crate::types::sealed::PointComparator;

        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        }).write_fmt(stringify!($ent), f)
      }
    }

    impl<'a, S, C, T> Clone for $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::TypeMode,
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
      T: $crate::types::TypeMode,
    {
      #[inline]
      pub(in crate::memtable) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, T::Comparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, C, T> $crate::memtable::MemtableEntry<'a> for $ent<'a, $crate::Active, C, T>
    where
      C: 'static,
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Comparator<C>: $crate::types::sealed::PointComparator<C> + dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
    {
      type Key = <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output;
      type Value = <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output;

      #[inline]
      fn key(&self) -> Self::Key {
        use $crate::types::sealed::{Pointee, PointComparator};

        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        })
        .key()
        .output()
      }

      #[inline]
      fn value(&self) -> Self::Value {
        use $crate::types::sealed::{Pointee, PointComparator};

        let ent = self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        });

        ent.value().expect("entry in Active state must have a value").output()
      }

      #[inline]
      fn next(&self) -> Option<Self> {
        self.ent.next().map(Self::new)
      }

      #[inline]
      fn prev(&self) -> Option<Self> {
        self.ent.prev().map(Self::new)
      }
    }

    impl<'a, C, T> $crate::memtable::MemtableEntry<'a> for $ent<'a, $crate::MaybeTombstone, C, T>
    where
      C: 'static,
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Comparator<C>: $crate::types::sealed::PointComparator<C> + dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
    {
      type Key = <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output;
      type Value = Option<<T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output>;

      #[inline]
      fn key(&self) -> Self::Key {
        use $crate::types::sealed::{Pointee, PointComparator};

        self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        })
        .key()
        .output()
      }

      #[inline]
      fn value(&self) -> Self::Value {
        use $crate::types::sealed::{Pointee, PointComparator};

        let ent = self.data.get_or_init(|| {
          self.ent.comparator().fetch_entry(self.ent.key())
        });

        ent.value().map(|v| v.output())
      }

      #[inline]
      fn next(&self) -> Option<Self> {
        self.ent.next().map(Self::new)
      }

      #[inline]
      fn prev(&self) -> Option<Self> {
        self.ent.prev().map(Self::new)
      }
    }

    $(
      impl<'a, S, C, T> $crate::WithVersion for $ent<'a, S, C, T>
      where
        C: 'static,
        S: $crate::State<'a>,
        T: $crate::types::TypeMode,
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
      T: $crate::types::TypeMode,
    {
      pub(crate) ent: $inner<'a, $crate::types::RecordPointer, (), S, T::RangeComparator<C>>,
      data: core::cell::OnceCell<$crate::types::$raw<'a, T>>,
    }

    impl<'a, S, C, T> core::fmt::Debug for $ent<'a, S, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>
        + $crate::types::sealed::RangeComparator<C>,
    {
      fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        use crate::types::sealed::RangeComparator;

        self.data.get_or_init(|| {
          self.ent.comparator().$fetch(self.ent.key())
        }).write_fmt(stringify!($ent), f)
      }
    }

    impl<'a, S, C, T> Clone for $ent<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::TypeMode,
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
      T: $crate::types::TypeMode,
    {
      pub(in crate::memtable) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, T::RangeComparator<C>>) -> Self {
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
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>
        + $crate::types::sealed::RangeComparator<C>,
    {
      type Key = <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output;

      #[inline]
      fn start_bound(&self) -> core::ops::Bound<Self::Key> {
        use $crate::types::sealed::{Pointee, RangeComparator};

        let ent = self
          .data
          .get_or_init(|| self.ent.comparator().$fetch(self.ent.key()));
        ent.start_bound().map(|k| k.output())
      }

      #[inline]
      fn end_bound(&self) -> core::ops::Bound<Self::Key> {
        use $crate::types::sealed::{Pointee, RangeComparator};

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
        T: $crate::types::TypeMode,
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
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>
        + $crate::types::sealed::RangeComparator<C>,
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
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>
        + $crate::types::sealed::RangeComparator<C>,
    {
      type Value = Option<<T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output>;

      #[inline]
      fn value(&self) -> Self::Value {
        use $crate::types::sealed::{RangeComparator, Pointee};

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
      T: $crate::types::TypeMode,
      T::Key<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      T::Value<'a>: $crate::types::sealed::Pointee<'a, Input = &'a [u8]>,
      <T::Key<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output: 'a,
      T::RangeComparator<C>:
        dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer>
        + $crate::types::sealed::RangeComparator<C>,
    {
      type Value = <T::Value<'a> as $crate::types::sealed::Pointee<'a>>::Output;

      #[inline]
      fn value(&self) -> Self::Value {
        use $crate::types::sealed::{RangeComparator, Pointee};

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
      T: $crate::types::TypeMode,
    {
      iter: $inner<'a, $crate::types::RecordPointer, (), S, T::$cmp<C>>,
    }

    impl<'a, S, C, T> $iter<'a, S, C, T>
    where
      S: $crate::State<'a>,
      T: $crate::types::TypeMode,
    {
      #[inline]
      pub(in crate::memtable) const fn new(iter: $inner<'a, $crate::types::RecordPointer, (), S, T::$cmp<C>>) -> Self {
        Self { iter }
      }
    }

    impl<'a, S, C, T> Iterator for $iter<'a, S, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      T: $crate::types::TypeMode,
      T::$cmp<C>: dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer> + 'a,
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
      T: $crate::types::TypeMode,
      T::$cmp<C>: dbutils::equivalentor::TypeRefComparator<'a, $crate::types::RecordPointer> + 'a,
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
      T: $crate::types::TypeMode,
    {
      range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, T::$cmp<C>>,
    }

    impl<'a, S, Q, R, C, T> $iter<'a, S, Q, R, C, T>
    where
      S: $crate::State<'a>,
      Q: ?Sized,
      T: $crate::types::TypeMode,
    {
      #[inline]
      pub(in crate::memtable) const fn new(range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, T::$cmp<C>>) -> Self {
        Self { range }
      }
    }

    impl<'a, S, Q, R, C, T> Iterator for $iter<'a, S, Q, R, C, T>
    where
      C: 'static,
      S: $crate::State<'a>,
      R: core::ops::RangeBounds<Q>,
      Q: ?Sized,
      T: $crate::types::TypeMode,
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
      T: $crate::types::TypeMode,
      T::$cmp<C>: dbutils::equivalentor::TypeRefQueryComparator<'a, $crate::types::RecordPointer, Q> + 'a,
    {
      #[inline]
      fn next_back(&mut self) -> Option<Self::Item> {
        self.range.next_back().map($ent::new)
      }
    }
  };
}

use skl::generic::Ascend;
pub use skl::Height;

pub(crate) mod multiple_version;
pub(crate) mod unique;

/// Options to configure the [`Table`] or [`MultipleVersionTable`].
#[derive(Debug, Copy, Clone)]
pub struct TableOptions<C = Ascend> {
  capacity: u32,
  map_anon: bool,
  max_height: Height,
  pub(in crate::memtable) cmp: C,
}

impl<C: Default> Default for TableOptions<C> {
  #[inline]
  fn default() -> Self {
    Self::with_comparator(Default::default())
  }
}

impl TableOptions {
  /// Creates a new instance of `TableOptions` with the default options.
  #[inline]
  pub const fn new() -> Self {
    Self {
      capacity: 8192,
      map_anon: false,
      max_height: Height::new(),
      cmp: Ascend::new(),
    }
  }
}

impl<C> TableOptions<C> {
  /// Creates a new instance of `TableOptions` with the default options.
  #[inline]
  pub const fn with_comparator(cmp: C) -> TableOptions<C> {
    Self {
      capacity: 8192,
      map_anon: false,
      max_height: Height::new(),
      cmp,
    }
  }

  /// Sets the capacity of the table.
  ///
  /// Default is `8KB`.
  #[inline]
  pub const fn with_capacity(mut self, capacity: u32) -> Self {
    self.capacity = capacity;
    self
  }

  /// Sets the table to use anonymous memory.
  #[inline]
  pub const fn with_map_anon(mut self, map_anon: bool) -> Self {
    self.map_anon = map_anon;
    self
  }

  /// Sets the maximum height of the table.
  ///
  /// Default is `20`.
  #[inline]
  pub const fn with_max_height(mut self, max_height: Height) -> Self {
    self.max_height = max_height;
    self
  }

  /// Returns the capacity of the table.
  #[inline]
  pub const fn capacity(&self) -> u32 {
    self.capacity
  }

  /// Returns `true` if the table is using anonymous memory.
  #[inline]
  pub const fn map_anon(&self) -> bool {
    self.map_anon
  }

  /// Returns the maximum height of the table.
  #[inline]
  pub const fn max_height(&self) -> Height {
    self.max_height
  }
}
