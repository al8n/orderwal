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

macro_rules! dynamic_memtable {
  ($mode:ident($($version:ident)?)) => {
    paste::paste! {
      use among::Among;
      use skl::{
        dynamic::BytesComparator, either::Either, generic::$mode::{sync::SkipMap, Map}
      };
      use triomphe::Arc;

      use crate::{
        memtable::{Memtable, bounded::TableOptions},
        types::{Mode, RecordPointer},
      };

      /// A memory table implementation based on ARENA [`SkipMap`](skl).
      pub struct Table<C, T>
      where
        T: $crate::types::Kind,
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
        T: $crate::types::Kind,
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

use skl::generic::Ascend;
pub use skl::Height;

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
