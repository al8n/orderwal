macro_rules! impl_point_entry {
  ($ent:ident$([$version:ident])? ($ty:ty)) => {
    impl<'a, C> $crate::dynamic::memtable::MemtableEntry<'a> for $ent<'a, $ty, $crate::dynamic::memtable::bounded::MemtableComparator<C>>
      where
        C: dbutils::equivalentor::DynComparator<[u8], [u8]>,
      {
        type Value = $ty;

        #[inline]
        fn key(&self) -> &'a [u8] {
          $ent::key(self)
        }

        #[inline]
        fn value(&self) -> Self::Value {
          $ent::value(self)
        }

        #[inline]
        fn next(&mut self) -> core::option::Option<Self> {
          $ent::next(self)
        }

        #[inline]
        fn prev(&mut self) -> core::option::Option<Self> {
          $ent::prev(self)
        }
      }

      $(
        impl<'a, C> $crate::WithVersion for $ent<'a, $ty, $crate::dynamic::memtable::bounded::MemtableComparator<C>> {
          #[inline]
          fn $version(&self) -> u64 {
            $ent::version(self)
          }
        }
      )?
  };
}

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
  ($opts:ident($arena:ident)) => {{
    let arena_opts = Options::new()
      .with_capacity($opts.capacity())
      .with_freelist(skl::options::Freelist::None)
      .with_unify(false)
      .with_max_height($opts.max_height());

    #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
    let mmap = $opts.map_anon();
    let cmp = Arc::new($opts.cmp);
    let ptr = $arena.raw_pointer();
    let points_cmp = MemtableComparator::new(ptr, cmp.clone());
    let rng_cmp = MemtableRangeComparator::new(ptr, cmp.clone());

    let b = Builder::new()
      .with_options(arena_opts)
      .with_comparator(points_cmp);

    #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
    let points: SkipMap<MemtableComparator<C>> = construct_skl!(b(mmap))?;
    #[cfg(not(all(feature = "memmap", not(target_family = "wasm"))))]
    let points: SkipMap<MemtableComparator<C>> = construct_skl!(b)?;

    let allocator = points.allocator().clone();
    let range_del_skl = SkipMap::<MemtableRangeComparator<C>>::create_from_allocator(
      allocator.clone(),
      rng_cmp.clone(),
    )?;
    let range_key_skl =
      SkipMap::<MemtableRangeComparator<C>>::create_from_allocator(allocator, rng_cmp)?;

    Ok(Self {
      skl: points,
      range_updates_skl: range_key_skl,
      range_deletions_skl: range_del_skl,
    })
  }};
}

use core::{borrow::Borrow, cmp, ops::Bound};

use skl::generic::Type;
use triomphe::Arc;

use crate::types::{fetch_entry, fetch_raw_key, fetch_raw_range_deletion_entry, fetch_raw_range_key_start_bound, fetch_raw_range_update_entry, RawEntryRef, RawRangeDeletionRef, RawRangeUpdateRef, RecordPointer};

pub use dbutils::{
  equivalent::{Comparable, Equivalent},
  equivalentor::*,
};
pub use skl::Height;

/// Options to configure the [`Table`] or [`MultipleVersionTable`].
#[derive(Debug, Copy, Clone)]
pub struct TableOptions<C = Ascend<[u8]>> {
  capacity: u32,
  map_anon: bool,
  max_height: Height,
  cmp: C,
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

/// The multiple version memtable implementation.
pub mod multiple_version;
/// The memtable implementation.
pub mod table;

// pub use multiple_version::MultipleVersionTable;
// pub use table::Table;

mod comparator;
use comparator::MemtableComparator;
mod range_comparator;
use range_comparator::MemtableRangeComparator;