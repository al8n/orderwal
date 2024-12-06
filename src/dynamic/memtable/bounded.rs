macro_rules! point_entry_wrapper {
  (
    $(#[$meta:meta])*
    $ent:ident($inner:ident) $(::$version:ident)?
  ) => {
    $(#[$meta])*
    pub struct $ent<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::MemtableComparator<C>>,
      data: core::cell::OnceCell<$crate::types::RawEntryRef<'a>>,
    }

    impl<'a, S, C> Clone for $ent<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,

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
      S: $crate::dynamic::types::State<'a>,
    {
      #[inline]
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::MemtableComparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, C> $crate::dynamic::memtable::MemtableEntry<'a> for $ent<'a, $crate::dynamic::types::Active, C>
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

    impl<'a, C> $crate::dynamic::memtable::MemtableEntry<'a> for $ent<'a, $crate::dynamic::types::MaybeTombstone, C>
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
        S: $crate::dynamic::types::State<'a>,
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
      S: $crate::dynamic::types::State<'a>,
    {
      ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::MemtableRangeComparator<C>>,
      data: core::cell::OnceCell<$crate::types::$raw<'a>>,
    }

    impl<'a, S, C> Clone for $ent<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
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
      S: $crate::dynamic::types::State<'a>,
    {
      pub(super) fn new(ent: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::MemtableRangeComparator<C>>) -> Self {
        Self {
          ent,
          data: core::cell::OnceCell::new(),
        }
      }
    }

    impl<'a, S, C> $crate::dynamic::memtable::RangeEntry<'a> for $ent<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,
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
        S: $crate::dynamic::types::State<'a>,
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

    impl<'a, S, C> crate::dynamic::memtable::RangeDeletionEntry<'a>
      for $ent<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,
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

    impl<'a, C> crate::dynamic::memtable::RangeUpdateEntry<'a>
      for $ent<'a, $crate::dynamic::types::MaybeTombstone, C>
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

    impl<'a, C> crate::dynamic::memtable::RangeUpdateEntry<'a>
      for $ent<'a, $crate::dynamic::types::Active, C>
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
      S: $crate::dynamic::types::State<'a>,
    {
      iter: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::$cmp<C>>,
    }

    impl<'a, S, C> $iter<'a, S, C>
    where
      S: $crate::dynamic::types::State<'a>,
    {
      #[inline]
      pub(super) const fn new(iter: $inner<'a, $crate::types::RecordPointer, (), S, $crate::dynamic::memtable::bounded::$cmp<C>>) -> Self {
        Self { iter }
      }
    }

    impl<'a, S, C> Iterator for $iter<'a, S, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,

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
      S: $crate::dynamic::types::State<'a>,

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
      S: $crate::dynamic::types::State<'a>,
      Q: ?Sized,
    {
      range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, $crate::dynamic::memtable::bounded::$cmp<C>>,
    }

    impl<'a, S, Q, R, C> $iter<'a, S, Q, R, C>
    where
      S: $crate::dynamic::types::State<'a>,
      Q: ?Sized,
    {
      #[inline]
      pub(super) const fn new(range: $inner<'a, $crate::types::RecordPointer, (), S, Q, R, $crate::dynamic::memtable::bounded::$cmp<C>>) -> Self {
        Self { range }
      }
    }

    impl<'a, S, Q, R, C> Iterator for $iter<'a, S, Q, R, C>
    where
      C: dbutils::equivalentor::BytesComparator,
      S: $crate::dynamic::types::State<'a>,

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
      S: $crate::dynamic::types::State<'a>,

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
    let ptr = $arena.raw_ptr();
    let points_cmp = MemtableComparator::new(ptr, cmp.clone());
    let rng_cmp = MemtableRangeComparator::new(ptr, cmp.clone());

    let b = Builder::with(points_cmp).with_options(arena_opts);

    #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
    let points: GenericSkipMap<_, _, MemtableComparator<C>> = construct_skl!(b(mmap))?;
    #[cfg(not(all(feature = "memmap", not(target_family = "wasm"))))]
    let points: GenericSkipMap<_, _, MemtableComparator<C>> = construct_skl!(b)?;

    let allocator = points.allocator().clone();
    let range_del_skl = GenericSkipMap::<_, _, MemtableRangeComparator<C>>::create_from_allocator(
      allocator.clone(),
      rng_cmp.clone(),
    )?;
    let range_key_skl = GenericSkipMap::<_, _, MemtableRangeComparator<C>>::create_from_allocator(
      allocator, rng_cmp,
    )?;

    Ok(Self {
      skl: points,
      range_updates_skl: range_key_skl,
      range_deletions_skl: range_del_skl,
      cmp,
    })
  }};
}

use crate::types::{
  fetch_entry, fetch_raw_key, fetch_raw_range_deletion_entry, fetch_raw_range_key_start_bound,
  fetch_raw_range_update_entry,
};

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
