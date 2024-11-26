#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
macro_rules! memmap_or_not {
  ($opts:ident($arena_opts:ident)) => {{
    if $opts.map_anon() {
      $arena_opts
        .map_anon()
        .map_err(skl::error::Error::IO)
    } else {
      $arena_opts.alloc()
    }
  }};
}

#[cfg(not(all(feature = "memmap", not(target_family = "wasm"))))]
macro_rules! memmap_or_not {
  ($opts:ident($arena_opts:ident)) => {{
    $arena_opts
      .alloc()
  }};
}

use core::slice;

pub use skl::Height;

pub use dbutils::equivalentor::*;

/// Options to configure the [`Table`] or [`MultipleVersionTable`].
#[derive(Debug, Copy, Clone)]
pub struct TableOptions<C = Ascend> {
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
      cmp: Ascend,
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


struct MemtableComparator<C> {
  /// The start pointer of the parent ARENA.
  ptr: *const u8,
  cmp: std::sync::Arc<C>,
}

impl<C> Clone for MemtableComparator<C>
where
  C: Clone
{
  #[inline]
  fn clone(&self) -> Self {
    Self {
      ptr: self.ptr,
      cmp: self.cmp.clone(),
    }
  }
}

impl<C> core::fmt::Debug for MemtableComparator<C>
where
  C: core::fmt::Debug,
{
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("MemtableComparator")
      .field("ptr", &self.ptr)
      .field("cmp", &self.cmp)
      .finish()
  }
}

impl<C> dbutils::equivalentor::Equivalentor for MemtableComparator<C>
where
  C: dbutils::equivalentor::Equivalentor,
{
  #[inline]
  fn equivalent(&self, a: &[u8], b: &[u8]) -> bool {
    let aoffset = u32::from_le_bytes(a[0..4].try_into().unwrap()) as usize;
    let alen = u32::from_le_bytes(a[4..8].try_into().unwrap()) as usize;

    let boffset = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let blen = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;

    unsafe {
      let a = slice::from_raw_parts(self.ptr.add(aoffset), alen);
      let b = slice::from_raw_parts(self.ptr.add(boffset), blen);
      self.cmp.equivalent(a, b)
    }
  }
}

impl<C> dbutils::equivalentor::Comparator for MemtableComparator<C>
where
  C: dbutils::equivalentor::Comparator,
{
  #[inline]
  fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let aoffset = u32::from_le_bytes(a[0..4].try_into().unwrap()) as usize;
    let alen = u32::from_le_bytes(a[4..8].try_into().unwrap()) as usize;

    let boffset = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let blen = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;

    unsafe {
      let a = slice::from_raw_parts(self.ptr.add(aoffset), alen);
      let b = slice::from_raw_parts(self.ptr.add(boffset), blen);
      self.cmp.compare(a, b)
    }
  }
}
