#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
macro_rules! memmap_or_not {
  ($opts:ident($arena_opts:ident)) => {{
    if $opts.map_anon() {
      $arena_opts
        .map_anon::<KeyPointer<K>, ValuePointer<V>, _>()
        .map_err(skl::error::Error::IO)
    } else {
      $arena_opts.alloc::<KeyPointer<K>, ValuePointer<V>, _>()
    }
    .map(|map| Self { map })
  }};
}

#[cfg(not(all(feature = "memmap", not(target_family = "wasm"))))]
macro_rules! memmap_or_not {
  ($opts:ident($arena_opts:ident)) => {{
    $arena_opts
      .alloc::<KeyPointer<K>, ValuePointer<V>, _>()
      .map(|map| Self { map })
  }};
}

pub use skl::Height;

/// Options to configure the [`Table`] or [`MultipleVersionTable`].
#[derive(Debug, Copy, Clone)]
pub struct TableOptions {
  capacity: u32,
  map_anon: bool,
  max_height: Height,
}

impl Default for TableOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
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

pub use multiple_version::MultipleVersionTable;
pub use table::Table;
