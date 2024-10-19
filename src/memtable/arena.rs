pub use skl::Height;

mod multiple_version;
mod table;

pub use multiple_version::VersionedArenaTable;
pub use table::ArenaTable;

/// Options to configure the [`ArenaTable`].
#[derive(Debug, Copy, Clone)]
pub struct ArenaTableOptions {
  capacity: u32,
  map_anon: bool,
  max_height: Height,
}

impl Default for ArenaTableOptions {
  #[inline]
  fn default() -> Self {
    Self {
      capacity: 2048,
      map_anon: false,
      max_height: Height::try_from(20u8).unwrap(),
    }
  }
}

impl ArenaTableOptions {
  /// Sets the capacity of the table.
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
