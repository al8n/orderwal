use crate::{
  error::Error,
  log::Log,
  memtable::Memtable,
  options::{arena_options, Options},
};
use dbutils::checksum::Crc32;

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
mod memmap;

/// A write-ahead log builder.
pub struct Builder<M, S = Crc32>
where
  M: Memtable,
{
  pub(super) opts: Options,
  pub(super) cks: S,
  pub(super) memtable_opts: M::Options,
}

impl<M> Default for Builder<M>
where
  M: Memtable,
  M::Options: Default,
{
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<M> Builder<M>
where
  M: Memtable,
  M::Options: Default,
{
  /// Returns a new write-ahead log builder with the given options.
  #[inline]
  pub fn new() -> Self {
    Self {
      opts: Options::default(),
      cks: Crc32::default(),
      memtable_opts: M::Options::default(),
    }
  }
}

impl<M, S> Builder<M, S>
where
  M: Memtable,
{
  /// Returns a new write-ahead log builder with the new checksumer
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, checksum::Crc32, generic::BoundedTable};
  ///
  /// let opts = Builder::<BoundedTable<str, str>>::new().with_checksumer(Crc32::new());
  /// ```
  #[inline]
  pub fn with_checksumer<NS>(self, cks: NS) -> Builder<M, NS> {
    Builder {
      opts: self.opts,
      cks,
      memtable_opts: self.memtable_opts,
    }
  }

  /// Returns a new write-ahead log builder with the new options
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, Options, generic::BoundedTable};
  ///
  /// let opts = Builder::<BoundedTable<str, str>>::new().with_options(Options::default());
  /// ```
  #[inline]
  pub fn with_options(self, opts: Options) -> Self {
    Self {
      opts,
      cks: self.cks,
      memtable_opts: self.memtable_opts,
    }
  }

  /// Returns a new write-ahead log builder with the new options
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::{BoundedTable, BoundedTableOptions}};
  ///
  /// let opts = Builder::<BoundedTable<str, str>>::new().with_memtable_options(BoundedTableOptions::default());
  /// ```
  #[inline]
  pub fn with_memtable_options(self, opts: M::Options) -> Self {
    Self {
      opts: self.opts,
      cks: self.cks,
      memtable_opts: opts,
    }
  }

  /// Returns a new write-ahead log builder with the new memtable.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::{UnboundedTable, BoundedTable}};
  ///
  /// let opts = Builder::<UnboundedTable<str, str>>::new().change_memtable::<BoundedTable<str, str>>();
  /// ```
  #[inline]
  pub fn change_memtable<NM>(self) -> Builder<NM, S>
  where
    NM: Memtable,
    NM::Options: Default,
  {
    Builder {
      opts: self.opts,
      cks: self.cks,
      memtable_opts: NM::Options::default(),
    }
  }

  /// Returns a new write-ahead log builder with the new memtable and its options
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::{UnboundedTable, BoundedTable, BoundedTableOptions}};
  ///
  /// let opts = Builder::<UnboundedTable<str, str>>::new().change_memtable_with_options::<BoundedTable<str, str>>(BoundedTableOptions::default().with_capacity(1000));
  /// ```
  #[inline]
  pub fn change_memtable_with_options<NM>(self, opts: NM::Options) -> Builder<NM, S>
  where
    NM: Memtable,
  {
    Builder {
      opts: self.opts,
      cks: self.cks,
      memtable_opts: opts,
    }
  }

  /// Set the reserved bytes of the WAL.
  ///
  /// The `reserved` is used to configure the start position of the WAL. This is useful
  /// when you want to add some bytes as your own WAL's header.
  ///
  /// The default reserved is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let opts = Builder::<BoundedTable<str, str>>::new().with_reserved(8);
  /// ```
  #[inline]
  pub const fn with_reserved(mut self, reserved: u32) -> Self {
    self.opts = self.opts.with_reserved(reserved);
    self
  }

  /// Get the reserved of the WAL.
  ///
  /// The `reserved` is used to configure the start position of the WAL. This is useful
  /// when you want to add some bytes as your own WAL's header.
  ///
  /// The default reserved is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let opts = Builder::<BoundedTable<str, str>>::new().with_reserved(8);
  ///
  /// assert_eq!(opts.reserved(), 8);
  /// ```
  #[inline]
  pub const fn reserved(&self) -> u32 {
    self.opts.reserved()
  }

  /// Returns the magic version.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn magic_version(&self) -> u16 {
    self.opts.magic_version()
  }

  /// Returns the capacity of the WAL.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_capacity(1000);
  /// assert_eq!(options.capacity(), 1000);
  /// ```
  #[inline]
  pub const fn capacity(&self) -> u32 {
    self.opts.capacity()
  }

  /// Returns the maximum key length.
  ///
  /// The default value is `u16::MAX`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_maximum_key_size(1024);
  /// assert_eq!(options.maximum_key_size(), 1024);
  /// ```
  #[inline]
  pub const fn maximum_key_size(&self) -> u32 {
    self.opts.maximum_key_size()
  }

  /// Returns the maximum value length.
  ///
  /// The default value is `u32::MAX`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn maximum_value_size(&self) -> u32 {
    self.opts.maximum_value_size()
  }

  /// Returns `true` if the WAL syncs on write.
  ///
  /// The default value is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new();
  /// assert_eq!(options.sync(), true);
  /// ```
  #[inline]
  pub const fn sync(&self) -> bool {
    self.opts.sync()
  }

  /// Sets the capacity of the WAL.
  ///
  /// This configuration will be ignored when using file-backed memory maps.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_capacity(100);
  /// assert_eq!(options.capacity(), 100);
  /// ```
  #[inline]
  pub const fn with_capacity(mut self, cap: u32) -> Self {
    self.opts = self.opts.with_capacity(cap);
    self
  }

  /// Sets the maximum key length.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_maximum_key_size(1024);
  /// assert_eq!(options.maximum_key_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_key_size(mut self, size: u32) -> Self {
    self.opts = self.opts.with_maximum_key_size(size);
    self
  }

  /// Sets the maximum value length.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_maximum_value_size(1024);
  /// assert_eq!(options.maximum_value_size(), 1024);
  /// ```
  #[inline]
  pub const fn with_maximum_value_size(mut self, size: u32) -> Self {
    self.opts = self.opts.with_maximum_value_size(size);
    self
  }

  /// Sets the WAL to sync on write.
  ///
  /// The default value is `true`.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_sync(false);
  /// assert_eq!(options.sync(), false);
  /// ```
  #[inline]
  pub const fn with_sync(mut self, sync: bool) -> Self {
    self.opts = self.opts.with_sync(sync);
    self
  }

  /// Sets the magic version.
  ///
  /// The default value is `0`.
  ///
  /// ## Example
  ///
  /// ```rust
  ///
  /// use orderwal::{Builder, generic::BoundedTable};
  ///
  /// let options = Builder::<BoundedTable<str, str>>::new().with_magic_version(1);
  /// assert_eq!(options.magic_version(), 1);
  /// ```
  #[inline]
  pub const fn with_magic_version(mut self, version: u16) -> Self {
    self.opts = self.opts.with_magic_version(version);
    self
  }
}

impl<M, S> Builder<M, S>
where
  M: Memtable,
{
  /// Creates a new in-memory write-ahead log backed by an aligned vec.
  ///
  /// ## Examples
  ///
  /// ### Generic order WAL example
  ///
  /// ```rust
  /// use orderwal::{generic::{OrderWal, BoundedTable}, Builder};
  ///
  /// let wal = Builder::new()
  ///   .with_capacity(1024)
  ///   .alloc::<OrderWal<BoundedTable<str, str>>>()
  ///   .unwrap();
  /// ```
  ///
  /// ### Dynamic order WAL example
  ///
  /// ```rust
  /// use orderwal::{dynamic::{OrderWal, BoundedTable}, Builder};
  ///
  /// let wal = Builder::new()
  ///   .with_capacity(1024)
  ///   .alloc::<OrderWal<BoundedTable>>()
  ///   .unwrap();
  /// ```
  pub fn alloc<L>(self) -> Result<L, Error<L::Memtable>>
  where
    L: Log<Memtable = M, Checksumer = S>,
  {
    let Self {
      opts,
      cks,
      memtable_opts,
    } = self;
    arena_options(opts.reserved())
      .with_capacity(opts.capacity())
      .alloc()
      .map_err(Error::from_insufficient_space)
      .and_then(|arena| L::new(arena, opts, memtable_opts, cks))
  }
}
