use super::*;
use crate::{options::ArenaOptionsExt, Immutable};
use dbutils::{
  checksum::BuildChecksumer,
  types::{KeyRef, Type},
};
use skl::either::Either;

impl<M, S> Builder<M, S>
where
  M: BaseTable,
{
  /// Set if lock the meta of the WAL in the memory to prevent OS from swapping out the header of WAL.
  /// When using memory map backed WAL, the meta of the WAL
  /// is in the header, meta is frequently accessed,
  /// lock (`mlock` on the header) the meta can reduce the page fault,
  /// but yes, this means that one WAL will have one page are locked in memory,
  /// and will not be swapped out. So, this is a trade-off between performance and memory usage.
  ///
  /// Default is `true`.
  ///
  /// This configuration has no effect on windows and vec backed WAL.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_lock_meta(false);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn with_lock_meta(mut self, lock_meta: bool) -> Self {
    self.opts.lock_meta = lock_meta;
    self
  }

  /// Sets the option for read access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `read`-able if opened.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_read(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_read(mut self, read: bool) -> Self {
    self.opts.read = read;
    self
  }

  /// Sets the option for write access.
  ///
  /// This option, when true, will indicate that the file should be
  /// `write`-able if opened.
  ///
  /// If the file already exists, any write calls on it will overwrite its
  /// contents, without truncating it.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_write(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_write(mut self, write: bool) -> Self {
    self.opts.write = write;
    self
  }

  /// Sets the option for the append mode.
  ///
  /// This option, when true, means that writes will append to a file instead
  /// of overwriting previous contents.
  /// Note that setting `.write(true).append(true)` has the same effect as
  /// setting only `.append(true)`.
  ///
  /// For most filesystems, the operating system guarantees that all writes are
  /// atomic: no writes get mangled because another process writes at the same
  /// time.
  ///
  /// One maybe obvious note when using append-mode: make sure that all data
  /// that belongs together is written to the file in one operation. This
  /// can be done by concatenating strings before passing them to [`write()`],
  /// or using a buffered writer (with a buffer of adequate size),
  /// and calling [`flush()`] when the message is complete.
  ///
  /// If a file is opened with both read and append access, beware that after
  /// opening, and after every write, the position for reading may be set at the
  /// end of the file. So, before writing, save the current position (using
  /// <code>[seek]\([SeekFrom](std::io::SeekFrom)::[Current]\(opts))</code>), and restore it before the next read.
  ///
  /// ## Note
  ///
  /// This function doesn't create the file if it doesn't exist. Use the
  /// [`Options::with_create`] method to do so.
  ///
  /// [`write()`]: std::io::Write::write "io::Write::write"
  /// [`flush()`]: std::io::Write::flush "io::Write::flush"
  /// [seek]: std::io::Seek::seek "io::Seek::seek"
  /// [Current]: std::io::SeekFrom::Current "io::SeekFrom::Current"
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_append(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_append(mut self, append: bool) -> Self {
    self.opts.write = true;
    self.opts.append = append;
    self
  }

  /// Sets the option for truncating a previous file.
  ///
  /// If a file is successfully opened with this option set it will truncate
  /// the file to opts length if it already exists.
  ///
  /// The file must be opened with write access for truncate to work.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_write(true).with_truncate(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_truncate(mut self, truncate: bool) -> Self {
    self.opts.truncate = truncate;
    self.opts.write = true;
    self
  }

  /// Sets the option to create a new file, or open it if it already exists.
  /// If the file does not exist, it is created and set the lenght of the file to the given size.
  ///
  /// In order for the file to be created, [`Options::with_write`] or
  /// [`Options::with_append`] access must be used.
  ///
  /// See also [`std::fs::write()`][std::fs::write] for a simple function to
  /// create a file with some given data.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_write(true).with_create(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_create(mut self, val: bool) -> Self {
    self.opts.create = val;
    self
  }

  /// Sets the option to create a new file and set the file length to the given value, failing if it already exists.
  ///
  /// No file is allowed to exist at the target location, also no (dangling) symlink. In this
  /// way, if the call succeeds, the file returned is guaranteed to be new.
  ///
  /// This option is useful because it is atomic. Otherwise between checking
  /// whether a file exists and creating a new one, the file may have been
  /// created by another process (a TOCTOU race condition / attack).
  ///
  /// If `.with_create_new(true)` is set, [`.with_create()`] and [`.with_truncate()`] are
  /// ignored.
  ///
  /// The file must be opened with write or append access in order to create
  /// a new file.
  ///
  /// [`.with_create()`]: Builder::with_create
  /// [`.with_truncate()`]: Builder::with_truncate
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new()
  ///   .with_write(true)
  ///   .with_create_new(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_create_new(mut self, val: bool) -> Self {
    self.opts.create_new = val;
    self
  }

  /// Configures the anonymous memory map to be suitable for a process or thread stack.
  ///
  /// This option corresponds to the `MAP_STACK` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on file-backed memory maps and vec backed `Wal`.
  ///
  /// ## Example
  ///
  /// ```
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let stack = Builder::<LinkedTable<[u8], [u8]>>::new().with_stack(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_stack(mut self, stack: bool) -> Self {
    self.opts.stack = stack;
    self
  }

  /// Configures the anonymous memory map to be allocated using huge pages.
  ///
  /// This option corresponds to the `MAP_HUGETLB` flag on Linux. It has no effect on Windows.
  ///
  /// The size of the requested page can be specified in page bits. If not provided, the system
  /// default is requested. The requested length should be a multiple of this, or the mapping
  /// will fail.
  ///
  /// This option has no effect on file-backed memory maps and vec backed `Wal`.
  ///
  /// ## Example
  ///
  /// ```
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_huge(Some(8));
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_huge(mut self, page_bits: Option<u8>) -> Self {
    self.opts.huge = page_bits;
    self
  }

  /// Populate (prefault) page tables for a mapping.
  ///
  /// For a file mapping, this causes read-ahead on the file. This will help to reduce blocking on page faults later.
  ///
  /// This option corresponds to the `MAP_POPULATE` flag on Linux. It has no effect on Windows.
  ///
  /// This option has no effect on vec backed `Wal`.
  ///
  /// ## Example
  ///
  /// ```
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_populate(true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn with_populate(mut self, populate: bool) -> Self {
    self.opts.populate = populate;
    self
  }
}

impl<M, S> Builder<M, S>
where
  M: BaseTable,
{
  /// Get if lock the meta of the WAL in the memory to prevent OS from swapping out the header of WAL.
  /// When using memory map backed WAL, the meta of the WAL
  /// is in the header, meta is frequently accessed,
  /// lock (`mlock` on the header) the meta can reduce the page fault,
  /// but yes, this means that one WAL will have one page are locked in memory,
  /// and will not be swapped out. So, this is a trade-off between performance and memory usage.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_lock_meta(false);
  /// assert_eq!(opts.lock_meta(), false);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn lock_meta(&self) -> bool {
    self.opts.lock_meta
  }

  /// Returns `true` if the file should be opened with read access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_read(true);
  /// assert_eq!(opts.read(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn read(&self) -> bool {
    self.opts.read
  }

  /// Returns `true` if the file should be opened with write access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_write(true);
  /// assert_eq!(opts.write(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn write(&self) -> bool {
    self.opts.write
  }

  /// Returns `true` if the file should be opened with append access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_append(true);
  /// assert_eq!(opts.append(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn append(&self) -> bool {
    self.opts.append
  }

  /// Returns `true` if the file should be opened with truncate access.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_truncate(true);
  /// assert_eq!(opts.truncate(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn truncate(&self) -> bool {
    self.opts.truncate
  }

  /// Returns `true` if the file should be created if it does not exist.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_create(true);
  /// assert_eq!(opts.create(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn create(&self) -> bool {
    self.opts.create
  }

  /// Returns `true` if the file should be created if it does not exist and fail if it does.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_create_new(true);
  /// assert_eq!(opts.create_new(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn create_new(&self) -> bool {
    self.opts.create_new
  }

  /// Returns `true` if the memory map should be suitable for a process or thread stack.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_stack(true);
  /// assert_eq!(opts.stack(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn stack(&self) -> bool {
    self.opts.stack
  }

  /// Returns the page bits of the memory map.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_huge(Some(8));
  /// assert_eq!(opts.huge(), Some(8));
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn huge(&self) -> Option<u8> {
    self.opts.huge
  }

  /// Returns `true` if the memory map should populate (prefault) page tables for a mapping.
  ///
  /// ## Examples
  ///
  /// ```rust
  /// use orderwal::{Builder, multiple_version::LinkedTable};
  ///
  /// let opts = Builder::<LinkedTable<[u8], [u8]>>::new().with_populate(true);
  /// assert_eq!(opts.populate(), true);
  /// ```
  #[inline]
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub const fn populate(&self) -> bool {
    self.opts.populate
  }
}

impl<M, S> Builder<M, S>
where
  M: BaseTable,
{
  /// Creates a new in-memory write-ahead log but backed by an anonymous mmap.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::OrderWal, Builder};
  ///
  /// let wal = Builder::new()
  ///   .with_capacity(1024)
  ///   .map_anon::<OrderWal<[u8], [u8]>>()
  ///   .unwrap();
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub fn map_anon<W>(self) -> Result<W, Error<W::Memtable>>
  where
    W: Constructable<Memtable = M, Checksumer = S>,
  {
    let Self {
      opts,
      cks,
      memtable_opts,
    } = self;
    arena_options(opts.reserved())
      .merge(&opts)
      .map_anon()
      .map_err(Into::into)
      .and_then(|arena| W::new_in(arena, opts, memtable_opts, cks).map(W::from_core))
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::OrderWalReader, Builder};
  ///
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir.path().join("map.wal");
  ///
  /// # let wal = unsafe {
  /// #  Builder::new()
  /// #  .with_capacity(1000).with_create(true).with_read(true).with_write(true)
  /// #  .map_mut::<orderwal::base::OrderWal<[u8], [u8]>, _>(&path)
  /// #  .unwrap()
  /// # };
  ///
  /// let wal = unsafe {
  ///   Builder::new()
  ///     .map::<OrderWalReader<[u8], [u8]>, _>(&path)
  ///     .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub unsafe fn map<'a, W, P>(self, path: P) -> Result<W, Error<W::Memtable>>
  where
    S: BuildChecksumer,
    P: AsRef<std::path::Path>,
    W: Constructable<Memtable = M, Checksumer = S> + Immutable,
  {
    self
      .map_with_path_builder::<W, _, ()>(|| Ok(path.as_ref().to_path_buf()))
      .map_err(Either::unwrap_right)
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::OrderWalReader, Builder};
  ///
  /// # let dir = tempfile::tempdir().unwrap();
  /// # let path = dir.path().join("map_with_path_builder.wal");
  ///
  /// # let wal = unsafe {
  /// #  Builder::new()
  /// #  .with_capacity(1000).with_create(true).with_read(true).with_write(true)
  /// #  .map_mut::<orderwal::base::OrderWal<[u8], [u8]>, _>(&path)
  /// #  .unwrap()
  /// # };
  ///
  /// let wal = unsafe {
  ///   Builder::new()
  ///     .map_with_path_builder::<OrderWalReader<[u8], [u8]>, _, ()>(|| Ok(path))
  ///     .unwrap()
  /// };
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub unsafe fn map_with_path_builder<'a, W, PB, E>(
    self,
    path_builder: PB,
  ) -> Result<W, Either<E, Error<W::Memtable>>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    S: BuildChecksumer,
    W: Constructable<Memtable = M, Checksumer = S> + Immutable,
  {
    let Self {
      opts,
      cks,
      memtable_opts,
    } = self;

    arena_options(opts.reserved())
      .merge(&opts)
      .with_read(true)
      .map_with_path_builder(path_builder)
      .map_err(|e| e.map_right(Into::into))
      .and_then(|arena| {
        W::replay(arena, Options::new(), memtable_opts, true, cks)
          .map(Constructable::from_core)
          .map_err(Either::Right)
      })
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  ///  
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::OrderWal, Builder};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  /// let path = dir.path().join("map_mut_with_path_builder_example.wal");
  ///
  /// let wal = unsafe {
  ///   Builder::new()
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(1000)
  ///     .map_mut::<OrderWal<[u8], [u8]>, _>(&path)
  ///     .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub unsafe fn map_mut<'a, W, P>(self, path: P) -> Result<W, Error<W::Memtable>>
  where
    S: BuildChecksumer,
    P: AsRef<std::path::Path>,
    W: Constructable<Memtable = M, Checksumer = S>,
  {
    self
      .map_mut_with_path_builder::<W, _, ()>(|| Ok(path.as_ref().to_path_buf()))
      .map_err(Either::unwrap_right)
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  ///
  /// ## Example
  ///
  /// ```rust
  /// use orderwal::{base::OrderWal, Builder};
  ///
  /// let dir = tempfile::tempdir().unwrap();
  ///  
  /// let wal = unsafe {
  ///   Builder::new()
  ///     .with_create_new(true)
  ///     .with_read(true)
  ///     .with_write(true)
  ///     .with_capacity(1000)
  ///     .map_mut_with_path_builder::<OrderWal<[u8], [u8]>, _, ()>(
  ///       || Ok(dir.path().join("map_mut_with_path_builder_example.wal")),
  ///     )
  ///     .unwrap()
  /// };
  /// ```
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  pub unsafe fn map_mut_with_path_builder<'a, W, PB, E>(
    self,
    path_builder: PB,
  ) -> Result<W, Either<E, Error<W::Memtable>>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    S: BuildChecksumer,
    W: Constructable<Memtable = M, Checksumer = S>,
  {
    let path = path_builder().map_err(Either::Left)?;
    let exist = path.exists();
    let Self {
      opts,
      cks,
      memtable_opts,
    } = self;

    arena_options(opts.reserved())
      .merge(&opts)
      .map_mut(path)
      .map_err(Into::into)
      .and_then(|arena| {
        if !exist {
          W::new_in(arena, opts, memtable_opts, cks).map(W::from_core)
        } else {
          W::replay(arena, opts, memtable_opts, false, cks).map(W::from_core)
        }
      })
      .map_err(Either::Right)
  }
}
