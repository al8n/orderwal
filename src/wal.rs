use super::*;

mod builder;
pub use builder::*;

pub(crate) mod sealed;

/// An abstract layer for the write-ahead log.
pub trait Wal<C, S>: Sized + sealed::WalSealed<C, S> {
  /// The iterator type.
  type Iter<'a>
  where
    Self: 'a;

  /// Creates a new in-memory write-ahead log backed by an aligned vec.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, WalBuilder, Options};
  ///
  /// let wal = OrderWal::new(WalBuilder::new(Options::new())).unwrap();
  /// ```
  fn new(b: WalBuidler<C, S>) -> Result<Self, Error> {
    let WalBuidler { opts, cmp, cks } = b;
    let arena = <Self::Allocator as Allocator>::new(
      arena_options(opts.reserved()).with_capacity(opts.capacity()),
    );
    <Self as sealed::WalSealed<C, S>>::new_in(arena, opts, cmp, cks)
      .map(|core| Self::from_core(core, false))
  }

  /// Creates a new in-memory write-ahead log but backed by an anonymous mmap.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, WalBuilder, Options};
  ///
  /// let wal = OrderWal::map_anon(WalBuidler::new(Options::new())).unwrap();
  /// ```
  fn map_anon(b: WalBuidler<C, S>) -> Result<Self, Error> {
    let WalBuidler { opts, cmp, cks } = b;
    let mmap_opts = MmapOptions::new().len(opts.capacity()).huge(opts.huge());
    <Self::Allocator as Allocator>::map_anon(arena_options(opts.reserved()), mmap_opts)
      .map_err(Into::into)
      .and_then(|arena| {
        <Self as sealed::WalSealed<C, S>>::new_in(arena, opts, cmp, cks)
          .map(|core| Self::from_core(core, false))
      })
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  fn map<P>(path: P, b: WalBuidler<C, S>) -> Result<Self, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
    P: AsRef<std::path::Path>,
  {
    Self::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), b)
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  fn map_mut<P>(path: P, b: WalBuidler<C, S>, open_opts: OpenOptions) -> Result<Self, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
    P: AsRef<std::path::Path>,
  {
    Self::map_mut_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), b, open_opts)
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  fn map_with_path_builder<PB, E>(
    path_builder: PB,
    b: WalBuidler<C, S>,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let open_options = OpenOptions::default().read(true);

    let WalBuidler { opts, cmp, cks } = b;

    <Self::Allocator as Allocator>::map_with_path_builder(
      path_builder,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))
    .and_then(|arena| {
      Self::replay(arena, Options::new(), true, cmp, cks)
        .map(|core| Self::from_core(core, true))
        .map_err(Either::Right)
    })
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  fn map_mut_with_path_builder<PB, E>(
    path_builder: PB,
    b: WalBuidler<C, S>,
    open_options: OpenOptions,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let path = path_builder().map_err(Either::Left)?;

    let exist = path.exists();

    let WalBuidler { opts, cmp, cks } = b;

    <Self::Allocator as Allocator>::map_mut(
      path,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(Into::into)
    .and_then(|arena| {
      if !exist {
        <Self as sealed::WalSealed<C, S>>::new_in(arena, opts, cmp, cks)
          .map(|core| Self::from_core(core, false))
      } else {
        <Self as sealed::WalSealed<C, S>>::replay(arena, opts, false, cmp, cks)
          .map(|core| Self::from_core(core, false))
      }
    })
    .map_err(Either::Right)
  }

  /// Returns `true` if this WAL instance is read-only.
  fn read_only(&self) -> bool;

  /// Returns the number of entries in the WAL.
  fn len(&self) -> usize;

  /// Returns `true` if the WAL is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Returns the maximum key size allowed in the WAL.
  fn maximum_key_size(&self) -> u32;

  /// Returns the maximum value size allowed in the WAL.
  fn maximum_value_size(&self) -> u32;

  /// Flushes the to disk.
  fn flush(&self) -> Result<(), Error>;

  /// Flushes the to disk.
  fn flush_async(&self) -> Result<(), Error>;

  /// Returns `true` if the WAL contains the specified key.
  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;

  /// Returns an iterator over the entries in the WAL.
  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator;

  /// Returns the value associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;

  /// Get or insert a new entry into the WAL.
  fn get_or_insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    self
      .get_or_insert_with_value_builder::<()>(
        key,
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }),
      )
      .map_err(|e| e.unwrap_right())
  }

  /// Get or insert a new entry into the WAL.
  fn get_or_insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer;

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key in place.
  ///
  /// See also [`insert_with_value_builder`](Wal::insert_with_value_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  fn insert_with_key_builder<E>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
    value: &[u8],
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .check(
        kb.size() as usize,
        value.len(),
        self.maximum_key_size(),
        self.maximum_value_size(),
      )
      .map_err(Either::Right)?;

    self
      .insert_with_in::<E, ()>(
        kb,
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }),
      )
      .map_err(|e| match e {
        Among::Left(e) => Either::Left(e),
        Among::Middle(_) => unreachable!(),
        Among::Right(e) => Either::Right(e),
      })
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the value in place.
  ///
  /// See also [`insert_with_key_builder`](Wal::insert_with_key_builder) and [`insert_with_builders`](Wal::insert_with_builders).
  fn insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<(), Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .check(
        key.len(),
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
      )
      .map_err(Either::Right)?;

    self
      .insert_with_in::<(), E>(
        KeyBuilder::new(key.len() as u32, |buf| {
          buf.write(key).unwrap();
          Ok(())
        }),
        vb,
      )
      .map_err(|e| match e {
        Among::Left(_) => unreachable!(),
        Among::Middle(e) => Either::Left(e),
        Among::Right(e) => Either::Right(e),
      })
  }

  /// Inserts a key-value pair into the WAL. This method
  /// allows the caller to build the key and value in place.
  fn insert_with_builders<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Among::Right(Error::read_only()));
    }

    self
      .check(
        kb.size() as usize,
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
      )
      .map_err(Among::Right)?;

    self.insert_with_in(kb, vb)
  }

  /// Inserts a key-value pair into the WAL.
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Error::read_only());
    }

    self.check(
      key.len(),
      value.len(),
      self.maximum_key_size(),
      self.maximum_value_size(),
    )?;

    self
      .insert_with_in::<(), ()>(
        KeyBuilder::new(key.len() as u32, |buf| {
          buf.write(key).unwrap();
          Ok(())
        }),
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.write(value).unwrap();
          Ok(())
        }),
      )
      .map_err(Among::unwrap_right)
  }
}
