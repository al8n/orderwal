use core::ops::RangeBounds;

use rarena_allocator::Error as ArenaError;

use super::*;

mod builder;
pub use builder::*;

pub(crate) mod sealed;

pub trait ImmutableWal<C, S>: sealed::Constructor<C, S> {
  /// The iterator type.
  type Iter<'a>: Iterator<Item = (&'a [u8], &'a [u8])>
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of entries in the WAL.
  type Range<'a, Q, R>: Iterator<Item = (&'a [u8], &'a [u8])>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// The keys iterator type.
  type Keys<'a>: Iterator<Item = &'a [u8]>
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of keys in the WAL.
  type RangeKeys<'a, Q, R>: Iterator<Item = &'a [u8]>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// The values iterator type.
  type Values<'a>: Iterator<Item = &'a [u8]>
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of values in the WAL.
  type RangeValues<'a, Q, R>: Iterator<Item = &'a [u8]>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// Returns the reserved space in the WAL.
  ///
  /// # Safety
  /// - The writer must ensure that the returned slice is not modified.
  /// - This method is not thread-safe, so be careful when using it.
  unsafe fn reserved_slice(&self) -> &[u8];

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

  /// Returns an iterator over a subset of entries in the WAL.
  fn range<Q, R>(&self, range: R) -> Self::Range<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator;

  /// Returns an iterator over the keys in the WAL.
  fn keys(&self) -> Self::Keys<'_>
  where
    C: Comparator;

  /// Returns an iterator over a subset of keys in the WAL.
  fn range_keys<Q, R>(&self, range: R) -> Self::RangeKeys<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator;

  /// Returns an iterator over the values in the WAL.
  fn values(&self) -> Self::Values<'_>
  where
    C: Comparator;

  /// Returns an iterator over a subset of values in the WAL.
  fn range_values<Q, R>(&self, range: R) -> Self::RangeValues<'_, Q, R>
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    C: Comparator;

  /// Returns the first key-value pair in the map. The key in this pair is the minimum key in the wal.
  fn first(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator;

  /// Returns the last key-value pair in the map. The key in this pair is the maximum key in the wal.
  fn last(&self) -> Option<(&[u8], &[u8])>
  where
    C: Comparator;

  /// Returns the value associated with the key.
  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator;
}

/// An abstract layer for the write-ahead log.
pub trait Wal<C, S>: sealed::Sealed<C, S> + ImmutableWal<C, S> {
  /// The read only reader type for this wal.
  type Reader: ImmutableWal<C, S>;

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
    )
    .map_err(|e| match e {
      ArenaError::InsufficientSpace {
        requested,
        available,
      } => Error::insufficient_space(requested, available),
      _ => unreachable!(),
    })?;
    <Self as sealed::Constructor<C, S>>::new_in(arena, opts, cmp, cks)
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
        <Self as sealed::Constructor<C, S>>::new_in(arena, opts, cmp, cks)
          .map(|core| Self::from_core(core, false))
      })
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  fn map<P>(path: P, b: WalBuidler<C, S>) -> Result<Self::Reader, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
    P: AsRef<std::path::Path>,
  {
    <Self as Wal<C, S>>::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), b)
      .map_err(|e| e.unwrap_right())
  }

  /// Opens a write-ahead log backed by a file backed memory map in read-only mode.
  fn map_with_path_builder<PB, E>(
    path_builder: PB,
    b: WalBuidler<C, S>,
  ) -> Result<Self::Reader, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let open_options = OpenOptions::default().read(true);

    let WalBuidler { opts, cmp, cks } = b;

    <<Self::Reader as sealed::Constructor<C, S>>::Allocator as Allocator>::map_with_path_builder(
      path_builder,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))
    .and_then(|arena| {
      <Self::Reader as sealed::Constructor<C, S>>::replay(arena, Options::new(), true, cmp, cks)
        .map(|core| <Self::Reader as sealed::Constructor<C, S>>::from_core(core, true))
        .map_err(Either::Right)
    })
  }

  /// Opens a write-ahead log backed by a file backed memory map.
  fn map_mut<P>(path: P, b: WalBuidler<C, S>, open_opts: OpenOptions) -> Result<Self, Error>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
    P: AsRef<std::path::Path>,
  {
    <Self as Wal<C, S>>::map_mut_with_path_builder::<_, ()>(
      || Ok(path.as_ref().to_path_buf()),
      b,
      open_opts,
    )
    .map_err(|e| e.unwrap_right())
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
        <Self as sealed::Constructor<C, S>>::new_in(arena, opts, cmp, cks)
          .map(|core| Self::from_core(core, false))
      } else {
        <Self as sealed::Constructor<C, S>>::replay(arena, opts, false, cmp, cks)
          .map(|core| Self::from_core(core, false))
      }
    })
    .map_err(Either::Right)
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// # Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  unsafe fn reserved_slice_mut(&mut self) -> &mut [u8];

  /// Flushes the to disk.
  fn flush(&self) -> Result<(), Error>;

  /// Flushes the to disk.
  fn flush_async(&self) -> Result<(), Error>;

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
