use core::ops::RangeBounds;

use super::*;

mod builder;
pub use builder::*;

pub(crate) mod sealed;

/// A batch of keys and values that can be inserted into the [`Wal`].
pub trait Batch {
  /// The key type.
  type Key: Borrow<[u8]>;

  /// The value type.
  type Value: Borrow<[u8]>;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<Item = &'a mut Entry<Self::Key, Self::Value, Self::Comparator>>
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, V, C, T> Batch for T
where
  K: Borrow<[u8]>,
  V: Borrow<[u8]>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut Entry<K, V, C>>,
{
  type Key = K;
  type Value = V;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`].
/// Comparing to [`Batch`], this trait is used to build
/// the key in place.
pub trait BatchWithKeyBuilder {
  /// The key builder type.
  type KeyBuilder: FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::Error>;

  /// The error for the key builder.
  type Error;

  /// The value type.
  type Value: Borrow<[u8]>;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithKeyBuilder<Self::KeyBuilder, Self::Value, Self::Comparator>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<KB, E, V, C, T> BatchWithKeyBuilder for T
where
  KB: FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>,
  V: Borrow<[u8]>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithKeyBuilder<KB, V, C>>,
{
  type KeyBuilder = KB;
  type Error = E;
  type Value = V;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`].
/// Comparing to [`Batch`], this trait is used to build
/// the value in place.
pub trait BatchWithValueBuilder {
  /// The value builder type.
  type ValueBuilder: FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::Error>;

  /// The error for the value builder.
  type Error;

  /// The key type.
  type Key: Borrow<[u8]>;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithValueBuilder<Self::Key, Self::ValueBuilder, Self::Comparator>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<K, VB, E, C, T> BatchWithValueBuilder for T
where
  VB: FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>,
  K: Borrow<[u8]>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithValueBuilder<K, VB, C>>,
{
  type Key = K;
  type Error = E;
  type ValueBuilder = VB;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// A batch of keys and values that can be inserted into the [`Wal`].
/// Comparing to [`Batch`], this trait is used to build
/// the key and value in place.
pub trait BatchWithBuilders {
  /// The value builder type.
  type ValueBuilder: FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::ValueError>;

  /// The error for the value builder.
  type ValueError;

  /// The value builder type.
  type KeyBuilder: FnOnce(&mut VacantBuffer<'_>) -> Result<(), Self::KeyError>;

  /// The error for the value builder.
  type KeyError;

  /// The [`Comparator`] type.
  type Comparator: Comparator;

  /// The iterator type.
  type IterMut<'a>: Iterator<
    Item = &'a mut EntryWithBuilders<Self::KeyBuilder, Self::ValueBuilder, Self::Comparator>,
  >
  where
    Self: 'a;

  /// Returns an iterator over the keys and values.
  fn iter_mut(&mut self) -> Self::IterMut<'_>;
}

impl<KB, KE, VB, VE, C, T> BatchWithBuilders for T
where
  VB: FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>,
  KB: FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>,
  C: Comparator,
  for<'a> &'a mut T: IntoIterator<Item = &'a mut EntryWithBuilders<KB, VB, C>>,
{
  type KeyBuilder = KB;
  type KeyError = KE;
  type ValueBuilder = VB;
  type ValueError = VE;
  type Comparator = C;

  type IterMut<'a> = <&'a mut T as IntoIterator>::IntoIter where Self: 'a;

  fn iter_mut(&mut self) -> Self::IterMut<'_> {
    IntoIterator::into_iter(self)
  }
}

/// An abstract layer for the immutable write-ahead log.
pub trait ImmutableWal<C, S>: sealed::Constructor<C, S> {
  /// The iterator type.
  type Iter<'a>: Iterator<Item = (&'a [u8], &'a [u8])> + DoubleEndedIterator
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of entries in the WAL.
  type Range<'a, Q, R>: Iterator<Item = (&'a [u8], &'a [u8])> + DoubleEndedIterator
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// The keys iterator type.
  type Keys<'a>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of keys in the WAL.
  type RangeKeys<'a, Q, R>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    R: RangeBounds<Q>,
    [u8]: Borrow<Q>,
    Q: Ord + ?Sized,
    Self: 'a,
    C: Comparator;

  /// The values iterator type.
  type Values<'a>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
  where
    Self: 'a,
    C: Comparator;

  /// The iterator type over a subset of values in the WAL.
  type RangeValues<'a, Q, R>: Iterator<Item = &'a [u8]> + DoubleEndedIterator
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
  unsafe fn reserved_slice<'a>(&'a self) -> &'a [u8]
  where
    Self::Allocator: 'a,
  {
    let reserved = self.options().reserved();
    if reserved == 0 {
      return &[];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice();
    &reserved_slice[HEADER_SIZE..]
  }

  /// Returns the path of the WAL if it is backed by a file.
  fn path(&self) -> Option<&std::path::Path>;

  /// Returns the number of entries in the WAL.
  fn len(&self) -> usize;

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.len() == 0
  }

  /// Returns the maximum key size allowed in the WAL.
  #[inline]
  fn maximum_key_size(&self) -> u32 {
    self.options().maximum_key_size()
  }

  /// Returns the maximum value size allowed in the WAL.
  #[inline]
  fn maximum_value_size(&self) -> u32 {
    self.options().maximum_value_size()
  }

  /// Returns the remaining capacity of the WAL.
  #[inline]
  fn remaining(&self) -> u32 {
    self.allocator().remaining() as u32
  }

  /// Returns the capacity of the WAL.
  #[inline]
  fn capacity(&self) -> u32 {
    self.options().capacity()
  }

  /// Returns the options used to create this WAL instance.
  fn options(&self) -> &Options;

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
  /// use orderwal::{swmr::OrderWal, Builder, Options, Wal};
  ///
  /// let wal = OrderWal::new(Builder::new().with_capacity(1024)).unwrap();
  /// ```
  fn new(b: Builder<C, S>) -> Result<Self, Error> {
    let Builder { opts, cmp, cks } = b;
    let arena = <Self::Allocator as Allocator>::new(
      arena_options(opts.reserved()).with_capacity(opts.capacity()),
    )
    .map_err(Error::from_insufficient_space)?;
    <Self as sealed::Constructor<C, S>>::new_in(arena, opts, cmp, cks).map(Self::from_core)
  }

  /// Creates a new in-memory write-ahead log but backed by an anonymous mmap.
  ///
  /// # Example
  ///
  /// ```rust
  /// use orderwal::{swmr::OrderWal, Builder, Wal};
  ///
  /// let wal = OrderWal::map_anon(Builder::new().with_capacity(1024)).unwrap();
  /// ```
  fn map_anon(b: Builder<C, S>) -> Result<Self, Error> {
    let Builder { opts, cmp, cks } = b;
    let mmap_opts = MmapOptions::new().len(opts.capacity());
    <Self::Allocator as Allocator>::map_anon(arena_options(opts.reserved()), mmap_opts)
      .map_err(Into::into)
      .and_then(|arena| {
        <Self as sealed::Constructor<C, S>>::new_in(arena, opts, cmp, cks).map(Self::from_core)
      })
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
  unsafe fn map<P>(path: P, b: Builder<C, S>) -> Result<Self::Reader, Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
    P: AsRef<std::path::Path>,
  {
    <Self as Wal<C, S>>::map_with_path_builder::<_, ()>(|| Ok(path.as_ref().to_path_buf()), b)
      .map_err(|e| e.unwrap_right())
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
  unsafe fn map_with_path_builder<PB, E>(
    path_builder: PB,
    b: Builder<C, S>,
  ) -> Result<Self::Reader, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let open_options = OpenOptions::default().read(true);

    let Builder { opts, cmp, cks } = b;

    <<Self::Reader as sealed::Constructor<C, S>>::Allocator as Allocator>::map_with_path_builder(
      path_builder,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(|e| e.map_right(Into::into))
    .and_then(|arena| {
      <Self::Reader as sealed::Constructor<C, S>>::replay(arena, Options::new(), true, cmp, cks)
        .map(<Self::Reader as sealed::Constructor<C, S>>::from_core)
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
  unsafe fn map_mut<P>(path: P, b: Builder<C, S>, open_opts: OpenOptions) -> Result<Self, Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
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
  ///
  /// ## Safety
  ///
  /// All file-backed memory map constructors are marked `unsafe` because of the potential for
  /// *Undefined Behavior* (UB) using the map if the underlying file is subsequently modified, in or
  /// out of process. Applications must consider the risk and take appropriate precautions when
  /// using file-backed maps. Solutions such as file permissions, locks or process-private (e.g.
  /// unlinked) files exist but are platform specific and limited.
  unsafe fn map_mut_with_path_builder<PB, E>(
    path_builder: PB,
    b: Builder<C, S>,
    open_options: OpenOptions,
  ) -> Result<Self, Either<E, Error>>
  where
    PB: FnOnce() -> Result<std::path::PathBuf, E>,
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    let path = path_builder().map_err(Either::Left)?;

    let exist = path.exists();

    let Builder { opts, cmp, cks } = b;

    <Self::Allocator as Allocator>::map_mut(
      path,
      arena_options(opts.reserved()),
      open_options,
      MmapOptions::new(),
    )
    .map_err(Into::into)
    .and_then(|arena| {
      if !exist {
        <Self as sealed::Constructor<C, S>>::new_in(arena, opts, cmp, cks).map(Self::from_core)
      } else {
        <Self as sealed::Constructor<C, S>>::replay(arena, opts, false, cmp, cks)
          .map(Self::from_core)
      }
    })
    .map_err(Either::Right)
  }

  /// Returns `true` if this WAL instance is read-only.
  fn read_only(&self) -> bool {
    self.allocator().read_only()
  }

  /// Returns the mutable reference to the reserved slice.
  ///
  /// # Safety
  /// - The caller must ensure that the there is no others accessing reserved slice for either read or write.
  /// - This method is not thread-safe, so be careful when using it.
  unsafe fn reserved_slice_mut<'a>(&'a mut self) -> &'a mut [u8]
  where
    Self::Allocator: 'a,
  {
    let reserved = sealed::Sealed::options(self).reserved();
    if reserved == 0 {
      return &mut [];
    }

    let allocator = self.allocator();
    let reserved_slice = allocator.reserved_slice_mut();
    &mut reserved_slice[HEADER_SIZE..]
  }

  /// Flushes the to disk.
  fn flush(&self) -> Result<(), Error> {
    if !self.read_only() {
      self.allocator().flush().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  /// Flushes the to disk.
  fn flush_async(&self) -> Result<(), Error> {
    if !self.read_only() {
      self.allocator().flush_async().map_err(Into::into)
    } else {
      Err(Error::read_only())
    }
  }

  /// Returns the read-only view for the WAL.
  fn reader(&self) -> Self::Reader;

  /// Get or insert a new entry into the WAL.
  fn get_or_insert(&mut self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self
      .get_or_insert_with_value_builder::<()>(
        key,
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.put_slice(value).unwrap();
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
    S: BuildChecksumer;

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
    S: BuildChecksumer,
  {
    self
      .check(
        kb.size() as usize,
        value.len(),
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

    self
      .insert_with_in::<E, ()>(
        kb,
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.put_slice(value).unwrap();
          Ok(())
        }),
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::into_left_right)
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
    S: BuildChecksumer,
  {
    self
      .check(
        key.len(),
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Either::Right)?;

    self
      .insert_with_in::<(), E>(
        KeyBuilder::new(key.len() as u32, |buf| {
          buf.put_slice(key).unwrap();
          Ok(())
        }),
        vb,
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::into_middle_right)
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
    S: BuildChecksumer,
  {
    self
      .check(
        kb.size() as usize,
        vb.size() as usize,
        self.maximum_key_size(),
        self.maximum_value_size(),
        self.read_only(),
      )
      .map_err(Among::Right)?;

    self
      .insert_with_in(kb, vb)
      .map(|ptr| self.insert_pointer(ptr))
  }

  /// Inserts a batch of key-value pairs into the WAL.
  fn insert_batch<B: Batch<Comparator = C>>(&mut self, batch: &mut B) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self
      .insert_batch_in(batch)
      .map(|_| self.insert_pointers(batch.iter_mut().map(|ent| ent.pointer.take().unwrap())))
  }

  /// Inserts a key-value pair into the WAL.
  fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>
  where
    C: Comparator + CheapClone,
    S: BuildChecksumer,
  {
    self.check(
      key.len(),
      value.len(),
      self.maximum_key_size(),
      self.maximum_value_size(),
      self.read_only(),
    )?;

    self
      .insert_with_in::<(), ()>(
        KeyBuilder::new(key.len() as u32, |buf| {
          buf.put_slice(key).unwrap();
          Ok(())
        }),
        ValueBuilder::new(value.len() as u32, |buf| {
          buf.put_slice(value).unwrap();
          Ok(())
        }),
      )
      .map(|ptr| self.insert_pointer(ptr))
      .map_err(Among::unwrap_right)
  }
}
