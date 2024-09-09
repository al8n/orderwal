use super::super::*;

use among::Among;
use either::Either;
use error::Error;
use sealed::{Base, WalCore, WalSealed};

use core::ptr::NonNull;
use rarena_allocator::{sync::Arena, Error as ArenaError};
use std::sync::Arc;

mod reader;
pub use reader::*;

mod iter;
pub use iter::*;

mod entry;
pub use entry::*;

#[cfg(test)]
mod tests;

pub struct OrderWalCore<C, S> {
  arena: Arena,
  map: SkipSet<Pointer<C>>,
  opts: Options,
  cmp: C,
  cks: UnsafeCellChecksumer<S>,
}

impl<C: Comparator, S> OrderWalCore<C, S> {
  #[inline]
  fn iter(&self) -> Iter<C> {
    Iter::new(self.map.iter())
  }
}

impl<C: Send + 'static> Base<C> for SkipSet<Pointer<C>> {
  fn insert(&mut self, ele: Pointer<C>)
  where
    C: Comparator,
  {
    SkipSet::insert(self, ele);
  }
}

impl<C: Send + 'static, S> WalCore<C, S> for OrderWalCore<C, S> {
  type Allocator = Arena;
  type Base = SkipSet<Pointer<C>>;

  #[inline]
  fn construct(
    arena: Arena,
    set: SkipSet<Pointer<C>>,
    opts: Options,
    cmp: C,
    checksumer: S,
  ) -> Self {
    Self {
      arena,
      map: set,
      cmp,
      opts,
      cks: UnsafeCellChecksumer::new(checksumer),
    }
  }
}

/// A single writer multiple readers ordered write-ahead log implementation.
///
/// Only the first instance of the WAL can write to the log, while the rest can only read from the log.
// ```text
// +----------------------+-------------------------+--------------------+
// | magic text (6 bytes) | magic version (2 bytes) |  header (8 bytes)  |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------|--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |     flag (1 byte)    |    key len (4 bytes)    |    key (n bytes)   | value len (4 bytes) | value (n bytes) | checksum (8 bytes) |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// |         ...          |            ...          |         ...        |          ...        |        ...      |         ...        |
// +----------------------+-------------------------+--------------------+---------------------+-----------------+--------------------+
// ```
pub struct OrderWal<C = Ascend, S = Crc32> {
  core: Arc<OrderWalCore<C, S>>,
  ro: bool,
  _s: PhantomData<S>,
}

impl<C, S> OrderWal<C, S> {
  /// Returns a read-only view of the WAL.
  #[inline]
  pub fn reader(&self) -> OrderWalReader<C, S> {
    OrderWalReader::new(self.core.clone())
  }
}

impl<C, S> WalSealed<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  type Allocator = Arena;
  type Core = OrderWalCore<C, S>;

  #[inline]
  fn from_core(core: Self::Core, ro: bool) -> Self {
    Self {
      core: Arc::new(core),
      ro,
      _s: PhantomData,
    }
  }

  #[inline]
  fn check(&self, klen: usize, vlen: usize) -> Result<(), error::Error> {
    let elen = klen as u64 + vlen as u64;

    if self.core.opts.maximum_key_size < klen as u32 {
      return Err(error::Error::key_too_large(
        klen as u32,
        self.core.opts.maximum_key_size,
      ));
    }

    if self.core.opts.maximum_value_size < vlen as u32 {
      return Err(error::Error::value_too_large(
        vlen as u32,
        self.core.opts.maximum_value_size,
      ));
    }

    if elen + FIXED_RECORD_SIZE as u64 > u32::MAX as u64 {
      return Err(error::Error::entry_too_large(
        elen,
        min_u64(
          self.core.opts.maximum_key_size as u64 + self.core.opts.maximum_value_size as u64,
          u32::MAX as u64,
        ),
      ));
    }

    Ok(())
  }

  fn insert_with_in<KE, VE>(
    &mut self,
    kb: KeyBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), KE>>,
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), VE>>,
  ) -> Result<(), Among<KE, VE, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    let (klen, kf) = kb.into_components();
    let (vlen, vf) = vb.into_components();
    let (len_size, kvlen, elen) = entry_size(klen, vlen);
    let klen = klen as usize;
    let vlen = vlen as usize;
    let buf = self.core.arena.alloc_bytes(elen);

    match buf {
      Err(e) => {
        let e = match e {
          ArenaError::InsufficientSpace {
            requested,
            available,
          } => error::Error::insufficient_space(requested, available),
          ArenaError::ReadOnly => error::Error::read_only(),
          _ => unreachable!(),
        };
        Err(Among::Right(e))
      }
      Ok(mut buf) => {
        unsafe {
          // We allocate the buffer with the exact size, so it's safe to write to the buffer.
          let flag = Flags::COMMITTED.bits();

          self.core.cks.reset();
          self.core.cks.update(&[flag]);

          buf.put_u8_unchecked(Flags::empty().bits());
          let written = buf.put_u64_varint_unchecked(kvlen);
          debug_assert_eq!(
            written, len_size,
            "the precalculated size should be equal to the written size"
          );

          let ko = STATUS_SIZE + written;
          buf.set_len(ko + klen + vlen);

          kf(&mut VacantBuffer::new(
            klen,
            NonNull::new_unchecked(buf.as_mut_ptr().add(ko)),
          ))
          .map_err(Among::Left)?;

          let vo = ko + klen;
          vf(&mut VacantBuffer::new(
            vlen,
            NonNull::new_unchecked(buf.as_mut_ptr().add(vo)),
          ))
          .map_err(Among::Middle)?;

          let cks = {
            self.core.cks.update(&buf[1..]);
            self.core.cks.digest()
          };
          buf.put_u64_le_unchecked(cks);

          // commit the entry
          buf[0] |= Flags::COMMITTED.bits();

          if self.core.opts.sync_on_write && self.core.arena.is_ondisk() {
            self
              .core
              .arena
              .flush_range(buf.offset(), elen as usize)
              .map_err(|e| Among::Right(e.into()))?;
          }
          buf.detach();
          self.core.map.insert(Pointer::new(
            klen,
            vlen,
            buf.as_ptr().add(ko),
            self.core.cmp.cheap_clone(),
          ));
          Ok(())
        }
      }
    }
  }
}

impl<C, S> Wal<C, S> for OrderWal<C, S>
where
  C: Send + 'static,
{
  type Iter<'a> = Iter<'a, C> where Self: 'a;

  #[inline]
  fn read_only(&self) -> bool {
    self.ro
  }

  /// Returns the number of entries in the WAL.
  #[inline]
  fn len(&self) -> usize {
    self.core.map.len()
  }

  /// Returns `true` if the WAL is empty.
  #[inline]
  fn is_empty(&self) -> bool {
    self.core.map.is_empty()
  }

  fn flush(&self) -> Result<(), Error> {
    if self.ro {
      return Err(error::Error::read_only());
    }

    self.core.arena.flush().map_err(Into::into)
  }

  fn flush_async(&self) -> Result<(), Error> {
    if self.ro {
      return Err(error::Error::read_only());
    }

    self.core.arena.flush_async().map_err(Into::into)
  }

  fn contains_key<Q>(&self, key: &Q) -> bool
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.core.map.contains(key)
  }

  fn iter(&self) -> Self::Iter<'_>
  where
    C: Comparator,
  {
    self.core.iter()
  }

  fn get<Q>(&self, key: &Q) -> Option<&[u8]>
  where
    [u8]: Borrow<Q>,
    Q: ?Sized + Ord,
    C: Comparator,
  {
    self.core.map.get(key).map(|ent| ent.as_value_slice())
  }

  fn get_or_insert_with_value_builder<E>(
    &mut self,
    key: &[u8],
    vb: ValueBuilder<impl FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>>,
  ) -> Result<Option<&[u8]>, Either<E, Error>>
  where
    C: Comparator + CheapClone,
    S: Checksumer,
  {
    if self.read_only() {
      return Err(Either::Right(Error::read_only()));
    }

    self
      .check(key.len(), vb.size() as usize)
      .map_err(Either::Right)?;

    if let Some(ent) = self.core.map.get(key) {
      return Ok(Some(ent.as_value_slice()));
    }

    self.insert_with_value_builder::<E>(key, vb).map(|_| None)
  }
}
