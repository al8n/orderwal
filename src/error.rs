use among::Among;
use dbutils::error::InsufficientBuffer;
use derive_where::derive_where;

use crate::memtable::BaseTable;

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
use crate::types::Kind;

/// The batch error type.
#[derive(Debug)]
pub enum BatchError {
  /// Returned when the expected batch encoding size does not match the actual size.
  EncodedSizeMismatch {
    /// The expected size.
    expected: u32,
    /// The actual size.
    actual: u32,
  },
  /// Larger encoding size than the expected batch encoding size.
  LargerEncodedSize(u32),
}

impl core::fmt::Display for BatchError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::EncodedSizeMismatch { expected, actual } => {
        write!(
          f,
          "the expected batch encoding size ({}) does not match the actual size {}",
          expected, actual
        )
      }
      Self::LargerEncodedSize(size) => {
        write!(
          f,
          "larger encoding size than the expected batch encoding size {}",
          size
        )
      }
    }
  }
}

impl core::error::Error for BatchError {}

/// The error type.
#[derive_where(Debug; T::Error)]
pub enum Error<T: BaseTable> {
  /// Insufficient space in the WAL
  InsufficientSpace(InsufficientBuffer),
  /// Memtable does not have enough space.
  Memtable(T::Error),
  /// The key is too large.
  KeyTooLarge {
    /// The size of the key.
    size: u64,
    /// The maximum key size.
    maximum_key_size: u32,
  },
  /// The value is too large.
  ValueTooLarge {
    /// The size of the value.
    size: u64,
    /// The maximum value size.
    maximum_value_size: u32,
  },
  /// The entry is too large.
  EntryTooLarge {
    /// The size of the entry.
    size: u64,
    /// The maximum entry size.
    maximum_entry_size: u64,
  },

  /// Returned when the expected batch encoding size does not match the actual size.
  Batch(BatchError),

  /// The WAL is read-only.
  ReadOnly,

  /// Unknown WAL kind.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  UnknownKind(UnknownKind),

  /// WAL kind mismatch.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  KindMismatch {
    /// The WAL was created with this kind.
    create: Kind,
    /// Trying to open the WAL with this kind.
    open: Kind,
  },

  /// I/O error.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
  IO(std::io::Error),
}

impl<T: BaseTable> From<BatchError> for Error<T> {
  #[inline]
  fn from(e: BatchError) -> Self {
    Self::Batch(e)
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl<T: BaseTable> From<UnknownKind> for Error<T> {
  #[inline]
  fn from(e: UnknownKind) -> Self {
    Self::UnknownKind(e)
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl<T: BaseTable> From<std::io::Error> for Error<T> {
  #[inline]
  fn from(e: std::io::Error) -> Self {
    Self::IO(e)
  }
}

impl<T> core::fmt::Display for Error<T>
where
  T: BaseTable,
  T::Error: core::fmt::Display,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::InsufficientSpace(e) => write!(f, "insufficient space in the WAL: {e}"),
      Self::Memtable(e) => write!(f, "{e}"),
      Self::KeyTooLarge {
        size,
        maximum_key_size,
      } => write!(
        f,
        "the key size is {} larger than the maximum key size {}",
        size, maximum_key_size
      ),
      Self::ValueTooLarge {
        size,
        maximum_value_size,
      } => write!(
        f,
        "the value size is {} larger than the maximum value size {}",
        size, maximum_value_size
      ),
      Self::EntryTooLarge {
        size,
        maximum_entry_size,
      } => write!(
        f,
        "the entry size is {} larger than the maximum entry size {}",
        size, maximum_entry_size
      ),
      Self::Batch(e) => write!(f, "{e}"),
      Self::ReadOnly => write!(f, "The WAL is read-only"),

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      Self::UnknownKind(e) => write!(f, "{e}"),
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      Self::KindMismatch { create, open } => write!(
        f,
        "the wal was {}, cannot be {}",
        create.display_created_err_msg(),
        open.display_open_err_msg()
      ),
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      Self::IO(e) => write!(f, "{e}"),
    }
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl Kind {
  #[inline]
  const fn display_created_err_msg(&self) -> &'static str {
    match self {
      Self::Plain => "created without multiple versions support",
      Self::MultipleVersion => "created with multiple versions support",
    }
  }

  #[inline]
  const fn display_open_err_msg(&self) -> &'static str {
    match self {
      Self::Plain => "opened without multiple versions support",
      Self::MultipleVersion => "opened with multiple versions support",
    }
  }
}

impl<T> core::error::Error for Error<T>
where
  T: BaseTable,
  T::Error: core::error::Error + 'static,
{
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::InsufficientSpace(e) => Some(e),
      Self::Memtable(e) => Some(e),
      Self::KeyTooLarge { .. } => None,
      Self::ValueTooLarge { .. } => None,
      Self::EntryTooLarge { .. } => None,
      Self::Batch(e) => Some(e),
      Self::ReadOnly => None,

      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      Self::UnknownKind(e) => Some(e),
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      Self::KindMismatch { .. } => None,
      #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
      Self::IO(e) => Some(e),
    }
  }
}

impl<T: BaseTable> From<Among<InsufficientBuffer, InsufficientBuffer, Error<T>>> for Error<T> {
  #[inline]
  fn from(value: Among<InsufficientBuffer, InsufficientBuffer, Error<T>>) -> Self {
    match value {
      Among::Left(a) => Self::InsufficientSpace(a),
      Among::Middle(b) => Self::InsufficientSpace(b),
      Among::Right(c) => c,
    }
  }
}

impl<T: BaseTable> Error<T> {
  /// Create a new `Error::InsufficientSpace` instance.
  #[inline]
  pub(crate) const fn insufficient_space(requested: u64, available: u32) -> Self {
    Self::InsufficientSpace(InsufficientBuffer::with_information(
      requested,
      available as u64,
    ))
  }

  /// Create a new `Error::MemtableInsufficientSpace` instance.
  #[inline]
  pub(crate) const fn memtable(e: T::Error) -> Self {
    Self::Memtable(e)
  }

  /// Create a new `Error::KeyTooLarge` instance.
  #[inline]
  pub(crate) const fn key_too_large(size: u64, maximum_key_size: u32) -> Self {
    Self::KeyTooLarge {
      size,
      maximum_key_size,
    }
  }

  /// Create a new `Error::ValueTooLarge` instance.
  #[inline]
  pub(crate) const fn value_too_large(size: u64, maximum_value_size: u32) -> Self {
    Self::ValueTooLarge {
      size,
      maximum_value_size,
    }
  }

  /// Create a new `Error::EntryTooLarge` instance.
  #[inline]
  pub(crate) const fn entry_too_large(size: u64, maximum_entry_size: u64) -> Self {
    Self::EntryTooLarge {
      size,
      maximum_entry_size,
    }
  }

  #[inline]
  pub(crate) const fn from_insufficient_space(error: rarena_allocator::Error) -> Self {
    match error {
      rarena_allocator::Error::InsufficientSpace {
        requested,
        available,
      } => Self::insufficient_space(requested as u64, available),
      _ => unreachable!(),
    }
  }

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[inline]
  pub(crate) const fn wal_kind_mismatch(create: Kind, open: Kind) -> Self {
    Self::KindMismatch { create, open }
  }

  /// Create a new corrupted error.
  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[inline]
  pub(crate) fn corrupted<E>(e: E) -> Self
  where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
  {
    #[derive(Debug)]
    struct Corrupted(Box<dyn std::error::Error + Send + Sync>);

    impl std::fmt::Display for Corrupted {
      fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "corrupted write-ahead log: {}", self.0)
      }
    }

    impl std::error::Error for Corrupted {}

    Self::IO(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      Corrupted(e.into()),
    ))
  }

  /// Create a new batch size mismatch error.
  #[inline]
  pub(crate) const fn batch_size_mismatch(expected: u32, actual: u32) -> Self {
    Self::Batch(BatchError::EncodedSizeMismatch { expected, actual })
  }

  /// Create a new larger batch size error.
  #[inline]
  pub(crate) const fn larger_batch_size(size: u32) -> Self {
    Self::Batch(BatchError::LargerEncodedSize(size))
  }

  /// Create a read-only error.
  #[inline]
  pub(crate) const fn read_only() -> Self {
    Self::ReadOnly
  }

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[inline]
  pub(crate) fn magic_text_mismatch() -> Self {
    Self::IO(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "magic text of orderwal does not match",
    ))
  }

  #[cfg(all(feature = "memmap", not(target_family = "wasm")))]
  #[inline]
  pub(crate) fn magic_version_mismatch() -> Self {
    Self::IO(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "magic version of orderwal does not match",
    ))
  }
}

/// Unknown WAL kind error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "memmap", not(target_family = "wasm")))))]
pub struct UnknownKind(pub(super) u8);

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl core::fmt::Display for UnknownKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "unknown WAL kind: {}", self.0)
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl core::error::Error for UnknownKind {}
