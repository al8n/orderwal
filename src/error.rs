use among::Among;
use dbutils::error::InsufficientBuffer;

/// The batch error type.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
  /// Returned when the expected batch encoding size does not match the actual size.
  #[error("the expected batch encoding size ({expected}) does not match the actual size {actual}")]
  EncodedSizeMismatch {
    /// The expected size.
    expected: u32,
    /// The actual size.
    actual: u32,
  },
  /// Larger encoding size than the expected batch encoding size.
  #[error("larger encoding size than the expected batch encoding size {0}")]
  LargerEncodedSize(u32),
}

/// The error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// Insufficient space in the WAL
  #[error("insufficient space in the WAL: {0}")]
  InsufficientSpace(#[from] InsufficientBuffer),
  /// The key is too large.
  #[error("the key size is {size} larger than the maximum key size {maximum_key_size}")]
  KeyTooLarge {
    /// The size of the key.
    size: u64,
    /// The maximum key size.
    maximum_key_size: u32,
  },
  /// The value is too large.
  #[error("the value size is {size} larger than the maximum value size {maximum_value_size}")]
  ValueTooLarge {
    /// The size of the value.
    size: u64,
    /// The maximum value size.
    maximum_value_size: u32,
  },
  /// The entry is too large.
  #[error("the entry size is {size} larger than the maximum entry size {maximum_entry_size}")]
  EntryTooLarge {
    /// The size of the entry.
    size: u64,
    /// The maximum entry size.
    maximum_entry_size: u64,
  },
  /// Returned when the expected batch encoding size does not match the actual size.
  #[error(transparent)]
  Batch(#[from] BatchError),
  /// I/O error.
  #[error("{0}")]
  IO(#[from] std::io::Error),
  /// The WAL is read-only.
  #[error("The WAL is read-only")]
  ReadOnly,
}

impl From<Among<InsufficientBuffer, InsufficientBuffer, Error>> for Error {
  #[inline]
  fn from(value: Among<InsufficientBuffer, InsufficientBuffer, Error>) -> Self {
    match value {
      Among::Left(a) => Self::from(a),
      Among::Middle(b) => Self::from(b),
      Among::Right(c) => c,
    }
  }
}

impl Error {
  /// Create a new `Error::InsufficientSpace` instance.
  pub(crate) const fn insufficient_space(requested: u64, available: u32) -> Self {
    Self::InsufficientSpace(InsufficientBuffer::with_information(
      requested,
      available as u64,
    ))
  }

  /// Create a new `Error::KeyTooLarge` instance.
  pub(crate) const fn key_too_large(size: u64, maximum_key_size: u32) -> Self {
    Self::KeyTooLarge {
      size,
      maximum_key_size,
    }
  }

  /// Create a new `Error::ValueTooLarge` instance.
  pub(crate) const fn value_too_large(size: u64, maximum_value_size: u32) -> Self {
    Self::ValueTooLarge {
      size,
      maximum_value_size,
    }
  }

  /// Create a new `Error::EntryTooLarge` instance.
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

  /// Create a new corrupted error.
  #[inline]
  pub(crate) fn corrupted<E>(e: E) -> Error
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
  pub(crate) const fn read_only() -> Self {
    Self::ReadOnly
  }

  pub(crate) fn magic_text_mismatch() -> Error {
    Self::IO(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "magic text of orderwal does not match",
    ))
  }

  pub(crate) fn magic_version_mismatch() -> Error {
    Self::IO(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "magic version of orderwal does not match",
    ))
  }
}
