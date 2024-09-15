/// The error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// Insufficient space in the WAL
  #[error("insufficient space in the WAL (requested: {requested}, available: {available})")]
  InsufficientSpace {
    /// The requested size
    requested: u32,
    /// The remaining size
    available: u32,
  },
  /// The key is too large.
  #[error("the key size is {size} larger than the maximum key size {maximum_key_size}")]
  KeyTooLarge {
    /// The size of the key.
    size: u32,
    /// The maximum key size.
    maximum_key_size: u32,
  },
  /// The value is too large.
  #[error("the value size is {size} larger than the maximum value size {maximum_value_size}")]
  ValueTooLarge {
    /// The size of the value.
    size: u32,
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
  /// I/O error.
  #[error("{0}")]
  IO(#[from] std::io::Error),
  /// The WAL is read-only.
  #[error("The WAL is read-only")]
  ReadOnly,
}

impl Error {
  /// Create a new `Error::InsufficientSpace` instance.
  pub(crate) const fn insufficient_space(requested: u32, available: u32) -> Self {
    Self::InsufficientSpace {
      requested,
      available,
    }
  }

  /// Create a new `Error::KeyTooLarge` instance.
  pub(crate) const fn key_too_large(size: u32, maximum_key_size: u32) -> Self {
    Self::KeyTooLarge {
      size,
      maximum_key_size,
    }
  }

  /// Create a new `Error::ValueTooLarge` instance.
  pub(crate) const fn value_too_large(size: u32, maximum_value_size: u32) -> Self {
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
      } => Self::insufficient_space(requested, available),
      _ => unreachable!(),
    }
  }

  /// Create a new corrupted error.
  #[inline]
  pub(crate) fn corrupted() -> Error {
    Self::IO(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "corrupted write-ahead log",
    ))
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
