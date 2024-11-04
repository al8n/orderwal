bitflags::bitflags! {
  /// The flags for each atomic write.
  pub(super) struct Flags: u8 {
    /// First bit: 1 indicates committed, 0 indicates uncommitted
    const COMMITTED = 0b00000001;
    /// Second bit: 1 indicates batching, 0 indicates single entry
    const BATCHING = 0b00000010;
  }
}

/// The kind of the Write-Ahead Log.
///
/// Currently, there are two kinds of Write-Ahead Log:
/// 1. Plain: The Write-Ahead Log is plain, which means it does not support multiple versions.
/// 2. MultipleVersion: The Write-Ahead Log supports multiple versions.
#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Kind {
  /// The Write-Ahead Log is plain, which means it does not support multiple versions.
  Plain = 0,
  /// The Write-Ahead Log supports multiple versions.
  MultipleVersion = 1,
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl TryFrom<u8> for Kind {
  type Error = crate::error::UnknownKind;

  #[inline]
  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Plain,
      1 => Self::MultipleVersion,
      _ => return Err(crate::error::UnknownKind(value)),
    })
  }
}

#[cfg(all(feature = "memmap", not(target_family = "wasm")))]
impl Kind {
  #[inline]
  pub(crate) const fn display_created_err_msg(&self) -> &'static str {
    match self {
      Self::Plain => "created without multiple versions support",
      Self::MultipleVersion => "created with multiple versions support",
    }
  }

  #[inline]
  pub(crate) const fn display_open_err_msg(&self) -> &'static str {
    match self {
      Self::Plain => "opened without multiple versions support",
      Self::MultipleVersion => "opened with multiple versions support",
    }
  }
}
