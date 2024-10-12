use core::borrow::Borrow;

use dbutils::{buffer::VacantBuffer, error::InsufficientBuffer, traits::Type};

use super::{
  types::{KeyBuilder, ValueBuilder},
  wal::generic::entry::Generic,
};

/// Writing self to the [`VacantBuffer`] in bytes format.
pub trait BufWriter {
  /// The error type.
  type Error;

  /// The length of the encoded bytes.
  fn len(&self) -> usize;

  /// Encode self to bytes and write to the [`VacantBuffer`].
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error>;
}

impl<A: Borrow<[u8]>> BufWriter for A {
  type Error = InsufficientBuffer;

  #[inline]
  fn len(&self) -> usize {
    self.borrow().len()
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    buf.put_slice(self.borrow())
  }
}

impl<T: ?Sized + Type> BufWriter for Generic<'_, T>
where
  T: Type,
{
  type Error = T::Error;

  #[inline]
  fn len(&self) -> usize {
    Generic::encoded_len(self)
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    Generic::encode_to_buffer(self, buf).map(|_| ())
  }
}

impl<W, E> BufWriter for ValueBuilder<W>
where
  W: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.builder()(buf)
  }
}

impl<W, E> BufWriter for KeyBuilder<W>
where
  W: Fn(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write(&self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.builder()(buf)
  }
}

pub trait BufWriterOnce {
  type Error;

  fn len(&self) -> usize;

  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error>;
}

impl<A: Borrow<[u8]>> BufWriterOnce for A {
  type Error = InsufficientBuffer;

  #[inline]
  fn len(&self) -> usize {
    self.borrow().len()
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    buf.put_slice(self.borrow())
  }
}

impl<T: ?Sized + Type> BufWriterOnce for Generic<'_, T>
where
  T: Type,
{
  type Error = T::Error;

  #[inline]
  fn len(&self) -> usize {
    Generic::encoded_len(self)
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    Generic::encode_to_buffer(&self, buf).map(|_| ())
  }
}

impl<W, E> BufWriterOnce for ValueBuilder<W>
where
  W: FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.into_components().1(buf)
  }
}

impl<W, E> BufWriterOnce for KeyBuilder<W>
where
  W: FnOnce(&mut VacantBuffer<'_>) -> Result<(), E>,
{
  type Error = E;

  #[inline]
  fn len(&self) -> usize {
    self.size() as usize
  }

  #[inline]
  fn write_once(self, buf: &mut VacantBuffer<'_>) -> Result<(), Self::Error> {
    self.into_components().1(buf)
  }
}
