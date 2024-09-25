use among::Among;
pub use dbutils::traits::{KeyRef, Type, TypeRef};

pub(crate) trait InsertAmongExt<T: Type> {
  fn encoded_len(&self) -> usize;
  fn encode(&self, buf: &mut [u8]) -> Result<usize, T::Error>;
}

impl<T: Type> InsertAmongExt<T> for Among<T, &T, &[u8]> {
  #[inline]
  fn encoded_len(&self) -> usize {
    match self {
      Among::Left(t) => t.encoded_len(),
      Among::Middle(t) => t.encoded_len(),
      Among::Right(t) => t.len(),
    }
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, T::Error> {
    match self {
      Among::Left(t) => t.encode(buf),
      Among::Middle(t) => t.encode(buf),
      Among::Right(t) => {
        buf.copy_from_slice(t);
        Ok(buf.len())
      }
    }
  }
}
