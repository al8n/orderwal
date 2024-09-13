use super::*;

mod bytes;
pub use bytes::*;
mod string;
pub use string::Str;

impl Type for () {
  type Ref<'a> = ();
  type Error = ();

  fn encoded_len(&self) -> usize {
    0
  }

  fn encode(&self, _buf: &mut [u8]) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl TypeRef<'_> for () {
  fn from_slice(_buf: &[u8]) -> Self {}
}
