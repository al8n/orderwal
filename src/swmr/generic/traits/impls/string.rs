use std::{borrow::Cow, sync::Arc};

use super::*;

macro_rules! impls {
  ($( $(#[cfg($cfg:meta)])? $ty:ty),+ $(,)?) => {
    $(
      $(#[cfg($cfg)])?
      impl Type for $ty {
        type Ref<'a> = &'a str;
        type Error = ();

        fn encoded_len(&self) -> usize {
          self.len()
        }

        fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
          buf.copy_from_slice(self.as_bytes());
          Ok(())
        }

        fn from_slice(src: &[u8]) -> Self::Ref<'_> {
          core::str::from_utf8(src).unwrap()
        }
      }

      $(#[cfg($cfg)])?
      impl<'a> KeyRef<'a, $ty> for str {
        fn compare<Q>(&self, a: &Q) -> cmp::Ordering
        where
          Q: ?Sized + Ord + Comparable<Self>,
        {
          Comparable::compare(a, self).reverse()
        }

        fn compare_binary(a: &[u8], b: &[u8]) -> cmp::Ordering {
          a.cmp(b)
        }
      }
    )*
  };
}

impls! {
  Cow<'_, str>,
  &'static str,
  String,
  Arc<str>,
  Box<str>,
  #[cfg(feature = "smol_str")]
  ::smol_str::SmolStr,
  #[cfg(feature = "faststr")]
  ::faststr::FastStr,
}
