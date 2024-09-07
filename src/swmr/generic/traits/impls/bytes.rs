use std::{borrow::Cow, sync::Arc};

use super::*;

macro_rules! impls {
  ($( $(#[cfg($cfg:meta)])? $ty:ty),+ $(,)?) => {
    $(
      $(#[cfg($cfg)])?
      impl Type for $ty {
        type Ref<'a> = &'a [u8];
        type Error = ();

        fn encoded_len(&self) -> usize {
          self.len()
        }

        fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
          buf.copy_from_slice(self.as_ref());
          Ok(())
        }
      }

      $(#[cfg($cfg)])?
      impl<'a> KeyRef<'a, $ty> for [u8] {
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

impl<'a> TypeRef<'a> for &'a [u8] {
  fn from_slice(src: &'a [u8]) -> Self {
    src
  }
}

impls! {
  Cow<'_, [u8]>,
  Vec<u8>,
  Box<[u8]>,
  Arc<[u8]>,
  #[cfg(feature = "bytes")]
  ::bytes::Bytes,
}

#[cfg(feature = "smallvec")]
impl<const N: usize> Type for ::smallvec::SmallVec<[u8; N]> {
  type Ref<'a> = &'a [u8];
  type Error = ();

  fn encoded_len(&self) -> usize {
    self.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf.copy_from_slice(self.as_ref());
    Ok(())
  }
}

#[cfg(feature = "smallvec")]
impl<'a, const N: usize> KeyRef<'a, ::smallvec::SmallVec<[u8; N]>> for [u8] {
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
