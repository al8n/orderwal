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

impl<const N: usize> Type for [u8; N] {
  type Ref<'a> = Self;

  type Error = ();

  fn encoded_len(&self) -> usize {
    N
  }

  fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
    buf[..N].copy_from_slice(self.as_ref());
    Ok(())
  }
}

impl<const N: usize> TypeRef<'_> for [u8; N] {
  #[inline]
  fn from_slice(src: &'_ [u8]) -> Self {
    let mut this = [0; N];
    this.copy_from_slice(src);
    this
  }
}

macro_rules! impl_numbers {
  ($($ty:ident), +$(,)?) => {
    $(
      impl Type for $ty {
        type Ref<'a> = Self;

        type Error = ();

        #[inline]
        fn encoded_len(&self) -> usize {
          core::mem::size_of::<$ty>()
        }

        #[inline]
        fn encode(&self, buf: &mut [u8]) -> Result<(), Self::Error> {
          const SIZE: usize = core::mem::size_of::<$ty>();
          Ok(buf[..SIZE].copy_from_slice(self.to_le_bytes().as_ref()))
        }
      }

      impl TypeRef<'_> for $ty {
        #[inline]
        fn from_slice(buf: &[u8]) -> Self {
          const SIZE: usize = core::mem::size_of::<$ty>();

          $ty::from_le_bytes(buf[..SIZE].try_into().unwrap())
        }
      }

      impl KeyRef<'_, $ty> for $ty {
        #[inline]
        fn compare<Q>(&self, a: &Q) -> core::cmp::Ordering
        where
          Q: ?Sized + Ord + Comparable<$ty> {
          Comparable::compare(a, self).reverse()
        }

        #[inline]
        fn compare_binary(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
          const SIZE: usize = core::mem::size_of::<$ty>();

          let a = $ty::from_le_bytes(a[..SIZE].try_into().unwrap());
          let b = $ty::from_le_bytes(b[..SIZE].try_into().unwrap());

          a.cmp(&b)
        }
      }
    )*
  };
}

impl_numbers!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128,);
