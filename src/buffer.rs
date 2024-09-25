pub use dbutils::{buffer::VacantBuffer, builder};

macro_rules! builder_ext {
  ($($name:ident),+ $(,)?) => {
    $(
      paste::paste! {
        impl<F> $name<F> {
          #[doc = "Creates a new `" $name "` with the given size and builder closure which requires `FnOnce`."]
          #[inline]
          pub const fn once<E>(size: u32, f: F) -> Self
          where
            F: for<'a> FnOnce(&mut VacantBuffer<'a>) -> Result<(), E>,
          {
            Self { size, f }
          }
        }
      }
    )*
  };
}

builder!(
  /// A value builder for the wal, which requires the value size for accurate allocation and a closure to build the value.
  pub ValueBuilder(u32);
  /// A key builder for the wal, which requires the key size for accurate allocation and a closure to build the key.
  pub KeyBuilder(u32);
);

builder_ext!(ValueBuilder, KeyBuilder,);
