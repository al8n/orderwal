pub use dbutils::buffer::VacantBuffer;

macro_rules! builder {
  ($($name:ident($size:ident)),+ $(,)?) => {
    $(
      paste::paste! {
        #[doc = "A " [< $name: snake>] " builder for the wal, which requires the " [< $name: snake>] " size for accurate allocation and a closure to build the " [< $name: snake>]]
        #[derive(Copy, Clone, Debug)]
        pub struct [< $name Builder >] <F> {
          size: $size,
          f: F,
        }

        impl<F> [< $name Builder >]<F> {
          #[doc = "Creates a new `" [<$name Builder>] "` with the given size and builder closure."]
          #[inline]
          pub const fn once<E>(size: $size, f: F) -> Self
          where
            F: for<'a> FnOnce(&mut VacantBuffer<'a>) -> Result<(), E>,
          {
            Self { size, f }
          }

          #[doc = "Creates a new `" [<$name Builder>] "` with the given size and builder closure."]
          #[inline]
          pub const fn new<E>(size: $size, f: F) -> Self
          where
            F: for<'a> Fn(&mut VacantBuffer<'a>) -> Result<(), E>,
          {
            Self { size, f }
          }

          #[doc = "Returns the required" [< $name: snake>] "size."]
          #[inline]
          pub const fn size(&self) -> $size {
            self.size
          }

          #[doc = "Returns the " [< $name: snake>] "builder closure."]
          #[inline]
          pub const fn builder(&self) -> &F {
            &self.f
          }

          /// Deconstructs the value builder into the size and the builder closure.
          #[inline]
          pub fn into_components(self) -> ($size, F) {
            (self.size, self.f)
          }
        }
      }
    )*
  };
}

builder!(Value(u32), Key(u32));
