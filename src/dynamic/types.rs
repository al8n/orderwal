pub use dbutils::{
  buffer::{BufWriter, BufWriterOnce, VacantBuffer},
  types::*,
};

/// State of the entry.
pub trait State<'a>: sealed::Sealed<'a> {}

impl<'a, T: sealed::Sealed<'a>> State<'a> for T {}

pub use skl::{Active, MaybeTombstone};

mod sealed {
  pub trait Sealed<'a>: skl::State<'a> {}

  impl<'a, T: skl::State<'a>> Sealed<'a> for T {}
}
