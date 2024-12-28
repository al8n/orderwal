mod reader;
mod wal;
mod writer;

pub use reader::OrderWalReader;
pub use writer::OrderWal;

#[cfg(all(
  test,
  any(
    all_orderwal_tests,
    test_generic_constructor,
    test_generic_insert,
    test_generic_get,
    test_generic_iters,
    test_dynamic_constructor,
    test_dynamic_insert,
    test_dynamic_get,
    test_dynamic_iters,
  )
))]
mod tests;
