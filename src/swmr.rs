mod reader;
mod wal;
mod writer;

pub use reader::OrderWalReader;
pub use writer::OrderWal;

// #[cfg(all(
//   test,
//   any(
//     all_orderwal_tests,
//     test_swmr_constructor,
//     test_swmr_insert,
//     test_swmr_get,
//     test_swmr_iters,
//   )
// ))]
#[cfg(test)]
mod tests;
