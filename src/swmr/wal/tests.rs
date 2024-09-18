use super::*;

#[cfg(all(test, any(test_swmr_constructor, all_tests)))]
mod constructor;

#[cfg(all(test, any(test_swmr_insert, all_tests)))]
mod insert;

#[cfg(all(test, any(test_swmr_iters, all_tests)))]
mod iter;

#[cfg(all(test, any(test_swmr_get, all_tests)))]
mod get;

#[cfg(all(test, any(test_swmr_insert_batch, all_tests)))]
mod insert_batch;

const MB: u32 = 1024 * 1024;
