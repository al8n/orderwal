# Rleases

## 0.5.0 (Oct 27th, 2024)

- Refactor the project to make all of the WALs based on the generic implementation.
- Support different memtables based on [`crossbeam-skiplist`](https://github.com/crossbeam-rs/crossbeam) or [`skl`](https://github.com/al8n/skl)
- More user-friendly APIs

## 0.4.0 (Sep 30th, 2024)

FEATURES

- Support `K: ?Sized` and `V: ?Sized` for `OrderWal`.
- Use `flush_header_and_range` instead of `flush_range` when insertion.

## 0.1.0 (Sep 14th, 2024)

- Publish version `0.1.0`
