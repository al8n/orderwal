use core::ops::Bound;

use crate::{
  dynamic::{OrderWal, Reader, Writer},
  memtable::{Entry, RawEntry as _},
};

#[cfg(feature = "bounded")]
use crate::dynamic::BoundedTable;

#[cfg(feature = "unbounded")]
use crate::dynamic::UnboundedTable;

use super::MB;

#[cfg(feature = "unbounded")]
expand_unit_tests!("unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
  unbounded_iter_with_tombstone_mvcc,
});

#[cfg(feature = "bounded")]
expand_unit_tests!("bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
  bounded_iter_with_tombstone_mvcc,
});

#[cfg(feature = "unbounded")]
expand_unit_tests!("unbounded": OrderWal<UnboundedTable> [Default::default()]: UnboundedTable {
  unbounded_iter_with_tombstone_next_by_entry,
  unbounded_iter_with_tombstone_next_by_with_tombstone_entry,
  unbounded_iter_next,
  unbounded_range_next,
  unbounded_iter_prev,
  unbounded_range_prev,
  unbounded_iter_with_tombstone_prev_by_entry,
  unbounded_iter_with_tombstone_prev_by_with_tombstone_entry,
});

macro_rules! bounded_builder {
  () => {{
    crate::Builder::new()
      .with_memtable_options(
        crate::memtable::bounded::TableOptions::new()
          .with_capacity(1024 * 1024)
          .into(),
      )
      .with_capacity(8 * 1024)
  }};
}

#[cfg(feature = "bounded")]
expand_unit_tests!("bounded": OrderWal<BoundedTable> [Default::default()]: BoundedTable {
  bounded_iter_with_tombstone_next_by_entry(bounded_builder!()),
  bounded_iter_with_tombstone_next_by_with_tombstone_entry(bounded_builder!()),
  bounded_iter_next(bounded_builder!()),
  bounded_range_next(bounded_builder!()),
  bounded_iter_prev(bounded_builder!()),
  bounded_range_prev(bounded_builder!()),
  bounded_iter_with_tombstone_prev_by_entry(bounded_builder!()),
  bounded_iter_with_tombstone_prev_by_with_tombstone_entry(bounded_builder!()),
});

fn make_int_key(i: usize) -> String {
  ::std::format!("{:05}", i)
}

fn make_value(i: usize) -> String {
  ::std::format!("v{:05}", i)
}

macro_rules! iter_with_tombstone_mvcc {
  ($wal:ident) => {{
    $wal.insert(1, b"a", b"a1").unwrap();
    $wal.insert(3, b"a", b"a2").unwrap();
    $wal.insert(1, b"c", b"c1").unwrap();
    $wal.insert(3, b"c", b"c2").unwrap();

    let mut iter = $wal.iter_all(0);
    let mut num = 0;
    while iter.next().is_some() {
      num += 1;
    }
    assert_eq!(num, 0);

    let mut iter = $wal.iter_all(1);
    let mut num = 0;
    while iter.next().is_some() {
      num += 1;
    }
    assert_eq!(num, 2);

    let mut iter = $wal.iter_all(2);
    let mut num = 0;
    while iter.next().is_some() {
      num += 1;
    }
    assert_eq!(num, 2);

    let mut iter = $wal.iter_all(3);
    let mut num = 0;
    while iter.next().is_some() {
      num += 1;
    }
    assert_eq!(num, 4);

    let upper_bound = $wal.upper_bound(1, Bound::Included(b"b")).unwrap();
    assert_eq!(upper_bound.value(), b"a1");

    let upper_bound = $wal
      .upper_bound_with_tombstone(1, Bound::Included(b"b"))
      .unwrap();
    assert_eq!(upper_bound.value().unwrap(), b"a1");

    let upper_bound = $wal.upper_bound(1, Bound::Included(b"b")).unwrap();
    assert_eq!(upper_bound.value(), b"a1");

    let upper_bound = $wal
      .upper_bound_with_tombstone(1, Bound::Included(b"b"))
      .unwrap();
    assert_eq!(upper_bound.value().unwrap(), b"a1");

    let lower_bound = $wal.lower_bound(1, Bound::Included(b"b")).unwrap();
    assert_eq!(lower_bound.value(), b"c1");

    let lower_bound = $wal
      .lower_bound_with_tombstone(1, Bound::Included(b"b"))
      .unwrap();
    assert_eq!(lower_bound.value().unwrap(), b"c1");

    let lower_bound = $wal.lower_bound(1, Bound::Included(b"b")).unwrap();
    assert_eq!(lower_bound.value(), b"c1");

    let lower_bound = $wal
      .lower_bound_with_tombstone(1, Bound::Included(b"b"))
      .unwrap();
    assert_eq!(lower_bound.value().unwrap(), b"c1");
  }};
}

fn bounded_iter_with_tombstone_mvcc(wal: &mut OrderWal<BoundedTable>) {
  iter_with_tombstone_mvcc!(wal);
}

fn unbounded_iter_with_tombstone_mvcc(wal: &mut OrderWal<UnboundedTable>) {
  iter_with_tombstone_mvcc!(wal);
}

macro_rules! iter_next {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in (0..N).rev() {
      $wal
        .insert(0, make_int_key(i).as_bytes(), make_value(i).as_bytes())
        .unwrap();
    }

    let iter = $wal.iter_all(0);

    let mut i = 0;
    for ent in iter {
      assert_eq!(ent.key(), make_int_key(i).as_bytes());
      assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
      assert_eq!(ent.value().unwrap(), make_value(i).as_bytes());
      assert_eq!(ent.raw_value().unwrap(), make_value(i).as_bytes());
      i += 1;
    }

    assert_eq!(i, N);

    let iter = $wal.iter(0);
    let mut i = 0;
    for ent in iter {
      assert_eq!(ent.key(), make_int_key(i).as_bytes());
      assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
      assert_eq!(ent.value(), make_value(i).as_bytes());
      assert_eq!(ent.raw_value(), make_value(i).as_bytes());
      i += 1;
    }

    assert_eq!(i, N);
  }};
}

fn bounded_iter_next(wal: &mut OrderWal<BoundedTable>) {
  iter_next!(wal);
}

fn unbounded_iter_next(wal: &mut OrderWal<UnboundedTable>) {
  iter_next!(wal);
}

macro_rules! iter_with_tombstone_next_by_entry {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in (0..N).rev() {
      $wal
        .insert(0, make_int_key(i).as_bytes(), make_value(i).as_bytes())
        .unwrap();
    }

    let mut ent = $wal.first(0).clone();
    #[cfg(feature = "std")]
    std::println!("{ent:?}");
    let mut i = 0;
    while let Some(ref mut entry) = ent {
      assert_eq!(entry.key(), make_int_key(i).as_bytes());
      assert_eq!(entry.value(), make_value(i).as_bytes());
      ent = entry.next();
      i += 1;
    }
    assert_eq!(i, N);

    let mut ent = $wal.iter(0).next().clone();
    #[cfg(feature = "std")]
    std::println!("{ent:?}");

    let mut i = 0;
    while let Some(ref mut entry) = ent {
      assert_eq!(entry.key(), make_int_key(i).as_bytes());
      assert_eq!(entry.value(), make_value(i).as_bytes());
      ent = entry.next();
      i += 1;
    }
    assert_eq!(i, N);
  }};
}

fn bounded_iter_with_tombstone_next_by_entry(wal: &mut OrderWal<BoundedTable>) {
  iter_with_tombstone_next_by_entry!(wal);
}

fn unbounded_iter_with_tombstone_next_by_entry(wal: &mut OrderWal<UnboundedTable>) {
  iter_with_tombstone_next_by_entry!(wal);
}

macro_rules! iter_with_tombstone_next_by_with_tombstone_entry {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in 0..N {
      let k = make_int_key(i);
      let v = make_value(i);
      $wal.insert(0, k.as_bytes(), v.as_bytes()).unwrap();
      $wal.remove(1, k.as_bytes()).unwrap();
    }

    let mut ent = $wal.first(0).clone();
    let mut i = 0;
    while let Some(ref mut entry) = ent {
      assert_eq!(entry.key(), make_int_key(i).as_bytes());
      assert_eq!(entry.value(), make_value(i).as_bytes());
      ent = entry.next();
      i += 1;
    }
    assert_eq!(i, N);

    let mut ent = $wal.first_with_tombstone(1).clone();
    #[cfg(feature = "std")]
    std::println!("{ent:?}");
    let mut i = 0;
    while let Some(ref mut entry) = ent {
      if i % 2 == 1 {
        assert_eq!(entry.version(), 0);
        assert_eq!(entry.key(), make_int_key(i / 2).as_bytes());
        assert_eq!(entry.value().unwrap(), make_value(i / 2).as_bytes());
      } else {
        assert_eq!(entry.version(), 1);
        assert_eq!(entry.key(), make_int_key(i / 2).as_bytes());
        assert!(entry.value().is_none());
      }

      ent = entry.next();
      i += 1;
    }
    assert_eq!(i, N * 2);
    let ent = $wal.first(1);
    assert!(ent.is_none());
  }};
}

fn bounded_iter_with_tombstone_next_by_with_tombstone_entry(wal: &mut OrderWal<BoundedTable>) {
  iter_with_tombstone_next_by_with_tombstone_entry!(wal);
}

fn unbounded_iter_with_tombstone_next_by_with_tombstone_entry(wal: &mut OrderWal<UnboundedTable>) {
  iter_with_tombstone_next_by_with_tombstone_entry!(wal);
}

macro_rules! range_next {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in (0..N).rev() {
      $wal
        .insert(0, make_int_key(i).as_bytes(), make_value(i).as_bytes())
        .unwrap();
    }

    let upper = make_int_key(50);
    let mut i = 0;
    let mut iter = $wal.range(0, ..=upper.as_bytes());
    for ent in &mut iter {
      assert_eq!(ent.key(), make_int_key(i).as_bytes());
      assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
      assert_eq!(ent.value(), make_value(i).as_bytes());
      assert_eq!(ent.raw_value(), make_value(i).as_bytes());
      i += 1;
    }

    assert_eq!(i, 51);

    let mut i = 0;
    let mut iter = $wal.range_all(0, ..=upper.as_bytes());
    for ent in &mut iter {
      assert_eq!(ent.key(), make_int_key(i).as_bytes());
      assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
      assert_eq!(ent.value().unwrap(), make_value(i).as_bytes());
      assert_eq!(ent.raw_value().unwrap(), make_value(i).as_bytes());
      i += 1;
    }

    assert_eq!(i, 51);
  }};
}

fn bounded_range_next(wal: &mut OrderWal<BoundedTable>) {
  range_next!(wal);
}

fn unbounded_range_next(wal: &mut OrderWal<UnboundedTable>) {
  range_next!(wal);
}

macro_rules! iter_prev {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in 0..N {
      $wal
        .insert(0, make_int_key(i).as_bytes(), make_value(i).as_bytes())
        .unwrap();
    }

    let iter = $wal.iter_all(0).rev();
    let mut i = N;
    for ent in iter {
      assert_eq!(ent.key(), make_int_key(i - 1).as_bytes());
      assert_eq!(ent.value().unwrap(), make_value(i - 1).as_bytes());
      i -= 1;
    }

    assert_eq!(i, 0);

    let iter = $wal.iter(0).rev();
    let mut i = N;
    for ent in iter {
      assert_eq!(ent.key(), make_int_key(i - 1).as_bytes());
      assert_eq!(ent.value(), make_value(i - 1).as_bytes());
      i -= 1;
    }

    assert_eq!(i, 0);
  }};
}

fn bounded_iter_prev(wal: &mut OrderWal<BoundedTable>) {
  iter_prev!(wal);
}

fn unbounded_iter_prev(wal: &mut OrderWal<UnboundedTable>) {
  iter_prev!(wal);
}

macro_rules! iter_with_tombstone_prev_by_entry {
  ($wal:ident) => {
    const N: usize = 100;

    for i in 0..N {
      $wal
        .insert(0, make_int_key(i).as_bytes(), make_value(i).as_bytes())
        .unwrap();
    }

    let mut ent = $wal.last(0);

    let mut i = 0;
    while let Some(ref mut entry) = ent {
      i += 1;
      assert_eq!(entry.key(), make_int_key(N - i).as_bytes());
      assert_eq!(entry.value(), make_value(N - i).as_bytes());
      ent = entry.prev();
    }
    assert_eq!(i, N);
  };
}

fn bounded_iter_with_tombstone_prev_by_entry(wal: &mut OrderWal<BoundedTable>) {
  iter_with_tombstone_prev_by_entry!(wal);
}

fn unbounded_iter_with_tombstone_prev_by_entry(wal: &mut OrderWal<UnboundedTable>) {
  iter_with_tombstone_prev_by_entry!(wal);
}

macro_rules! iter_with_tombstone_prev_by_with_tombstone_entry {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in 0..N {
      let k = make_int_key(i);
      let v = make_value(i);
      $wal.insert(0, k.as_bytes(), v.as_bytes()).unwrap();
      $wal.remove(1, k.as_bytes()).unwrap();
    }

    let mut ent = $wal.last(0);
    let mut i = 0;
    while let Some(ref mut entry) = ent {
      i += 1;
      assert_eq!(entry.key(), make_int_key(N - i).as_bytes());
      assert_eq!(entry.value(), make_value(N - i).as_bytes());
      ent = entry.prev();
    }
    assert_eq!(i, N);

    let mut ent = $wal.last_with_tombstone(1);
    let mut i = 0;
    while let Some(ref mut entry) = ent {
      if i % 2 == 0 {
        assert_eq!(entry.version(), 0);
        assert_eq!(entry.key(), make_int_key(N - 1 - i / 2).as_bytes());
        assert_eq!(entry.value().unwrap(), make_value(N - 1 - i / 2).as_bytes());
      } else {
        assert_eq!(entry.version(), 1);
        assert_eq!(entry.key(), make_int_key(N - 1 - i / 2).as_bytes());
        assert!(entry.value().is_none());
      }

      ent = entry.prev();
      i += 1;
    }

    assert_eq!(i, N * 2);
    let ent = $wal.last(1);
    assert!(ent.is_none());
  }};
}

fn bounded_iter_with_tombstone_prev_by_with_tombstone_entry(wal: &mut OrderWal<BoundedTable>) {
  iter_with_tombstone_prev_by_with_tombstone_entry!(wal);
}

fn unbounded_iter_with_tombstone_prev_by_with_tombstone_entry(wal: &mut OrderWal<UnboundedTable>) {
  iter_with_tombstone_prev_by_with_tombstone_entry!(wal);
}

macro_rules! range_prev {
  ($wal:ident) => {{
    const N: usize = 100;

    for i in 0..N {
      $wal
        .insert(0, make_int_key(i).as_bytes(), make_value(i).as_bytes())
        .unwrap();
    }

    let lower = make_int_key(50);
    let it = $wal.range(0, lower.as_bytes()..).rev();
    let mut i = N - 1;

    for ent in it {
      assert_eq!(ent.key(), make_int_key(i).as_bytes());
      assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
      assert_eq!(ent.value(), make_value(i).as_bytes());
      assert_eq!(ent.raw_value(), make_value(i).as_bytes());
      assert_eq!(ent.version(), 0);
      i -= 1;
    }

    assert_eq!(i, 49);

    let it = $wal.range_all(0, lower.as_bytes()..).rev();
    let mut i = N - 1;

    for ent in it {
      assert_eq!(ent.key(), make_int_key(i).as_bytes());
      assert_eq!(ent.raw_key(), make_int_key(i).as_bytes());
      assert_eq!(ent.value().unwrap(), make_value(i).as_bytes());
      assert_eq!(ent.raw_value().unwrap(), make_value(i).as_bytes());
      assert_eq!(ent.version(), 0);
      i -= 1;
    }

    assert_eq!(i, 49);
  }};
}

fn bounded_range_prev(wal: &mut OrderWal<BoundedTable>) {
  range_prev!(wal);
}

fn unbounded_range_prev(wal: &mut OrderWal<UnboundedTable>) {
  range_prev!(wal);
}
