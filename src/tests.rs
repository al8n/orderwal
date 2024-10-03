use core::ops::Bound;

use super::*;
use wal::{ImmutableWal, Wal};

const MB: usize = 1024 * 1024;

macro_rules! expand_unit_tests {
  ($wal:ident { $($name:ident), +$(,)? }) => {
    $(
      paste::paste! {
        #[test]
        fn [< test_ $name _inmemory >]() {
          $crate::tests::$name(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
        }

        #[test]
        fn [< test_ $name _map_anon >]() {
          $crate::tests::$name(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
        }

        #[test]
        #[cfg_attr(miri, ignore)]
        fn [< test_ $name _map_file >]() {
          let dir = ::tempfile::tempdir().unwrap();
          $crate::tests::$name(
            &mut unsafe { $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
              dir.path().join(concat!("test_", stringify!($prefix), "_", stringify!($name), "_map_file")),

            )
            .unwrap() },
          );
        }
      }
    )*
  };
}

macro_rules! common_unittests {
  ($prefix:ident::insert::$wal:ty) => {
    paste::paste! {
      #[test]
      fn test_insert_to_full_inmemory() {
        $crate::tests::insert_to_full(&mut $crate::Builder::new().with_capacity(100).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_to_full_map_anon() {
        $crate::tests::insert_to_full(&mut $crate::Builder::new().with_capacity(100).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_to_full_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::insert_to_full(
          &mut unsafe {
            $crate::Builder::new()
              .with_capacity(100)
              .with_create_new(true)
              .with_read(true)
              .with_write(true)
              .map_mut::<$wal, _>(
                dir.path().join(concat!("test_", stringify!($prefix), "_insert_to_full_map_file")),
              )
              .unwrap()
          },
        );
      }

      #[test]
      fn test_insert_inmemory() {
        $crate::tests::insert(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_map_anon() {
        $crate::tests::insert(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::insert(
          &mut unsafe { $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_map_file")),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_with_key_builder_inmemory() {
        $crate::tests::insert_with_key_builder(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_with_key_builder_map_anon() {
        $crate::tests::insert_with_key_builder(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_key_builder_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::insert_with_key_builder(
          &mut unsafe { $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_key_builder_map_file")),

          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_with_value_builder_inmemory() {
        $crate::tests::insert_with_value_builder(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_with_value_builder_map_anon() {
        $crate::tests::insert_with_value_builder(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_value_builder_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::insert_with_value_builder(
          &mut unsafe { $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_value_builder_map_file")),

          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_with_builders_inmemory() {
        $crate::tests::insert_with_builders(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_with_builders_map_anon() {
        $crate::tests::insert_with_builders(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_builders_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::insert_with_builders(
          &mut unsafe { $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_builders_map_file")),

          )
          .unwrap() },
        );
      }
    }
  };
  ($prefix:ident::insert_batch::$wal:ident) => {
    paste::paste! {
      #[test]
      fn test_insert_batch_inmemory() {
        $crate::tests::insert_batch(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_batch_map_anon() {
        $crate::tests::insert_batch(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_batch_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        let path = dir.path().join(concat!(
          "test_",
          stringify!($prefix),
          "_insert_batch_map_file"
        ));
        let mut map = unsafe {
          $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            &path,

          )
          .unwrap()
        };

        $crate::tests::insert_batch(&mut map);

        let map = unsafe { $crate::Builder::new().map::<$wal, _>(&path).unwrap() };

        for i in 0..100u32 {
          assert_eq!(map.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
        }

        assert_eq!(map.get(&1000u32.to_be_bytes()).unwrap(), 1000u32.to_be_bytes());
      }

      #[test]
      fn test_insert_batch_with_key_builder_inmemory() {
        $crate::tests::insert_batch_with_key_builder(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_batch_with_key_builder_map_anon() {
        $crate::tests::insert_batch_with_key_builder(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_batch_with_key_builder_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        let path = dir.path().join(concat!(
          "test_",
          stringify!($prefix),
          "_insert_batch_with_key_builder_map_file"
        ));
        let mut map = unsafe {
          $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            &path,

          )
          .unwrap()
        };

        $crate::tests::insert_batch_with_key_builder(&mut map);
        map.flush().unwrap();

        let map = unsafe { $crate::Builder::new().map::<$wal, _>(&path).unwrap() };

        for i in 0..100u32 {
          assert_eq!(map.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
        }
      }

      #[test]
      fn test_insert_batch_with_value_builder_inmemory() {
        $crate::tests::insert_batch_with_value_builder(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_batch_with_value_builder_map_anon() {
        $crate::tests::insert_batch_with_value_builder(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_batch_with_value_builder_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        let path = dir.path().join(concat!(
          "test_",
          stringify!($prefix),
          "_insert_batch_with_value_builder_map_file"
        ));
        let mut map = unsafe {
          $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            &path,

          )
          .unwrap()
        };

        $crate::tests::insert_batch_with_value_builder(&mut map);
        map.flush_async().unwrap();

        let map = unsafe { $crate::Builder::new().map::<$wal, _>(&path).unwrap() };

        for i in 0..100u32 {
          assert_eq!(map.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
        }
      }

      #[test]
      fn test_insert_batch_with_builders_inmemory() {
        $crate::tests::insert_batch_with_builders(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_insert_batch_with_builders_map_anon() {
        $crate::tests::insert_batch_with_builders(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_batch_with_builders_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        let path = dir.path().join(concat!(
          "test_",
          stringify!($prefix),
          "_insert_batch_with_builders_map_file"
        ));
        let mut map = unsafe {
          $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            &path,

          )
          .unwrap()
        };

        $crate::tests::insert_batch_with_builders(&mut map);

        let map = unsafe { $crate::Builder::new().map::<$wal, _>(&path).unwrap() };

        for i in 0..100u32 {
          assert_eq!(map.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
        }
      }
    }
  };
  ($prefix:ident::iters::$wal:ident) => {
    expand_unit_tests!(
      $wal {
        iter,
        range,
        keys,
        values,
        bounds,
        range_keys,
        range_values,
      }
    );
  };
  ($prefix:ident::get::$wal:ident) => {
    expand_unit_tests!(
      $wal {
        first,
        last,
        get_or_insert,
        get_or_insert_with_value_builder,
      }
    );
  };
  ($prefix:ident::constructor::$wal:ident) => {
    paste::paste! {
      #[test]
      fn test_construct_inmemory() {
        $crate::tests::construct_inmemory::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      fn test_construct_map_anon() {
        $crate::tests::construct_map_anon::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_construct_map_file() {
        $crate::tests::construct_map_file::<OrderWal<Ascend, Crc32>>(stringify!($prefix));
      }

      #[test]
      fn test_construct_with_small_capacity_inmemory() {
        $crate::tests::construct_with_small_capacity_inmemory::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      fn test_construct_with_small_capacity_map_anon() {
        $crate::tests::construct_with_small_capacity_map_anon::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_construct_with_small_capacity_map_file() {
        $crate::tests::construct_with_small_capacity_map_file::<OrderWal<Ascend, Crc32>>(stringify!($prefix));
      }

      #[test]
      fn test_zero_reserved_inmemory() {
        $crate::tests::zero_reserved(&mut $crate::Builder::new().with_capacity(MB).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_zero_reserved_map_anon() {
        $crate::tests::zero_reserved(&mut $crate::Builder::new().with_capacity(MB).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_zero_reserved_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::zero_reserved(
          &mut unsafe { $crate::Builder::new().with_create_new(true).with_read(true).with_write(true).with_capacity(MB as u32).map_mut::<$wal, _>(
            dir.path().join(concat!("test_", stringify!($prefix), "_zero_reserved_map_file")),

          )
          .unwrap() },
        );
      }

      #[test]
      fn test_reserved_inmemory() {
        $crate::tests::reserved(&mut $crate::Builder::new().with_capacity(MB).with_reserved(4).alloc::<$wal>().unwrap());
      }

      #[test]
      fn test_reserved_map_anon() {
        $crate::tests::reserved(&mut $crate::Builder::new().with_capacity(MB).with_reserved(4).map_anon::<$wal>().unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_reserved_map_file() {
        let dir = ::tempfile::tempdir().unwrap();
        $crate::tests::reserved(
          &mut unsafe { Builder::new().with_reserved(4).with_capacity(MB).with_create_new(true).with_write(true).with_read(true).map_mut::<$wal, _>(
            dir.path().join(concat!("test_", stringify!($prefix), "_reserved_map_file")),

          )
          .unwrap() },
        );
      }
    }
  }
}

pub(crate) fn construct_inmemory<W: Wal<Ascend, Crc32>>() {
  let mut wal = Builder::new()
    .with_capacity(MB as u32)
    .alloc::<W>()
    .unwrap();
  let wal = &mut wal;
  assert!(wal.is_empty());
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_anon<W: Wal<Ascend, Crc32>>() {
  let mut wal = Builder::new()
    .with_capacity(MB as u32)
    .map_anon::<W>()
    .unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = ::tempfile::tempdir().unwrap();
  let path = dir.path().join(format!("{prefix}_construct_map_file"));

  unsafe {
    let mut wal = Builder::new()
      .with_capacity(MB as u32)
      .with_create_new(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<W, _>(&path)
      .unwrap();

    let wal = &mut wal;
    wal.insert(b"key1", b"value1").unwrap();
    assert_eq!(wal.get(b"key1").unwrap(), b"value1");
  }

  unsafe {
    let wal = Builder::new()
      .with_capacity(MB as u32)
      .with_create(true)
      .with_read(true)
      .with_write(true)
      .map_mut::<W, _>(&path)
      .unwrap();

    assert_eq!(wal.get(b"key1").unwrap(), b"value1");
    assert!(!wal.read_only());
  }

  let wal = unsafe { Builder::new().map::<W, _>(&path).unwrap() };
  assert_eq!(wal.get(b"key1").unwrap(), b"value1");
  assert_eq!(wal.path().unwrap(), path);
  assert_eq!(wal.maximum_key_size(), Options::new().maximum_key_size());
  assert_eq!(
    wal.maximum_value_size(),
    Options::new().maximum_value_size()
  );
}

pub(crate) fn construct_with_small_capacity_inmemory<W: Wal<Ascend, Crc32>>() {
  let wal = Builder::new().with_capacity(1).alloc::<W>();

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn construct_with_small_capacity_map_anon<W: Wal<Ascend, Crc32>>() {
  let wal = Builder::new().with_capacity(1).map_anon::<W>();

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn construct_with_small_capacity_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = ::tempfile::tempdir().unwrap();
  let path = dir
    .path()
    .join(format!("{prefix}_construct_with_small_capacity_map_file"));

  let wal = unsafe {
    Builder::new()
      .with_capacity(1)
      .with_create_new(true)
      .with_write(true)
      .with_read(true)
      .map_mut::<W, _>(&path)
  };

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("{:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn insert_to_full<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  let mut full = false;
  for i in 0u32.. {
    match wal.insert(&i.to_be_bytes(), &i.to_be_bytes()) {
      Ok(_) => {}
      Err(e) => match e {
        Error::InsufficientSpace { .. } => {
          full = true;
          break;
        }
        _ => panic!("unexpected error"),
      },
    }
  }
  assert!(full);
}

pub(crate) fn insert<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  assert!(!wal.is_empty());
  assert_eq!(wal.len(), 100);

  for i in 0..100u32 {
    assert!(wal.contains_key(&i.to_be_bytes()));
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  assert!(!wal.is_empty());
  assert_eq!(wal.len(), 100);

  for i in 0..100u32 {
    assert!(wal.contains_key(&i.to_be_bytes()));
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_key_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .insert_with_key_builder::<()>(
        KeyBuilder::<_>::once(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
        &i.to_be_bytes(),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_value_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::once(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_builders<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .insert_with_builders::<(), ()>(
        KeyBuilder::<_>::once(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
        ValueBuilder::<_>::once(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn iter<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let mut iter = wal.iter();

  for i in 0..100u32 {
    let (key, value) = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  let mut iter = wal.iter();
  for i in (0..100u32).rev() {
    let (key, value) = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  let wal = wal.reader();
  let mut iter = wal.iter();

  for i in 0..100u32 {
    let (key, value) = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  let mut iter = wal.iter();
  for i in (0..100u32).rev() {
    let (key, value) = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }
}

pub(crate) fn bounds<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let upper50 = wal
    .upper_bound(Bound::Included(&50u32.to_be_bytes()))
    .unwrap();
  assert_eq!(upper50, 50u32.to_be_bytes());
  let upper51 = wal
    .upper_bound(Bound::Excluded(&51u32.to_be_bytes()))
    .unwrap();
  assert_eq!(upper51, 50u32.to_be_bytes());

  let upper101 = wal
    .upper_bound(Bound::Included(&101u32.to_be_bytes()))
    .unwrap();
  assert_eq!(upper101, 99u32.to_be_bytes());

  let upper_unbounded = wal.upper_bound(Bound::Unbounded).unwrap();
  assert_eq!(upper_unbounded, 99u32.to_be_bytes());

  let lower50 = wal
    .lower_bound(Bound::Included(&50u32.to_be_bytes()))
    .unwrap();
  assert_eq!(lower50, 50u32.to_be_bytes());
  let lower51 = wal
    .lower_bound(Bound::Excluded(&51u32.to_be_bytes()))
    .unwrap();
  assert_eq!(lower51, 52u32.to_be_bytes());

  let lower0 = wal
    .lower_bound(Bound::Excluded(&0u32.to_be_bytes()))
    .unwrap();
  assert_eq!(lower0, 1u32.to_be_bytes());

  let lower_unbounded = wal.lower_bound(Bound::Unbounded).unwrap();
  assert_eq!(lower_unbounded, 0u32.to_be_bytes());

  let wal = wal.reader();
  let upper50 = wal
    .upper_bound(Bound::Included(&50u32.to_be_bytes()))
    .unwrap();
  assert_eq!(upper50, 50u32.to_be_bytes());
  let upper51 = wal
    .upper_bound(Bound::Excluded(&51u32.to_be_bytes()))
    .unwrap();
  assert_eq!(upper51, 50u32.to_be_bytes());

  let upper101 = wal
    .upper_bound(Bound::Included(&101u32.to_be_bytes()))
    .unwrap();
  assert_eq!(upper101, 99u32.to_be_bytes());

  let upper_unbounded = wal.upper_bound(Bound::Unbounded).unwrap();
  assert_eq!(upper_unbounded, 99u32.to_be_bytes());

  let lower50 = wal
    .lower_bound(Bound::Included(&50u32.to_be_bytes()))
    .unwrap();
  assert_eq!(lower50, 50u32.to_be_bytes());
  let lower51 = wal
    .lower_bound(Bound::Excluded(&51u32.to_be_bytes()))
    .unwrap();
  assert_eq!(lower51, 52u32.to_be_bytes());

  let lower0 = wal
    .lower_bound(Bound::Excluded(&0u32.to_be_bytes()))
    .unwrap();
  assert_eq!(lower0, 1u32.to_be_bytes());

  let lower_unbounded = wal.lower_bound(Bound::Unbounded).unwrap();
  assert_eq!(lower_unbounded, 0u32.to_be_bytes());
}

pub(crate) fn range<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let x = 50u32.to_be_bytes();

  let mut iter = wal.range((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 50..100u32 {
    let (key, value) = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.range((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in (50..100u32).rev() {
    let (key, value) = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  let wal = wal.reader();

  let mut iter = wal.range((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 50..100u32 {
    let (key, value) = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.range((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in (50..100u32).rev() {
    let (key, value) = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }
}

pub(crate) fn keys<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let mut iter = wal.keys();

  for i in 0..100u32 {
    let key = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.keys();
  for i in (0..100u32).rev() {
    let key = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  let wal = wal.reader();
  let mut iter = wal.keys();

  for i in 0..100u32 {
    let key = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.keys();
  for i in (0..100u32).rev() {
    let key = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }
}

pub(crate) fn range_keys<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let x = 50u32.to_be_bytes();

  let mut iter = wal.range_keys((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 50..100u32 {
    let key = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.range_keys((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in (50..100u32).rev() {
    let key = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  let wal = wal.reader();
  let mut iter = wal.range_keys((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 50..100u32 {
    let key = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.range_keys((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in (50..100u32).rev() {
    let key = iter.next_back().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }
}

pub(crate) fn values<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let mut iter = wal.values();

  for i in 0..100u32 {
    let value = iter.next().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.values();
  for i in (0..100u32).rev() {
    let value = iter.next_back().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  let wal = wal.reader();
  let mut iter = wal.values();

  for i in 0..100u32 {
    let value = iter.next().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.values();
  for i in (0..100u32).rev() {
    let value = iter.next_back().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }
}

pub(crate) fn range_values<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let x = 50u32.to_be_bytes();

  let mut iter = wal.range_values((Bound::Included(x.as_slice()), Bound::Unbounded));

  for i in 50..100u32 {
    let value = iter.next().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.range_values((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in (50..100u32).rev() {
    let value = iter.next_back().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  let wal = wal.reader();
  let mut iter = wal.range_values((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 50..100u32 {
    let value = iter.next().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());

  let mut iter = wal.range_values((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in (50..100u32).rev() {
    let value = iter.next_back().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }
}

pub(crate) fn first<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let (key, value) = wal.first().unwrap();
  assert_eq!(key, 0u32.to_be_bytes());
  assert_eq!(value, 0u32.to_be_bytes());

  let wal = wal.reader();
  let (key, value) = wal.first().unwrap();
  assert_eq!(key, 0u32.to_be_bytes());
  assert_eq!(value, 0u32.to_be_bytes());
}

pub(crate) fn last<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let (key, value) = wal.last().unwrap();
  assert_eq!(key, 99u32.to_be_bytes());
  assert_eq!(value, 99u32.to_be_bytes());

  let wal = wal.reader();
  let (key, value) = wal.last().unwrap();
  assert_eq!(key, 99u32.to_be_bytes());
  assert_eq!(value, 99u32.to_be_bytes());
}

pub(crate) fn get_or_insert<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .get_or_insert(&i.to_be_bytes(), &i.to_be_bytes())
      .unwrap();
  }

  for i in 0..100u32 {
    wal
      .get_or_insert(&i.to_be_bytes(), &(i * 2).to_be_bytes())
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn get_or_insert_with_value_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .get_or_insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::once(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    wal
      .get_or_insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::once(4, |buf| {
          let _ = buf.put_u32_be(i * 2);
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_batch<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  const N: u32 = 100;

  let mut batch = vec![];

  for i in 0..N {
    batch.push(Entry::new(i.to_be_bytes(), i.to_be_bytes()));
  }

  wal.insert_batch(&mut batch).unwrap();

  wal
    .insert(&1000u32.to_be_bytes(), &1000u32.to_be_bytes())
    .unwrap();

  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  assert_eq!(
    wal.get(&1000u32.to_be_bytes()).unwrap(),
    1000u32.to_be_bytes()
  );

  let wal = wal.reader();
  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  assert_eq!(
    wal.get(&1000u32.to_be_bytes()).unwrap(),
    1000u32.to_be_bytes()
  );
}

pub(crate) fn insert_batch_with_key_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  const N: u32 = 100;

  let mut batch = vec![];

  for i in 0..N {
    batch.push(EntryWithKeyBuilder::new(
      KeyBuilder::new(4, move |buf: &mut VacantBuffer<'_>| buf.put_u32_be(i)),
      i.to_be_bytes(),
    ));
  }

  wal.insert_batch_with_key_builder(&mut batch).unwrap();

  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_batch_with_value_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  const N: u32 = 100;

  let mut batch = vec![];
  for i in 0..N {
    batch.push(EntryWithValueBuilder::new(
      i.to_be_bytes(),
      ValueBuilder::new(4, move |buf: &mut VacantBuffer<'_>| buf.put_u32_be(i)),
    ));
  }

  wal.insert_batch_with_value_builder(&mut batch).unwrap();

  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_batch_with_builders<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  const N: u32 = 100;

  let mut batch = vec![];

  for i in 0..N {
    batch.push(EntryWithBuilders::new(
      KeyBuilder::new(4, move |buf: &mut VacantBuffer<'_>| buf.put_u32_be(i)),
      ValueBuilder::new(4, move |buf: &mut VacantBuffer<'_>| buf.put_u32_be(i)),
    ));
  }

  wal.insert_batch_with_builders(&mut batch).unwrap();

  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }

  let wal = wal.reader();
  for i in 0..N {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn zero_reserved<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  unsafe {
    assert_eq!(wal.reserved_slice(), &[]);
    assert_eq!(wal.reserved_slice_mut(), &mut []);

    let reader = wal.reader();
    assert_eq!(reader.reserved_slice(), &[]);
  }
}

pub(crate) fn reserved<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  unsafe {
    let buf = wal.reserved_slice_mut();
    buf.copy_from_slice(b"al8n");
    assert_eq!(wal.reserved_slice(), b"al8n");
    assert_eq!(wal.reserved_slice_mut(), b"al8n");

    let reader = wal.reader();
    assert_eq!(reader.reserved_slice(), b"al8n");
  }
}
