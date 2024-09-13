use core::ops::Bound;

use super::*;
use tempfile::tempdir;
use wal::ImmutableWal;

const MB: usize = 1024 * 1024;

macro_rules! common_unittests {
  ($prefix:ident::$wal:ident) => {
    paste::paste! {
      #[test]
      fn test_construct_inmemory() {
        construct_inmemory::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      fn test_construct_map_anon() {
        construct_map_anon::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_construct_map_file() {
        construct_map_file::<OrderWal<Ascend, Crc32>>(stringify!($prefix));
      }

      #[test]
      fn test_construct_with_small_capacity_inmemory() {
        construct_with_small_capacity_inmemory::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      fn test_construct_with_small_capacity_map_anon() {
        construct_with_small_capacity_map_anon::<OrderWal<Ascend, Crc32>>();
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_construct_with_small_capacity_map_file() {
        construct_with_small_capacity_map_file::<OrderWal<Ascend, Crc32>>(stringify!($prefix));
      }

      #[test]
      fn test_insert_to_full_inmemory() {
        insert_to_full(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_to_full_map_anon() {
        insert_to_full(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_to_full_map_file() {
        let dir = tempdir().unwrap();
        insert_to_full(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_to_full_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_inmemory() {
        insert(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_map_anon() {
        insert(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_map_file() {
        let dir = tempdir().unwrap();
        insert(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_with_key_builder_inmemory() {
        insert_with_key_builder(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_with_key_builder_map_anon() {
        insert_with_key_builder(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_key_builder_map_file() {
        let dir = tempdir().unwrap();
        insert_with_key_builder(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_key_builder_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_with_value_builder_inmemory() {
        insert_with_value_builder(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_with_value_builder_map_anon() {
        insert_with_value_builder(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_value_builder_map_file() {
        let dir = tempdir().unwrap();
        insert_with_value_builder(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_value_builder_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_insert_with_builders_inmemory() {
        insert_with_builders(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_with_builders_map_anon() {
        insert_with_builders(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_builders_map_file() {
        let dir = tempdir().unwrap();
        insert_with_builders(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_builders_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_iter_inmemory() {
        iter(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_iter_map_anon() {
        iter(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_iter_map_file() {
        let dir = tempdir().unwrap();
        iter(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_iter_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_range() {
        range(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_range_map_anon() {
        range(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_range_map_file() {
        let dir = tempdir().unwrap();
        range(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_range_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_keys() {
        keys(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_keys_map_anon() {
        keys(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_keys_map_file() {
        let dir = tempdir().unwrap();
        keys(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_keys_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_values() {
        values(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_values_map_anon() {
        values(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_values_map_file() {
        let dir = tempdir().unwrap();
        values(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_values_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_range_keys() {
        range_keys(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_range_keys_map_anon() {
        range_keys(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_range_keys_map_file() {
        let dir = tempdir().unwrap();
        range_keys(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_range_keys_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_range_values() {
        range_values(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_range_values_map_anon() {
        range_values(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_range_values_map_file() {
        let dir = tempdir().unwrap();
        range_values(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test", stringify!($prefix), "_range_values_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_first_inmemory() {
        first(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_first_map_anon() {
        first(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_first_map_file() {
        let dir = tempdir().unwrap();
        first(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_first_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_last_inmemory() {
        last(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_last_map_anon() {
        last(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_last_map_file() {
        let dir = tempdir().unwrap();
        last(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_last_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_get_or_insert_inmemory() {
        get_or_insert(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_get_or_insert_map_anon() {
        get_or_insert(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_get_or_insert_map_file() {
        let dir = tempdir().unwrap();

        let mut wal = unsafe { OrderWal::map_mut(
          dir.path().join(concat!("test_", stringify!($prefix), "_get_or_insert_map_file")),
          Builder::new(),
          OpenOptions::new()
            .create_new(Some(MB))
            .write(true)
            .read(true),
        )
        .unwrap() };

        get_or_insert(
          &mut wal,
        );

        wal.flush().unwrap();
      }

      #[test]
      fn test_get_or_insert_with_value_builder_inmemory() {
        get_or_insert_with_value_builder(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_get_or_insert_with_value_builder_map_anon() {
        get_or_insert_with_value_builder(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_get_or_insert_with_value_builder_map_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(concat!("test_", stringify!($prefix), "_get_or_insert_with_value_builder_map_file"));
        let mut wal = unsafe { OrderWal::map_mut(
          &path,
          Builder::new(),
          OpenOptions::new()
            .create_new(Some(MB))
            .write(true)
            .read(true),
        )
        .unwrap() };
        get_or_insert_with_value_builder(
          &mut wal,
        );

        wal.flush_async().unwrap();

        assert_eq!(wal.path().unwrap(), path);
      }

      #[test]
      fn test_zero_reserved_inmemory() {
        zero_reserved(&mut OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_zero_reserved_map_anon() {
        zero_reserved(&mut OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_zero_reserved_map_file() {
        let dir = tempdir().unwrap();
        zero_reserved(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_zero_reserved_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }

      #[test]
      fn test_reserved_inmemory() {
        reserved(&mut OrderWal::new(Builder::new().with_capacity(MB).with_reserved(4)).unwrap());
      }

      #[test]
      fn test_reserved_map_anon() {
        reserved(&mut OrderWal::map_anon(Builder::new().with_capacity(MB).with_reserved(4)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_reserved_map_file() {
        let dir = tempdir().unwrap();
        reserved(
          &mut unsafe { OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_reserved_map_file")),
            Builder::new().with_reserved(4),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap() },
        );
      }
    }
  }
}

pub(crate) fn construct_inmemory<W: Wal<Ascend, Crc32>>() {
  let mut wal = W::new(Builder::new().with_capacity(MB as u32)).unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_anon<W: Wal<Ascend, Crc32>>() {
  let mut wal = W::map_anon(Builder::new().with_capacity(MB as u32)).unwrap();
  let wal = &mut wal;
  wal.insert(b"key1", b"value1").unwrap();
}

pub(crate) fn construct_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = tempdir().unwrap();
  let path = dir.path().join(format!("{prefix}_construct_map_file"));

  unsafe {
    let mut wal = W::map_mut(
      &path,
      Builder::new(),
      OpenOptions::new()
        .create_new(Some(MB as u32))
        .write(true)
        .read(true),
    )
    .unwrap();

    let wal = &mut wal;
    wal.insert(b"key1", b"value1").unwrap();
    assert_eq!(wal.get(b"key1").unwrap(), b"value1");
  }

  let wal = unsafe { W::map(&path, Builder::new()).unwrap() };
  assert_eq!(wal.get(b"key1").unwrap(), b"value1");
}

pub(crate) fn construct_with_small_capacity_inmemory<W: Wal<Ascend, Crc32>>() {
  let wal = W::new(Builder::new().with_capacity(1));

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn construct_with_small_capacity_map_anon<W: Wal<Ascend, Crc32>>() {
  let wal = W::map_anon(Builder::new().with_capacity(1));

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("error: {:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn construct_with_small_capacity_map_file<W: Wal<Ascend, Crc32>>(prefix: &str) {
  let dir = tempdir().unwrap();
  let path = dir
    .path()
    .join(format!("{prefix}_construct_with_small_capacity_map_file"));

  let wal = unsafe {
    W::map_mut(
      &path,
      Builder::new(),
      OpenOptions::new()
        .create_new(Some(1))
        .write(true)
        .read(true),
    )
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
        KeyBuilder::<_>::new(4, |buf| {
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
}

pub(crate) fn insert_with_value_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_builders<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .insert_with_builders::<(), ()>(
        KeyBuilder::<_>::new(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.put_u32_be(i);
          Ok(())
        }),
      )
      .unwrap();
  }

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
}

pub(crate) fn first<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let (key, value) = wal.first().unwrap();
  assert_eq!(key, 0u32.to_be_bytes());
  assert_eq!(value, 0u32.to_be_bytes());
}

pub(crate) fn last<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let (key, value) = wal.last().unwrap();
  assert_eq!(key, 999u32.to_be_bytes());
  assert_eq!(value, 999u32.to_be_bytes());
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
}

pub(crate) fn get_or_insert_with_value_builder<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  for i in 0..100u32 {
    wal
      .get_or_insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::new(4, |buf| {
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
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.put_u32_be(i * 2);
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..100u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn zero_reserved<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  unsafe {
    assert_eq!(wal.reserved_slice(), &[]);
    assert_eq!(wal.reserved_slice_mut(), &mut []);
  }
}

pub(crate) fn reserved<W: Wal<Ascend, Crc32>>(wal: &mut W) {
  unsafe {
    let buf = wal.reserved_slice_mut();
    buf.copy_from_slice(b"al8n");
    assert_eq!(wal.reserved_slice(), b"al8n");
    assert_eq!(wal.reserved_slice_mut(), b"al8n");
  }
}
