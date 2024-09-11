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
      fn test_insert_inmemory() {
        insert(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_map_anon() {
        insert(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_map_file() {
        let dir = tempdir().unwrap();
        insert(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_insert_with_key_builder_inmemory() {
        insert_with_key_builder(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_with_key_builder_map_anon() {
        insert_with_key_builder(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_key_builder_map_file() {
        let dir = tempdir().unwrap();
        insert_with_key_builder(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_key_builder_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_insert_with_value_builder_inmemory() {
        insert_with_value_builder(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_with_value_builder_map_anon() {
        insert_with_value_builder(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_value_builder_map_file() {
        let dir = tempdir().unwrap();
        insert_with_value_builder(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_value_builder_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_insert_with_builders_inmemory() {
        insert_with_builders(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_insert_with_builders_map_anon() {
        insert_with_builders(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_insert_with_builders_map_file() {
        let dir = tempdir().unwrap();
        insert_with_builders(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_insert_with_builders_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_iter_inmemory() {
        iter(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_iter_map_anon() {
        iter(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_iter_map_file() {
        let dir = tempdir().unwrap();
        iter(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_iter_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_range() {
        range(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_range_map_anon() {
        range(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_range_map_file() {
        let dir = tempdir().unwrap();
        range(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_range_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_keys() {
        keys(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_keys_map_anon() {
        keys(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_keys_map_file() {
        let dir = tempdir().unwrap();
        keys(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_keys_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_values() {
        values(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_values_map_anon() {
        values(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_values_map_file() {
        let dir = tempdir().unwrap();
        values(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_values_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_range_keys() {
        range_keys(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_range_keys_map_anon() {
        range_keys(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_range_keys_map_file() {
        let dir = tempdir().unwrap();
        range_keys(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_range_keys_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_range_values() {
        range_values(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_range_values_map_anon() {
        range_values(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_range_values_map_file() {
        let dir = tempdir().unwrap();
        range_values(
          OrderWal::map_mut(
            dir.path().join(concat!("test", stringify!($prefix), "_range_values_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_first_inmemory() {
        first(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_first_map_anon() {
        first(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_first_map_file() {
        let dir = tempdir().unwrap();
        first(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_first_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_last_inmemory() {
        last(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_last_map_anon() {
        last(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_last_map_file() {
        let dir = tempdir().unwrap();
        last(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_last_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_get_or_insert_inmemory() {
        get_or_insert(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_get_or_insert_map_anon() {
        get_or_insert(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_get_or_insert_map_file() {
        let dir = tempdir().unwrap();
        get_or_insert(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_get_or_insert_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
        );
      }

      #[test]
      fn test_get_or_insert_with_value_builder_inmemory() {
        get_or_insert_with_value_builder(OrderWal::new(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      fn test_get_or_insert_with_value_builder_map_anon() {
        get_or_insert_with_value_builder(OrderWal::map_anon(Builder::new().with_capacity(MB)).unwrap());
      }

      #[test]
      #[cfg_attr(miri, ignore)]
      fn test_get_or_insert_with_value_builder_map_file() {
        let dir = tempdir().unwrap();
        get_or_insert_with_value_builder(
          OrderWal::map_mut(
            dir.path().join(concat!("test_", stringify!($prefix), "_get_or_insert_with_value_builder_map_file")),
            Builder::new(),
            OpenOptions::new()
              .create_new(Some(MB))
              .write(true)
              .read(true),
          )
          .unwrap(),
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

  {
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

  let wal = W::map(&path, Builder::new()).unwrap();
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

  let wal = W::map_mut(
    &path,
    Builder::new(),
    OpenOptions::new()
      .create_new(Some(1))
      .write(true)
      .read(true),
  );

  assert!(wal.is_err());
  match wal {
    Err(e) => println!("{:?}", e),
    _ => panic!("unexpected error"),
  }
}

pub(crate) fn insert<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  assert_eq!(wal.len(), 1000);

  for i in 0..1000u32 {
    assert!(wal.contains_key(&i.to_be_bytes()));
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_key_builder<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal
      .insert_with_key_builder::<()>(
        KeyBuilder::<_>::new(4, |buf| {
          let _ = buf.write(&i.to_be_bytes());
          Ok(())
        }),
        &i.to_be_bytes(),
      )
      .unwrap();
  }

  for i in 0..1000u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_value_builder<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal
      .insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.write(&i.to_be_bytes());
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..1000u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn insert_with_builders<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal
      .insert_with_builders::<(), ()>(
        KeyBuilder::<_>::new(4, |buf| {
          let _ = buf.write(&i.to_be_bytes());
          Ok(())
        }),
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.write(&i.to_be_bytes());
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..1000u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn iter<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let mut iter = wal.iter();
  for i in 0..1000u32 {
    let (key, value) = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }
}

pub(crate) fn range<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let x = 500u32.to_be_bytes();

  let mut iter = wal.range((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 500..1000u32 {
    let (key, value) = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());
}

pub(crate) fn keys<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let mut iter = wal.keys();
  for i in 0..1000u32 {
    let key = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }
}

pub(crate) fn range_keys<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let x = 500u32.to_be_bytes();

  let mut iter = wal.range_keys((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 500..1000u32 {
    let key = iter.next().unwrap();
    assert_eq!(key, i.to_be_bytes());
  }

  assert!(iter.next().is_none());
}

pub(crate) fn values<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let mut iter = wal.values();
  for i in 0..1000u32 {
    let value = iter.next().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }
}

pub(crate) fn range_values<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let x = 500u32.to_be_bytes();

  let mut iter = wal.range_values((Bound::Included(x.as_slice()), Bound::Unbounded));
  for i in 500..1000u32 {
    let value = iter.next().unwrap();
    assert_eq!(value, i.to_be_bytes());
  }

  assert!(iter.next().is_none());
}

pub(crate) fn first<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let (key, value) = wal.first().unwrap();
  assert_eq!(key, 0u32.to_be_bytes());
  assert_eq!(value, 0u32.to_be_bytes());
}

pub(crate) fn last<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
  }

  let (key, value) = wal.last().unwrap();
  assert_eq!(key, 999u32.to_be_bytes());
  assert_eq!(value, 999u32.to_be_bytes());
}

pub(crate) fn get_or_insert<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal
      .get_or_insert(&i.to_be_bytes(), &i.to_be_bytes())
      .unwrap();
  }

  for i in 0..1000u32 {
    wal
      .get_or_insert(&i.to_be_bytes(), &(i * 2).to_be_bytes())
      .unwrap();
  }

  for i in 0..1000u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}

pub(crate) fn get_or_insert_with_value_builder<W: Wal<Ascend, Crc32>>(mut wal: W) {
  for i in 0..1000u32 {
    wal
      .get_or_insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.write(&i.to_be_bytes());
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..1000u32 {
    wal
      .get_or_insert_with_value_builder::<()>(
        &i.to_be_bytes(),
        ValueBuilder::<_>::new(4, |buf| {
          let _ = buf.write(&(i * 2).to_be_bytes());
          Ok(())
        }),
      )
      .unwrap();
  }

  for i in 0..1000u32 {
    assert_eq!(wal.get(&i.to_be_bytes()).unwrap(), i.to_be_bytes());
  }
}
