use dbutils::{buffer::VacantBuffer, types::MaybeStructured};
use skl::generic::Type;

use std::thread::spawn;

use crate::{
  batch::BatchEntry,
  generic::{ArenaTable, OrderWal, OrderWalReader, Reader, Writer},
  memtable::{bounded::Table, MemtableEntry},
  types::{KeyBuilder, ValueBuilder},
  Builder,
};

use super::{Person, MB};

#[cfg(feature = "std")]
fn concurrent_basic(mut w: OrderWal<ArenaTable<u32, [u8; 4]>>) {
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();

  let handles = readers.into_iter().map(|(i, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(1, &i) {
        assert_eq!(p.key(), i);
        assert_eq!(p.value(), i.to_le_bytes());
        break;
      }
    })
  });

  spawn(move || {
    for i in 0..100u32 {
      #[allow(clippy::needless_borrows_for_generic_args)]
      w.insert(1, &i, &i.to_le_bytes()).unwrap();
    }
  });

  for handle in handles {
    handle.join().unwrap();
  }
}

#[cfg(feature = "std")]
fn concurrent_one_key(mut w: OrderWal<ArenaTable<u32, [u8; 4]>>) {
  let readers = (0..100u32).map(|i| (i, w.reader())).collect::<Vec<_>>();
  let handles = readers.into_iter().map(|(_, reader)| {
    spawn(move || loop {
      if let Some(p) = reader.get(1, &1) {
        assert_eq!(p.key(), 1);
        assert_eq!(p.value(), 1u32.to_le_bytes());
        break;
      }
    })
  });

  w.insert(1, &1, &1u32.to_le_bytes()).unwrap();

  for handle in handles {
    handle.join().unwrap();
  }
}

fn apply(mut wal: OrderWal<ArenaTable<Person, String>>) -> (Person, Vec<(Person, String)>, Person) {
  const N: u32 = 5;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      MaybeStructured::from(person),
      MaybeStructured::from(val),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn apply_with_key_builder(
  mut wal: OrderWal<ArenaTable<Person, String>>,
) -> (Person, Vec<(Person, String)>, Person) {
  const N: u32 = 5;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      KeyBuilder::new(person.encoded_len(), |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf)
      }),
      MaybeStructured::from(val),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn apply_with_value_builder(
  mut wal: OrderWal<ArenaTable<Person, String>>,
) -> (Person, Vec<(Person, String)>, Person) {
  const N: u32 = 5;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      MaybeStructured::from(person),
      ValueBuilder::new(val.len(), |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes()).map(|_| val.len())
      }),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

fn apply_with_builders(
  mut wal: OrderWal<ArenaTable<Person, String>>,
) -> (Person, Vec<(Person, String)>, Person) {
  const N: u32 = 1;

  let mut batch = vec![];
  let output = (0..N)
    .map(|i| {
      (
        {
          let mut p = Person::random();
          p.id = i as u64;
          p
        },
        std::format!("My id is {i}"),
      )
        .clone()
    })
    .collect::<Vec<_>>();

  for (person, val) in output.iter() {
    batch.push(BatchEntry::insert(
      1,
      KeyBuilder::new(person.encoded_len(), |buf: &mut VacantBuffer<'_>| {
        buf.set_len(person.encoded_len());
        person.encode(buf)
      }),
      ValueBuilder::new(val.len(), |buf: &mut VacantBuffer<'_>| {
        buf.put_slice(val.as_bytes()).map(|_| val.len())
      }),
    ));
  }

  let rp1 = Person::random();
  wal.insert(1, &rp1, &"rp1".to_string()).unwrap();
  wal.apply(&mut batch).unwrap();
  let rp2 = Person::random();
  wal.insert(1, &rp2, &"rp2".to_string()).unwrap();

  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  let wal = wal.reader();
  for (p, val) in output.iter() {
    assert_eq!(wal.get(1, p).unwrap().value(), val);
  }

  assert_eq!(wal.get(1, &rp1).unwrap().value(), "rp1");
  assert_eq!(wal.get(1, &rp2).unwrap().value(), "rp2");

  (rp1, output, rp2)
}

// #[cfg(feature = "std")]
// expand_unit_tests!(
//   move "linked": OrderWalAlternativeTable<u32, [u8; 4]> [TableOptions::Linked]: Table<_, _> {
//     concurrent_basic |p, _res| {
//       let wal = unsafe { Builder::new().map::<OrderWalReaderAlternativeTable<u32, [u8; 4]>, _>(p).unwrap() };

//       for i in 0..100u32 {
//         assert!(wal.contains_key(&i));
//       }
//     },
//     concurrent_one_key |p, _res| {
//       let wal = unsafe { Builder::new().map::<OrderWalReaderAlternativeTable<u32, [u8; 4]>, _>(p).unwrap() };
//       assert!(wal.contains_key(&1));
//     },
//   }
// );

// #[cfg(feature = "std")]
// expand_unit_tests!(
//   move "linked": OrderWal<ArenaTable<Person, String>> [TableOptions::Linked]: Table<_, _> {
//     apply |p, (rp1, data, rp2)| {
//       let map = unsafe {
//         Builder::new()
//           .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
//           .unwrap()
//       };

//       for (p, val) in data {
//         assert_eq!(map.get(&p).unwrap().value(), &val);
//       }
//       assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
//       assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
//     },
//     apply_with_key_builder |p, (rp1, data, rp2)| {
//       let map = unsafe {
//         Builder::new()
//           .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
//           .unwrap()
//       };

//       for (p, val) in data {
//         assert_eq!(map.get(&p).unwrap().value(), &val);
//       }
//       assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
//       assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
//     },
//     apply_with_value_builder |p, (rp1, data, rp2)| {
//       let map = unsafe {
//         Builder::new()
//           .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
//           .unwrap()
//       };

//       for (p, val) in data {
//         assert_eq!(map.get(&p).unwrap().value(), &val);
//       }
//       assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
//       assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
//     },
//     apply_with_builders |p, (rp1, data, rp2)| {
//       let map = unsafe {
//         Builder::new()
//           .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
//           .unwrap()
//       };

//       for (p, val) in data {
//         assert_eq!(map.get(&p).unwrap().value(), &val);
//       }
//       assert_eq!(map.get(&rp1).unwrap().value(), "rp1");
//       assert_eq!(map.get(&rp2).unwrap().value(), "rp2");
//     }
//   }
// );

#[cfg(feature = "std")]
expand_unit_tests!(
  move "arena": OrderWal<ArenaTable<u32, [u8; 4]>> [Default::default()]: Table<_, _> {
    concurrent_basic |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<ArenaTable<u32, [u8; 4]>>, _>(p).unwrap() };

      for i in 0..100u32 {
        assert!(wal.contains_key(1, &i));
      }
    },
    concurrent_one_key |p, _res| {
      let wal = unsafe { Builder::new().map::<OrderWalReader<ArenaTable<u32, [u8; 4]>>, _>(p).unwrap() };
      assert!(wal.contains_key(1, &1));
    },
  }
);

expand_unit_tests!(
  move "arena": OrderWal<ArenaTable<Person, String>> [Default::default()]: Table<_, _> {
    apply |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_key_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_value_builder |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    },
    apply_with_builders |p, (rp1, data, rp2)| {
      let map = unsafe {
        Builder::new()
          .map::<OrderWalReader<ArenaTable<Person, String>>, _>(&p)
          .unwrap()
      };

      for (p, val) in data {
        assert_eq!(map.get(1, &p).unwrap().value(), &val);
      }
      assert_eq!(map.get(1, &rp1).unwrap().value(), "rp1");
      assert_eq!(map.get(1, &rp2).unwrap().value(), "rp2");
    }
  }
);
