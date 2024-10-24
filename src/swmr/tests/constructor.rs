use generic::{GenericPointer, Reader, Writer};

use crate::{memtable::Memtable, sealed::WithoutVersion};

use super::*;

fn zero_reserved<M>(wal: &mut GenericOrderWal<Person, String, M>)
where
  M: Memtable<Pointer = GenericPointer<Person, String>> + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
{
  unsafe {
    assert_eq!(wal.reserved_slice(), b"");
    assert_eq!(wal.reserved_slice_mut(), b"");

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), b"");
  }
}

fn reserved<M>(wal: &mut GenericOrderWal<Person, String, M>)
where
  M: Memtable<Pointer = GenericPointer<Person, String>> + 'static,
  M::Pointer: WithoutVersion,
  M::Error: std::fmt::Debug,
{
  unsafe {
    let buf = wal.reserved_slice_mut();
    buf.copy_from_slice(b"al8n");
    assert_eq!(wal.reserved_slice(), b"al8n");
    assert_eq!(wal.reserved_slice_mut(), b"al8n");

    let wal = wal.reader();
    assert_eq!(wal.reserved_slice(), b"al8n");
  }
}

expand_unit_tests!(
  "linked": GenericOrderWal<Person, String, LinkedTable<Person, String>> {
    zero_reserved,
    reserved,
  }
);

expand_unit_tests!(
  "arena": GenericOrderWal<Person, String, ArenaTable<Person, String>> {
    zero_reserved,
    reserved,
  }
);
