use core::mem;

// /// The ARENA used to get key and value from the WAL by the pointer.
// pub struct Arena<A>(A);

// impl<A> Arena<A> {
//   #[inline]
//   pub(crate) const fn new(arena: A) -> Self {
//     Self(arena)
//   }
// }

// impl<A> Arena<A>
// where
//   A: rarena_allocator::Allocator,
// {
//   /// Get the key from the WAL by the pointer.
//   #[inline]
//   pub fn key(&self, kp: RecordPointer) -> &[u8] {
//     unsafe { self.0.get_bytes(kp.offset as usize, kp.len as usize) }
//   }

//   /// Get the value from the WAL by the pointer.
//   #[inline]
//   pub fn value(&self, vp: ValuePointer) -> &[u8] {
//     unsafe { self.0.get_bytes(vp.offset as usize, vp.len as usize) }
//   }

//   #[inline]
//   pub(crate) fn raw_pointer(&self) -> *const u8 {
//     self.0.raw_ptr()
//   }
// }
