/// The multiple version memtable implementation.
pub mod multiple_version;
/// The memtable implementation.
pub mod table;

pub use multiple_version::MultipleVersionTable;
pub use table::Table;
