pub mod id;
pub mod routing_table;

pub use id::ID;
pub use routing_table::RoutingTable;

// Constants
pub const ID_LENGTH_BITS: usize = 256;
pub const ID_LENGTH_BYTES: usize = ID_LENGTH_BITS / 8;

// K represents the max nodes per k-bucket
pub const K: usize = 20;
