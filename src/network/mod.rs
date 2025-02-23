pub mod kademlia_client;

pub use kademlia_client::KademliaClient;

// Alpha represents the max number of concurrent requests
pub const ALPHA: usize = 3;
