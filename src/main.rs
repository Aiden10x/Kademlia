use std::time::Duration;

use network::kademlia_client::{FindValueMessage, RpcPayload, StoreMessage};

mod network;
mod routing;
mod storage;

#[tokio::main]
async fn main() {
    // Bootstrap

    // Initialise self node
    let self_address = "127.0.0.1:8080";
    let self_id = routing::ID::random_id();
    let storage_id = routing::ID::random_id();
    println!("Starting node with ID: {}", self_id.as_hex_string());

    let client = network::KademliaClient::new(self_address, self_id.clone()).unwrap();
    let save_resp = client
        .send_request(
            RpcPayload::Store(StoreMessage {
                id: storage_id.clone(),
                value: "Hello, world!".as_bytes().to_vec(),
            }),
            "127.0.0.1:8080".parse().unwrap(),
            Duration::from_secs(1),
        )
        .await;
    println!("Save response: {:?}", save_resp);

    let retrieved = client
        .send_request(
            RpcPayload::FindValue(FindValueMessage {
                id: storage_id.clone(),
            }),
            "127.0.0.1:8080".parse().unwrap(),
            Duration::from_secs(1),
        )
        .await;

    println!("Retrieved: {:?}", retrieved);
}
