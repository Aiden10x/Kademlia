use routing::ID;

mod network;
mod routing;
mod storage;

#[tokio::main]
async fn main() {
    // Read args
    // --headless: Run without GUI
    let headless = std::env::args().any(|arg| arg == "--headless");
    let address = std::env::args()
        .skip_while(|arg| arg != "--address")
        .skip(1)
        .next()
        .unwrap_or("127.0.0.1:8080".to_string());
    // Bootstrap

    // Initialise self node
    let self_id = routing::ID::random_id();
    println!(
        "Starting node with ID: {} at {}",
        self_id.as_hex_string(),
        address
    );

    let client = network::KademliaClient::new(&address, self_id.clone()).unwrap();
    client
        .bootstrap(["127.0.0.1:8081".parse().unwrap()].to_vec())
        .await;

    if (headless) {
        for handle in client.handles {
            handle.join().unwrap();
        }
    } else {
        // Input loop
        loop {
            let mut input = String::new();
            std::io::stdin().read_line(&mut input).unwrap();
            let input = input.trim();
            // store <key> <value>
            if input.starts_with("store") {
                let mut parts = input.split_whitespace();
                parts.next();
                let key = ID::from_sha_256(parts.next().unwrap().as_bytes());
                let value = parts.next().unwrap();
                client.store(key, value.into()).await;
            } else if input.starts_with("get") {
                let mut parts = input.split_whitespace();
                parts.next();
                let key = ID::from_sha_256(parts.next().unwrap().as_bytes());
                let ret = client.get(key).await;
                println!("Value: {:?}", ret);
            } else if input.starts_with("exit") {
                break;
            }
        }
    }
}
