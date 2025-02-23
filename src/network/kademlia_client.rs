use crate::routing::routing_table::Peer;
use crate::{routing, storage};
use crossbeam_channel::{unbounded, Sender};
use dashmap::DashMap;
use futures::future;
use num_cpus;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{cmp, io};
use tokio::sync::oneshot;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PingMessage {
    pub id: routing::ID,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PingResponse {
    pub id: routing::ID,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FindNodeMessage {
    pub id: routing::ID,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FindNodeResponse {
    pub id: routing::ID,
    pub nodes: Vec<Peer>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoreMessage {
    pub id: routing::ID,
    pub value: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoreResponse {
    pub id: routing::ID,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FindValueMessage {
    pub id: routing::ID,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FindValueResponse {
    pub id: routing::ID,
    pub value: Option<Vec<u8>>,
    pub nodes: Option<Vec<Peer>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RpcPayload {
    Ping(PingMessage),
    PingResponse(PingResponse),
    FindNode(FindNodeMessage),
    FindNodeResponse(FindNodeResponse),
    Store(StoreMessage),
    StoreResponse(StoreResponse),
    FindValue(FindValueMessage),
    FindValueResponse(FindValueResponse),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RpcMessage {
    pub transaction_id: u64,
    pub payload: RpcPayload,
}

pub struct KademliaClient {
    sender: Sender<(RpcMessage, SocketAddr)>,
    pending_requests: Arc<DashMap<u64, oneshot::Sender<RpcMessage>>>,
    next_transaction_id: Arc<AtomicU64>,
    routing_table: Arc<routing::RoutingTable>,
}

impl KademliaClient {
    pub fn new(bind_addr: &str, self_id: routing::ID) -> std::io::Result<Self> {
        let routing_table = Arc::new(routing::RoutingTable::new(&self_id));
        let storage = Arc::new(RwLock::new(storage::Storage::new()));
        let pending_requests = Arc::new(DashMap::<u64, oneshot::Sender<RpcMessage>>::new());
        let next_transaction_id = Arc::new(AtomicU64::new(0));

        let socket = UdpSocket::bind(bind_addr)?;
        let (sender_tx, sender_rx) = unbounded();
        let (receiver_tx, receiver_rx) = unbounded();

        // Clone sockets for threads
        let receiver_socket = socket.try_clone()?;
        let sender_socket = socket.try_clone()?;

        // Receiver thread
        let receiver = std::thread::spawn(move || {
            let mut buf = [0u8; 1024];
            loop {
                match receiver_socket.recv_from(&mut buf) {
                    Ok((size, addr)) => {
                        if let Ok(msg) = bincode::deserialize::<RpcMessage>(&buf[..size]) {
                            if receiver_tx.send((msg, addr)).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => eprintln!("Receive error: {}", e),
                }
            }
        });

        // Sender thread
        let sender = std::thread::spawn(move || loop {
            match sender_rx.recv() {
                Ok((msg, addr)) => {
                    if let Ok(data) = bincode::serialize(&msg) {
                        let _ = sender_socket.send_to(&data, addr);
                    }
                }
                Err(_) => break,
            }
        });

        // Worker threads (minus 2 for receiver and sender threads)
        let num_workers = cmp::max(num_cpus::get() - 2, 1);

        for _ in 0..num_workers {
            let receiver = receiver_rx.clone();
            let sender = sender_tx.clone();
            let rt = Arc::clone(&routing_table);
            let storage = Arc::clone(&storage);
            let pending_requests = Arc::clone(&pending_requests);

            std::thread::spawn(move || {
                while let Ok((msg, addr)) = receiver.recv() {
                    // Fast path: Handle responses without blocking
                    if is_response(&msg.payload) {
                        // Lock-free check first
                        // TODO: make sure this doesn't block
                        if let Some((_, tx)) = pending_requests.remove(&msg.transaction_id) {
                            let _ = tx.send(msg.clone());
                        }
                    }

                    if let Some(response) = handle_rpc_message(addr, msg, &rt, &storage) {
                        let _ = sender.send((response, addr));
                    }
                }
            });
        }

        Ok(Self {
            sender: sender_tx,
            pending_requests,
            next_transaction_id,
            routing_table,
        })
    }

    pub fn send(
        &self,
        payload: RpcPayload,
        addr: SocketAddr,
    ) -> Result<u64, crossbeam_channel::SendError<(RpcMessage, SocketAddr)>> {
        let transaction_id = self.next_transaction_id.fetch_add(1, Ordering::SeqCst);
        let msg = RpcMessage {
            transaction_id,
            payload,
        };
        self.sender.send((msg, addr)).map(|_| transaction_id)
    }

    pub async fn send_request(
        &self,
        payload: RpcPayload,
        addr: SocketAddr,
        timeout: Duration,
    ) -> io::Result<RpcMessage> {
        let (tx, rx) = oneshot::channel();

        let transaction_id = self.send(payload, addr).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to send request: {}", e),
            )
        })?;

        // Add to pending requests
        self.pending_requests.insert(transaction_id, tx);

        // Wait for the response with timeout
        tokio::select! {
            res = rx => res.map_err(|_| io::Error::new(io::ErrorKind::Other, "Response channel closed")),
            _ = tokio::time::sleep(timeout) => {
                self.pending_requests.remove(&transaction_id);
                Err(io::Error::new(io::ErrorKind::TimedOut, "Request timed out"))
            }
        }
    }

    pub async fn bootstrap(&self, known_hosts: Vec<SocketAddr>) {
        // Ping all known hosts
        futures::future::join_all(
            known_hosts
                .iter()
                .map(|addr| async {
                    self.send_request(
                        RpcPayload::Ping(PingMessage {
                            id: self.routing_table.id.clone(),
                        }),
                        *addr,
                        Duration::from_secs(1),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await;
        // Self lookup
        let _ = self.lookup(&self.routing_table.id).await;
    }

    pub async fn store(&self, key: routing::ID, value: Vec<u8>) -> bool {
        let nodes = self.lookup(&key).await;
        let payload = RpcPayload::Store(StoreMessage {
            id: key.clone(),
            value: value.clone(),
        });

        let futures = nodes.iter().map(|node| {
            self.send_request(
                payload.clone(),
                node.address.parse().unwrap(),
                Duration::from_secs(1),
            )
        });

        let responses = future::join_all(futures).await;
        for response in responses {
            match response {
                Ok(msg) => {
                    if let RpcPayload::StoreResponse(store_response) = msg.payload {
                        if store_response.success {
                            return true;
                        }
                    }
                }
                Err(_) => (),
            }
        }

        return false;
    }

    pub async fn get(&self, key: routing::ID) -> Option<Vec<u8>> {
        let nodes = self.lookup(&key).await;
        let payload = RpcPayload::FindValue(FindValueMessage { id: key.clone() });

        let futures = nodes.iter().map(|node| {
            self.send_request(
                payload.clone(),
                node.address.parse().unwrap(),
                Duration::from_secs(1),
            )
        });

        let responses = future::join_all(futures).await;
        for response in responses {
            match response {
                Ok(msg) => {
                    if let RpcPayload::FindValueResponse(find_value_response) = msg.payload {
                        if let Some(value) = find_value_response.value {
                            return Some(value);
                        }
                    }
                }
                Err(_) => (),
            }
        }

        return None;
    }

    async fn lookup(&self, target_id: &routing::ID) -> Vec<Peer> {
        let all_nodes = self.routing_table.get_closest_k(&target_id);
        let queried: std::collections::HashSet<Peer> = std::collections::HashSet::new();
        return self.lookup_recursive(target_id, all_nodes, queried).await;
    }

    async fn lookup_recursive(
        &self,
        target_id: &routing::ID,
        mut all_nodes: Vec<Peer>,
        mut queried: std::collections::HashSet<Peer>,
    ) -> Vec<Peer> {
        // Sort nodes by distance to the TARGET ID
        all_nodes.sort_by(|a, b| {
            a.id.distance(target_id)
                .value
                .cmp(&b.id.distance(target_id).value)
        });

        let candidates = all_nodes
            .iter()
            .filter(|node| !queried.contains(node))
            .take(super::ALPHA)
            .cloned()
            .collect::<Vec<Peer>>();

        if candidates.is_empty() {
            // Return at max K closest nodes
            return all_nodes[0..cmp::min(routing::K, all_nodes.len())].to_vec();
        }

        let futures = candidates.iter().map(|node| {
            return self.send_request(
                RpcPayload::FindNode(FindNodeMessage {
                    id: node.id.clone(),
                }),
                node.address.parse().unwrap(),
                Duration::from_secs(1),
            );
        });

        let responses = futures::future::join_all(futures).await;
        for node in candidates {
            queried.insert(node);
        }

        for response in responses {
            match response {
                Ok(msg) => {
                    if let RpcPayload::FindNodeResponse(find_node_response) = msg.payload {
                        all_nodes.extend(find_node_response.nodes);
                    }
                }
                Err(e) => (),
            }
        }

        return Box::pin(self.lookup_recursive(target_id, all_nodes, queried)).await;
    }
}

fn handle_rpc_message(
    addr: SocketAddr,
    msg: RpcMessage,
    rt: &routing::RoutingTable,
    storage: &Arc<RwLock<storage::Storage>>,
) -> Option<RpcMessage> {
    match msg.payload {
        RpcPayload::Ping(ping_message) => Some(RpcMessage {
            transaction_id: msg.transaction_id,
            payload: handle_ping_message(addr, ping_message, rt)
                .map(RpcPayload::PingResponse)
                .unwrap(),
        }),
        RpcPayload::PingResponse(ping_response) => {
            handle_ping_response(addr, ping_response, rt);
            None
        }
        RpcPayload::FindNode(find_node_message) => Some(RpcMessage {
            transaction_id: msg.transaction_id,
            payload: handle_find_node_message(addr, find_node_message, rt)
                .map(RpcPayload::FindNodeResponse)
                .unwrap(),
        }),
        RpcPayload::FindNodeResponse(find_node_response) => {
            handle_find_node_response(addr, find_node_response, rt);
            None
        }
        RpcPayload::Store(store_message) => handle_store_message(addr, store_message, rt, storage)
            .map(|store_response| RpcMessage {
                transaction_id: msg.transaction_id,
                payload: RpcPayload::StoreResponse(store_response),
            }),
        RpcPayload::StoreResponse(store_response) => {
            handle_store_response(addr, store_response, rt);
            None
        }
        RpcPayload::FindValue(find_value_message) => Some(RpcMessage {
            transaction_id: msg.transaction_id,
            payload: handle_find_value_message(addr, find_value_message, rt, storage)
                .map(RpcPayload::FindValueResponse)
                .unwrap(),
        }),
        RpcPayload::FindValueResponse(find_value_response) => {
            handel_find_value_response(addr, find_value_response, rt);
            None
        }
        _ => None,
    }
}

fn is_response(payload: &RpcPayload) -> bool {
    matches!(
        payload,
        RpcPayload::PingResponse(_)
            | RpcPayload::FindNodeResponse(_)
            | RpcPayload::StoreResponse(_)
            | RpcPayload::FindValueResponse(_)
    )
}

fn handle_ping_message(
    addr: SocketAddr,
    msg: PingMessage,
    rt: &routing::RoutingTable,
) -> Option<PingResponse> {
    rt.add(&Peer {
        id: msg.id.clone(),
        address: addr.to_string(),
    })
    .ok(); // TODO: Handle error
    Some(PingResponse { id: rt.id.clone() })
}

fn handle_ping_response(addr: SocketAddr, msg: PingResponse, rt: &routing::RoutingTable) {
    rt.add(&Peer {
        id: msg.id.clone(),
        address: addr.to_string(),
    })
    .ok(); // TODO: Handle error
}

fn handle_find_node_message(
    addr: SocketAddr,
    msg: FindNodeMessage,
    rt: &routing::RoutingTable,
) -> Option<FindNodeResponse> {
    rt.add(&Peer {
        id: msg.id.clone(),
        address: addr.to_string(),
    })
    .ok(); // TODO: Handle error
    let nodes = rt.get_closest_k(&msg.id);
    Some(FindNodeResponse {
        id: rt.id.clone(),
        nodes,
    })
}

fn handle_find_node_response(_: SocketAddr, msg: FindNodeResponse, rt: &routing::RoutingTable) {
    for node in msg.nodes {
        rt.add(&node).ok(); // TODO: Handle error
    }
}

fn handle_store_message(
    addr: SocketAddr,
    msg: StoreMessage,
    rt: &routing::RoutingTable,
    storage: &Arc<RwLock<storage::Storage>>,
) -> Option<StoreResponse> {
    rt.add(&Peer {
        id: msg.id.clone(),
        address: addr.to_string(),
    })
    .ok(); // TODO: Handle error
    match storage.write().unwrap().set(&msg.id, msg.value) {
        Ok(_) => Some(StoreResponse {
            id: rt.id.clone(),
            success: true,
        }),
        Err(_) => Some(StoreResponse {
            id: rt.id.clone(),
            success: false,
        }),
    }
}

fn handle_store_response(addr: SocketAddr, msg: StoreResponse, rt: &routing::RoutingTable) {
    rt.add(&Peer {
        id: msg.id.clone(),
        address: addr.to_string(),
    })
    .ok(); // TODO: Handle error
}

fn handle_find_value_message(
    addr: SocketAddr,
    msg: FindValueMessage,
    rt: &routing::RoutingTable,
    storage: &Arc<RwLock<storage::Storage>>,
) -> Option<FindValueResponse> {
    rt.add(&Peer {
        id: msg.id.clone(),
        address: addr.to_string(),
    })
    .ok(); // TODO: Handle error
    let value = storage.read().unwrap().get(&msg.id);
    let nodes = match value {
        Some(_) => None,
        None => Some(rt.get_closest_k(&msg.id)),
    };
    Some(FindValueResponse {
        id: rt.id.clone(),
        value: value,
        nodes,
    })
}

fn handel_find_value_response(_: SocketAddr, msg: FindValueResponse, rt: &routing::RoutingTable) {
    if let Some(nodes) = msg.nodes {
        for node in nodes {
            rt.add(&node).ok(); // TODO: Handle error
        }
    }
}
