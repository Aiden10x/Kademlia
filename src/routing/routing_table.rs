use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::{Deserialize, Serialize};

use crate::routing::ID;

/// Represents the routing table in the Kademlia network.
///
/// The `RoutingTable` class is responsible for storing and managing the routing information of nodes in the Kademlia network.
/// It maintains a hierarchical structure, typically using a binary tree or a similar data structure, for efficient lookup
/// and organization of nodes based on their proximity to the local node's ID.
///
/// Responsibilities:
/// - Storing and organizing node entries in the routing table based on their proximity to the local node's ID.
/// - Adding, removing, and updating nodes in the routing table as nodes join, leave, or change contact information.
/// - Providing methods to retrieve the closest nodes to a given ID, necessary for operations like lookups and queries.
/// - Performing lookup operations to locate nodes responsible for specific IDs in the network.
/// - Implementing utility methods for splitting buckets, calculating bucket indexes, and other internal operations.
///
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Peer {
    pub id: ID,
    pub address: String,
}

#[derive(Debug)]
pub struct Node {
    peers: Option<Vec<Peer>>,
    left: Option<NodeRef>,
    right: Option<NodeRef>,
}
type NodeRef = Arc<RwLock<Node>>;

impl Node {
    pub fn new(peers: Option<Vec<Peer>>, left: Option<NodeRef>, right: Option<NodeRef>) -> Self {
        Node { peers, left, right }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    pub id: ID,
    root: NodeRef,
}

pub enum RoutingTableError {
    BucketFullError,
}

impl RoutingTable {
    // Initialize a new RoutingTable instance
    pub fn new(id: &ID) -> Self {
        let root: Node = Node::new(Some(Vec::new()), None, None);
        RoutingTable {
            id: id.clone(),
            root: Arc::new(RwLock::new(root)),
        }
    }

    pub fn add(&self, peer: &Peer) -> Result<(), RoutingTableError> {
        let self_id = &self.id.clone();

        if peer.id == *self_id {
            return Ok(());
        }

        return self.traverse_mut(
            &peer.id,
            &mut |node_guard, depth, can_split, distance_bits| {
                // Check if the contact already exists.
                let existing_index = node_guard
                    .peers
                    .as_ref()
                    .unwrap()
                    .iter()
                    .position(|x| x.id == peer.id);
                if existing_index.is_some() {
                    // Move to the front
                    let peer = node_guard.peers.as_ref().unwrap()[existing_index.unwrap()].clone();
                    node_guard
                        .peers
                        .as_mut()
                        .unwrap()
                        .remove(existing_index.unwrap());
                    node_guard.peers.as_mut().unwrap().insert(0, peer);
                    return Ok(());
                }

                // If the leaf node has less than K contacts, add the contact
                if node_guard.peers.as_mut().unwrap().len() < super::K {
                    node_guard.peers.as_mut().unwrap().insert(0, peer.clone());
                    return Ok(());
                }

                // If the bucket is full and can't be split, evict the contact at the end
                if !can_split {
                    node_guard.peers.as_mut().unwrap().pop();
                    node_guard.peers.as_mut().unwrap().insert(0, peer.clone());
                    return Err(RoutingTableError::BucketFullError);
                }

                // The bucket is full and needs to be split
                let mut new_left = Node::new(Some(Vec::new()), None, None);
                let mut new_right = Node::new(Some(Vec::new()), None, None);

                // Add the existing contacts to the new nodes
                for existing_peer in node_guard.peers.as_ref().unwrap() {
                    let existing_id_bits =
                        self_id.distance(&existing_peer.id).as_big_endian_bit_vec();
                    if existing_id_bits[depth] {
                        new_right
                            .peers
                            .as_mut()
                            .unwrap()
                            .push(existing_peer.clone());
                    } else {
                        new_left.peers.as_mut().unwrap().push(existing_peer.clone());
                    }
                }

                // Add the new contact to the correct node
                if distance_bits[depth] {
                    new_right.peers.as_mut().unwrap().insert(0, peer.clone());
                } else {
                    new_left.peers.as_mut().unwrap().insert(0, peer.clone());
                }

                // Update the leaf node to be a parent node
                node_guard.peers = None;
                node_guard.left = Some(Arc::new(RwLock::new(new_left)));
                node_guard.right = Some(Arc::new(RwLock::new(new_right)));

                Ok(())
            },
            None,
            None,
            None,
        );
    }

    pub fn has(&self, id: &ID) -> bool {
        self.traverse(
            id,
            &mut |node_guard, _, _, _| {
                node_guard
                    .peers
                    .as_ref()
                    .unwrap()
                    .iter()
                    .any(|x| x.id == *id)
            },
            None,
            None,
            None,
        )
    }

    pub fn remove(&mut self, id: &ID) {
        self.traverse_mut(
            id,
            &mut |node_guard, _, _, _| {
                let peers = node_guard.peers.as_mut().unwrap();
                if let Some(index) = peers.iter().position(|x| x.id == *id) {
                    peers.remove(index);
                }
            },
            None,
            None,
            None,
        );
    }

    pub fn get_closest(&self, id: &ID) -> (Vec<Peer>, usize) {
        self.traverse(
            id,
            &mut |node_guard, depth, _, _| {
                let mut sorted_peers = node_guard.peers.as_ref().unwrap().clone();
                sorted_peers
                    .sort_by(|a, b| id.distance(&a.id).value.cmp(&id.distance(&b.id).value));
                (sorted_peers, depth)
            },
            None,
            None,
            None,
        )
    }

    // TODO: verify this function
    pub fn get_closest_k(&self, id: &ID) -> Vec<Peer> {
        let (mut closest, depth) = self.get_closest(id).clone();
        while closest.len() < super::K && depth > 0 {
            let new_id = id.flip_bit(depth);
            let (new_closest, _) = self.get_closest(&new_id);
            closest.extend(new_closest);
        }
        closest.truncate(super::K);
        closest
    }

    pub fn get_contacts(&self, id: &ID) -> Vec<Peer> {
        self.traverse(
            id,
            &mut |node_guard, _, _, _| node_guard.peers.as_ref().unwrap().clone(),
            None,
            None,
            None,
        )
    }

    fn traverse_mut<T>(
        &self,
        id: &ID,
        closure: &mut dyn FnMut(&mut RwLockWriteGuard<Node>, usize, bool, Vec<bool>) -> T,
        depth: Option<usize>,
        can_split: Option<bool>,
        current_node: Option<NodeRef>,
    ) -> T {
        // TODO: make non optional arg
        let distance_bits: Vec<bool> = self.id.distance(id).as_big_endian_bit_vec();
        let node: Arc<RwLock<Node>> = current_node.unwrap_or_else(|| self.root.clone());
        let depth = depth.unwrap_or(0);
        let can_split = can_split.unwrap_or(true);

        // NOTE: If we ever add node deletion this needs to be changed to write lock all the way down
        let node_guard = node.read().unwrap();

        if let Some(_) = &node_guard.peers {
            // Drop node_guard to allow write lock
            drop(node_guard);
            return closure(&mut node.write().unwrap(), depth, can_split, distance_bits);
        }

        let branch_left = !distance_bits[depth];
        return self.traverse_mut(
            id,
            closure,
            Some(depth + 1),
            Some(branch_left),
            (if branch_left {
                node_guard.left.clone()
            } else {
                node_guard.right.clone()
            }),
        );
    }

    fn traverse<T>(
        &self,
        id: &ID,
        closure: &mut dyn FnMut(&RwLockReadGuard<Node>, usize, bool, Vec<bool>) -> T,
        depth: Option<usize>,
        can_split: Option<bool>,
        current_node: Option<NodeRef>,
    ) -> T {
        // TODO: make non optional arg
        let distance_bits: Vec<bool> = self.id.distance(id).as_big_endian_bit_vec();
        let node = current_node.unwrap_or_else(|| self.root.clone());
        let depth = depth.unwrap_or(0);
        let can_split = can_split.unwrap_or(true);
        let node_guard = node.read().unwrap();

        if let Some(_) = &node_guard.peers {
            return closure(&node_guard, depth, can_split, distance_bits);
        }

        let branch_left = !distance_bits[depth];
        return self.traverse(
            id,
            closure,
            Some(depth + 1),
            Some(branch_left),
            (if branch_left {
                node_guard.left.clone()
            } else {
                node_guard.right.clone()
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_routing_table() {
        let id = ID::zero();
        let routing_table = RoutingTable::new(&id);
        assert_eq!(routing_table.id.value, id.value);
    }

    #[test]
    fn test_add() {
        let id = ID::zero();
        let routing_table = RoutingTable::new(&id);
        let new_id = ID::random_id();
        routing_table.add(&Peer {
            id: new_id.clone(),
            address: "".to_owned(),
        });
        assert_eq!(
            routing_table.root.read().unwrap().peers.as_ref().unwrap()[0]
                .id
                .value,
            new_id.value
        );
    }

    #[test]
    fn test_add_split() {
        let id = ID::zero();
        let mut routing_table = RoutingTable::new(&id);
        for _ in 0..20 {
            let new_id = ID::random_id();
            routing_table.add(&Peer {
                id: new_id,
                address: "".to_owned(),
            });
        }
        let new_id = ID::random_id();
        routing_table.add(&Peer {
            id: new_id.clone(),
            address: "".to_owned(),
        });
        // Make sure the bucket was split
        assert_eq!(routing_table.root.read().unwrap().peers, None);
    }

    #[test]
    fn test_has() {
        let id = ID::zero();
        let mut routing_table = RoutingTable::new(&id);
        for _ in 0..1000 {
            routing_table.add(&Peer {
                id: ID::random_id(),
                address: "".to_owned(),
            });
        }
        let new_id = ID::random_id();
        routing_table.add(&Peer {
            id: new_id.clone(),
            address: "".to_owned(),
        });
        assert!(routing_table.has(&new_id));
    }

    #[test]
    fn test_remove() {
        let id = ID::zero();
        let mut routing_table = RoutingTable::new(&id);
        for _ in 0..20 {
            routing_table.add(&Peer {
                id: ID::random_id(),
                address: "".to_owned(),
            });
        }
        let new_id = ID::random_id();
        routing_table.add(&Peer {
            id: new_id.clone(),
            address: "".to_owned(),
        });
        assert!(routing_table.has(&new_id));
        routing_table.remove(&new_id);
        assert!(!routing_table.has(&new_id));
    }

    #[test]
    fn test_get_closest() {
        let id = ID::zero();
        let mut routing_table = RoutingTable::new(&id);
        for _ in 0..2000 {
            routing_table.add(&Peer {
                id: ID::random_id(),
                address: "".to_owned(),
            });
        }
        let new_id = ID::random_id();
        routing_table.add(&Peer {
            id: new_id.clone(),
            address: "".to_owned(),
        });
        let mut distances = routing_table
            .get_closest(&new_id)
            .0
            .into_iter()
            .map(|peer| peer.id.distance(&new_id));

        let mut last_distance = distances.next().unwrap();
        for distance in distances {
            assert!(last_distance.value <= distance.value);
            last_distance = distance;
        }
    }

    #[test]
    fn tesst_get_closest_K() {
        let id = ID::zero();
        let routing_table = RoutingTable::new(&id);
        for _ in 0..30 {
            routing_table.add(&Peer {
                id: ID::random_id(),
                address: "".to_owned(),
            });
        }
        let new_id = ID::random_id();
        let closest = routing_table.get_closest_k(&new_id);
        assert_eq!(closest.len(), 20);
    }
}
