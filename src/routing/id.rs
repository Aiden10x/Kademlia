// src/routing/id.rs
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher}; // Add this line

/// The ID struct represents the identifier used in the Kademlia protocol.
#[derive(Debug)]
pub struct ID {
    pub value: BigUint,
}

impl Eq for ID {}

impl Hash for ID {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl PartialEq for ID {
    fn eq(&self, other: &ID) -> bool {
        self.value == other.value
    }
}

impl Clone for ID {
    fn clone(&self) -> Self {
        ID {
            value: self.value.clone(),
        }
    }
}

// TODO: neaten
impl Serialize for ID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_bytes().serialize(serializer)
    }
}

// TODO: neaten
impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> Result<ID, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        ID::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

impl ID {
    pub fn new(value: &BigUint) -> Result<Self, &str> {
        if value.to_bytes_be().len() > super::ID_LENGTH_BYTES {
            // TODO: string interpolation
            return Err("ID must be 32 bytes");
        }
        Ok(ID {
            value: value.clone(),
        })
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, &str> {
        if data.len() != super::ID_LENGTH_BYTES {
            // TODO: string interpolation
            return Err("ID must be 32 bytes");
        }
        Ok(ID {
            value: BigUint::from_bytes_be(data),
        })
    }

    pub fn zero() -> Self {
        ID {
            value: BigUint::from_bytes_be(&[0_u8; super::ID_LENGTH_BYTES]),
        }
    }

    /// Returns a new ID with the value of an unsigned integer (useful for testing)
    pub fn from_u32(data: u32) -> Self {
        ID {
            value: BigUint::from(data),
        }
    }

    pub fn random_id() -> Self {
        let rand_bytes: Vec<u8> = (0..super::ID_LENGTH_BYTES)
            .map(|_| rand::random::<u8>())
            .collect();
        ID {
            value: BigUint::from_bytes_be(&rand_bytes),
        }
    }

    pub fn min_id() -> Self {
        ID {
            value: BigUint::from(0_u32),
        }
    }

    pub fn max_id() -> Self {
        ID {
            value: BigUint::from_bytes_be(&[255_u8; super::ID_LENGTH_BYTES]),
        }
    }

    /// Returns the bytes of the ID in big-endian order - ID_LENGTH_BYTES bytes long
    ///
    /// # Examples
    ///
    /// ```
    /// let mut vec: Vec<u8> = vec![0; ID_LENGTH_BYTES];
    /// vec[19] = 0x01;
    /// assert_eq!(ID::new(&vec).unwrap().value, BigUint::from(1_u32));
    /// ```
    pub fn to_bytes(&self) -> Vec<u8> {
        self.value.to_bytes_be()
    }

    /// Returns a trimmed big endian formatted string of the ID (clamps to bits not bytes)
    ///
    /// # Examples
    ///
    /// ```
    /// let mut vec: Vec<u8> = vec![0; ID_LENGTH_BYTES];
    /// vec[18] = 0x01;
    /// vec[19] = 0x80;
    /// assert_eq!(ID::new(&vec).unwrap().as_big_endian_string(), "110000000");
    /// ```
    pub fn as_big_endian_string(&self) -> String {
        self.value.to_str_radix(2)
    }

    pub fn as_hex_string(&self) -> String {
        self.value.to_str_radix(16)
    }

    /// Returns a big endian formatted vector of booleans of the ID (160 long)
    ///
    /// # Examples
    ///
    /// ```
    /// let mut vec: Vec<u8> = vec![0; ID_LENGTH_BYTES];
    /// vec[0] = 0x01;
    /// vec[1] = 0x01;
    /// let bit_vec = ID::new(&vec).unwrap().as_big_endian_bit_vec();
    /// assert_eq!(bit_vec[0], true);
    /// assert_eq!(bit_vec[8], true);
    /// ```
    pub fn as_big_endian_bit_vec(&self) -> Vec<bool> {
        let mut vec = Vec::new();
        for byte in self.value.to_bytes_be() {
            for bit in 0..8 {
                vec.push((byte >> bit) & 1 == 1);
            }
        }
        // Pad the end of the vec to 160 bits
        for _ in vec.len()..160 {
            vec.push(false);
        }
        vec
    }

    /// Calculates the XOR distance between two IDs.
    pub fn distance(&self, other: &ID) -> ID {
        let xor_result = &self.value ^ &other.value;
        ID { value: xor_result }
    }

    pub fn flip_bit(&self, bit: usize) -> ID {
        let operand = BigUint::from(1_u32) << bit;
        let flipped = &self.value ^ &operand;
        ID { value: flipped }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_id() {
        let data: [u8; 32] = [0xFF; 32];
        let id = ID::from_bytes(&data);
        assert!(id.is_ok());
        assert_eq!(id.unwrap().value, BigUint::from_bytes_be(&data));

        let short_data: [u8; 32 - 1] = [0xFF; 32 - 1];
        let id = ID::from_bytes(&short_data);
        assert!(id.is_err());
    }

    #[test]
    fn test_zero_id() {
        let zero_id = ID::zero();
        assert_eq!(zero_id.value, BigUint::from_bytes_be(&[0_u8; 32]));
    }

    #[test]
    fn test_distance() {
        let id1 = ID::from_bytes(&[0x00; 32]).unwrap();
        let id2 = ID::from_bytes(&[0xFF; 32]).unwrap();

        let distance = id1.distance(&id2);
        let expected_distance = ID::from_bytes(&[0xFF; 32]).unwrap();

        assert_eq!(
            distance.as_big_endian_string(),
            expected_distance.as_big_endian_string()
        );
    }
}
