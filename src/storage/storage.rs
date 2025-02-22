use crate::routing::ID;
use rocksdb::DB;

#[derive(Debug, Clone)]
pub struct Storage {
    pub path: String,
}

// TODO: multithread
impl Storage {
    pub fn new() -> Self {
        Storage {
            path: "./data".to_string(),
        }
    }

    pub fn get(&self, key: &ID) -> Option<Vec<u8>> {
        let db = DB::open_default(&self.path).unwrap();
        match db.get(key.value.to_bytes_be()) {
            Ok(Some(value)) => Some(value),
            Ok(None) => None,
            Err(e) => {
                println!("Error: {}", e);
                None
            }
        }
    }

    // TODO: wrap this error
    pub fn set(&mut self, key: &ID, value: Vec<u8>) -> Result<(), rocksdb::Error> {
        let db = DB::open_default(&self.path).unwrap();
        db.put(key.value.to_bytes_be(), value)
    }

    pub fn remove(&mut self, key: &ID) {
        let db = DB::open_default(&self.path).unwrap();
        db.delete(key.value.to_bytes_be()).unwrap();
    }
}
