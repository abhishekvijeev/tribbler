use crate::lab2::storage_client_proxy::StorageClientProxy;
use crate::lab2::utils::calculate_hash;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tribbler::{err::TribResult, storage::BinStorage, storage::Storage};

#[derive(Clone, Debug)]
pub struct BinStorageBackend {
    pub addr: String,
    pub addr_hash: u64,
}

#[derive(Clone, Debug)]
pub struct BinStorageClient {
    pub backends: Vec<BinStorageBackend>,
}

impl BinStorageClient {
    // The constructor takes a list of IP addresses corresponding to the backend servers it knows
    // about and hashes these addresses onto a circular space as in Chord.
    pub fn new(backends: Vec<String>) -> Self {
        let mut backends_vec = Vec::new();
        for backend_addr in backends.iter() {
            let backend_addr_hash = calculate_hash(backend_addr);
            println!("Hash of {} = {}", backend_addr, backend_addr_hash);
            let scheme = "http://".to_string();
            let addr = format!("{}{}", scheme, backend_addr.clone());
            backends_vec.push(BinStorageBackend {
                addr: addr,
                addr_hash: backend_addr_hash,
            });
        }
        backends_vec.sort_by(|backend_1, backend_2| backend_2.addr_hash.cmp(&backend_1.addr_hash));
        return Self {
            backends: backends_vec,
        };
    }
}

#[async_trait]
impl BinStorage for BinStorageClient {
    // Fetches a [Storage] bin based on the given bin name by hashing the bin name onto the
    // same circular space of backend server address hashes. The bin is assignedto the server
    // immediately succeeding it on the circle
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        // let name_hash = calculate_hash(&name);
        // for backend in self.backends.iter() {
        //     if name_hash < backend.addr_hash {
        //         return Ok(Box::new(StorageClientProxy::new(name, &backend.addr[..])));
        //     }
        // }
        // return Ok(Box::new(StorageClientProxy::new(
        //     name,
        //     &self.backends[0].addr[..],
        // )));
        return Ok(Box::new(StorageClientProxy::new(
            name,
            // &self.backends[0].addr[..],
            &self.backends,
        )));
    }
}
