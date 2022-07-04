use crate::lab1::client::StorageClient;
use crate::lab2::bin_client::BinStorageBackend;
use crate::lab2::utils::calculate_hash;
use async_trait::async_trait;
use std::cmp::max;
use tribbler::{
    colon,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

const NUM_REPLICAS: u64 = 3;

// Provides a wrapper around lab1's StorageClient that sanitizes all input using tribbler::colon
// and prefixes each key with the bin name
pub struct StorageClientProxy {
    pub bin_name: String,
    pub bin_hash: u64,
    pub backends_info: Vec<BinStorageBackend>,
    // pub backends: Vec<Box<dyn Storage>>,
    pub backends: Vec<Box<StorageClient>>,
}

impl StorageClientProxy {
    pub fn new(bin_name: &str, backends_info: &Vec<BinStorageBackend>) -> StorageClientProxy {
        let bin_name = colon::escape(bin_name.to_string());
        let bin_hash = calculate_hash(&bin_name);
        let mut backends = Vec::new();

        for backend_info in backends_info.iter() {
            let backend = Box::new(StorageClient {
                addr: backend_info.addr.to_string(),
            });
            backends.push(backend);
        }
        return Self {
            bin_name: colon::escape(bin_name.to_string()),
            bin_hash: bin_hash,
            backends_info: backends_info.to_vec(),
            backends: backends,
        };
    }

    async fn is_backend_alive(&self, backend_index: usize) -> bool {
        let status = self.backends[backend_index].get("").await;
        return status.is_ok();
    }

    // This function searches through the list of
    // backends to find the first 'n' live backends
    // that immediately succeed this bin on the
    // Chord circle
    async fn find_live_backends(&self, n: u64) -> Vec<usize> {
        let mut live_backend_indices = Vec::new();
        let mut count = 0;

        // TODO: Binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr = &backend_info.addr;
            let backend_addr_hash = backend_info.addr_hash;
            if (self.bin_hash < backend_addr_hash) && (self.is_backend_alive(index).await) {
                if count < n {
                    live_backend_indices.push(index);
                    count += 1;
                } else {
                    break;
                }
            }
        }
        return live_backend_indices;
    }
}

#[async_trait]
impl KeyString for StorageClientProxy {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        println!("StorageClientProxy::get()");
        let sanitized_named_key = self.bin_name.clone() + "::" + &colon::escape(key)[..];
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!("\tkey = {}", sanitized_named_key);
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            let status = self.backends[backend_index].get(&sanitized_named_key).await;
            if status.is_ok() {
                return status;
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }
        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        println!("StorageClientProxy::set()");
        let sanitized_named_key = self.bin_name.clone() + "::" + &colon::escape(kv.key.clone())[..];
        let sanitized_value = colon::escape(kv.value.clone());
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!(
            "\tkey = {}, value = {}",
            sanitized_named_key, sanitized_value
        );
        let updated_kv = KeyValue {
            key: sanitized_named_key,
            value: sanitized_value,
        };
        let mut replica_count = 0;
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case

        // TODO: Handle concurrent set operations from multiple front-ends
        while backends_visited < self.backends_info.len() {
            let status = self.backends[backend_index].set(&updated_kv).await;
            if status.is_ok() {
                println!(
                    "\treplicated on server {}, count = {}",
                    self.backends_info[backend_index].addr, replica_count
                );
                replica_count += 1;
                if replica_count == NUM_REPLICAS {
                    return status;
                }
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }

        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        println!("StorageClientProxy::keys()");
        let prefix = p.prefix.clone();
        let suffix = p.suffix.clone();
        let sanitized_named_prefix = self.bin_name.clone() + "::" + &colon::escape(prefix)[..];
        let sanitized_suffix = colon::escape(suffix);
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!(
            "\tprefix = {}, suffix = {}",
            sanitized_named_prefix, sanitized_suffix
        );
        let updated_pattern = Pattern {
            prefix: sanitized_named_prefix,
            suffix: sanitized_suffix,
        };
        let prefix_to_discard = self.bin_name.clone() + "::";
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            if let Ok(List(keys_vec)) = self.backends[backend_index].keys(&updated_pattern).await {
                let keys_vec = keys_vec
                    .iter()
                    .map(|key| {
                        colon::unescape(key.trim_start_matches(&prefix_to_discard[..]).to_string())
                    })
                    .collect::<Vec<_>>();
                println!(
                    "\tretrieved keys from server {}",
                    self.backends_info[backend_index].addr
                );
                return Ok(List(keys_vec));
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }
        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }
}

#[async_trait]
impl KeyList for StorageClientProxy {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        println!("StorageClientProxy::list_get()");
        let sanitized_named_key = self.bin_name.clone() + "::" + &colon::escape(key)[..];
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!("\tkey = {}", sanitized_named_key);
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            let status = self.backends[backend_index]
                .list_get(&sanitized_named_key)
                .await;
            if status.is_ok() {
                println!(
                    "\tretrieved list from server {}",
                    self.backends_info[backend_index].addr
                );
                return status;
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }
        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        println!("StorageClientProxy::list_append()");
        let sanitized_named_key = self.bin_name.clone() + "::" + &colon::escape(kv.key.clone())[..];
        let sanitized_value = colon::escape(kv.value.clone());
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!(
            "\tkey = {}, value = {}",
            sanitized_named_key, sanitized_value
        );

        let updated_kv = KeyValue {
            key: sanitized_named_key,
            value: sanitized_value,
        };
        let mut replica_count = 0;
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // println!("\tstart_index: {}", backend_index);

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            let status = self.backends[backend_index].list_append(&updated_kv).await;
            if status.is_ok() {
                println!(
                    "\treplicated on server {}, count = {}",
                    self.backends_info[backend_index].addr, replica_count
                );
                replica_count += 1;
                if replica_count == NUM_REPLICAS {
                    return status;
                }
            }
            // else {
            //     println!(
            //         "\terr status for backend {:?}",
            //         self.backends_info[backend_index]
            //     )
            // }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }

        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        println!("StorageClientProxy::list_remove()");
        let sanitized_named_key = self.bin_name.clone() + "::" + &colon::escape(kv.key.clone())[..];
        let sanitized_value = colon::escape(kv.value.clone());
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!(
            "\tkey = {}, value = {}",
            sanitized_named_key, sanitized_value
        );
        let updated_kv = KeyValue {
            key: sanitized_named_key,
            value: sanitized_value,
        };
        let mut replica_count = 0;
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            let status = self.backends[backend_index].list_remove(&updated_kv).await;
            if status.is_ok() {
                println!(
                    "\tremoved from server {}, count = {}",
                    self.backends_info[backend_index].addr, replica_count
                );
                replica_count += 1;
                if replica_count == NUM_REPLICAS {
                    return status;
                }
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }

        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        println!("StorageClientProxy::list_keys()");
        let prefix = p.prefix.clone();
        let suffix = p.suffix.clone();
        let sanitized_named_prefix = self.bin_name.clone() + "::" + &colon::escape(prefix)[..];
        let sanitized_suffix = colon::escape(suffix);
        println!("\tbin_name = {}", self.bin_name);
        println!("\tbin_hash = {}", self.bin_hash);
        println!(
            "\tprefix = {}, suffix = {}",
            sanitized_named_prefix, sanitized_suffix
        );
        let updated_pattern = Pattern {
            prefix: sanitized_named_prefix,
            suffix: sanitized_suffix,
        };
        let prefix_to_discard = self.bin_name.clone() + "::";
        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            if let Ok(List(keys_vec)) = self.backends[backend_index]
                .list_keys(&updated_pattern)
                .await
            {
                let keys_vec = keys_vec
                    .iter()
                    .map(|key| {
                        colon::unescape(key.trim_start_matches(&prefix_to_discard[..]).to_string())
                    })
                    .collect::<Vec<_>>();
                println!(
                    "\tretrieved keys from server {}",
                    self.backends_info[backend_index].addr
                );
                return Ok(List(keys_vec));
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }

        println!();
        return Err(Box::new(TribblerError::Unknown(String::from(
            "All backends dead!",
        ))));
    }
}

#[async_trait]
impl Storage for StorageClientProxy {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        println!("StorageClientProxy::clock()");
        let mut most_recent_clock: u64 = 0;
        let mut replica_count = 0;
        let mut backend_indices_to_update = Vec::new();

        let mut backend_index = 0;
        let mut backends_visited = 0;

        // TODO: Get start index using binary search
        for (index, backend_info) in self.backends_info.iter().enumerate() {
            let backend_addr_hash = backend_info.addr_hash;
            if self.bin_hash < backend_addr_hash {
                backend_index = index;
                break;
            }
        }

        // TODO: Store live backends somewhere so that we
        // don't need to potentially iterate through all
        // backends in the worst case
        while backends_visited < self.backends_info.len() {
            if let Ok(clock) = self.backends[backend_index].clock(at_least).await {
                most_recent_clock = max(most_recent_clock, clock);
                backend_indices_to_update.push(backend_index);
                println!(
                    "\tretrieved clock from server {}, count = {}",
                    self.backends_info[backend_index].addr, replica_count
                );
                replica_count += 1;
                if replica_count == NUM_REPLICAS {
                    // return status;
                    break;
                }
            }
            backend_index = (backend_index + 1) % (self.backends_info.len());
            backends_visited += 1;
        }

        for &backend_index in backend_indices_to_update.iter() {
            self.backends[backend_index]
                .clock(most_recent_clock)
                .await?;
        }

        return if replica_count == NUM_REPLICAS {
            Ok(most_recent_clock)
        } else {
            Err(Box::new(TribblerError::Unknown(String::from(
                "All backends dead!",
            ))))
        };
    }
}
