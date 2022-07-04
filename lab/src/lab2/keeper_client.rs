use crate::keeper;
use crate::keeper::keeper_liveness_client::KeeperLivenessClient;
use crate::keeper::keeper_liveness_server::KeeperLiveness;
use crate::keeper::keeper_liveness_server::KeeperLivenessServer;
use crate::lab1::client::StorageClient;
use crate::lab1::new_client;
use crate::lab2::bin_client::BinStorageClient;
use crate::lab2::utils::{
    calculate_hash, BIN_REPLICATION_META, FREQ_CHECK_BACKEND_LIVENESS, LIST_ALIVE_IDS,
    MIN_ALIVE_BACKENDS, REPLICA_NUM, SCHEME_HTTP,
};

use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::thread;
use std::{
    cmp::Ordering,
    sync::{Arc, RwLock},
};
use tokio::time;
use tonic::transport::Server;
use tribbler::{
    colon,
    config::KeeperConfig,
    err::TribResult,
    storage::{BinStorage, KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

const KEEPER_METADATA_BIN_NAME: &str = "KEEPER_METADATA_BIN";
const KEEPER_LIVENESS_LOG_KEY: &str = "KEEPER_LIVENESS_LOG";
const BACKEND_REPLICATION_LOG_KEY: &str = "BACKEND_REPLICATION_LOG";

#[derive(Clone, Debug)]
pub struct RemoteKeeperInfo {
    pub addr: String,
    pub addr_hash: u64,
    pub conn: Option<u64>,
    // pub conn: Option<rpc_conn_type>,
    pub alive: bool,
}

#[derive(Clone, Debug)]
pub struct BackendInfo {
    pub addr: String,
    pub addr_hash: u64,
    pub storage: Box<StorageClient>,
    pub alive: bool,
}

#[derive(Clone, Debug)]
pub struct Keeper {
    pub index: usize,
    pub addr_hash: u64,
    pub predecessor_addr_hash: Arc<RwLock<u64>>,
    pub num_keepers: usize,
    pub remote_keepers: Vec<RemoteKeeperInfo>,
    pub backends_info: Vec<BackendInfo>,
    pub alive_backend_ids: Vec<usize>,
    pub bin_storage_client: BinStorageClient,
    pub live_keeper_indices: HashSet<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LiveKeeperOp {
    pub index: usize,
    pub clock: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationOp {
    pub keeper_index: usize,
    pub backend_src_index: usize,
    pub backend_dest_index: usize,
    pub clock: u64,
    pub complete: bool,
}

// impl KeeperLivenessClient for Keeper {

// }

#[tonic::async_trait]
impl KeeperLiveness for Keeper {
    async fn poll(
        &self,
        request: tonic::Request<keeper::AddressHash>,
    ) -> Result<tonic::Response<keeper::Status>, tonic::Status> {
        println!("Received RPC");
        self.predecessor_addr_hash
            .write()
            .unwrap()
            .clone_from(&request.into_inner().address_hash);

        // self.predecessor_addr_hash = request.into_inner().address_hash;
        return Ok(tonic::Response::new(keeper::Status { alive: true }));
    }
}

impl Keeper {
    pub fn new(
        index: usize,
        addr_hash: u64,
        predecessor_addr_hash: Arc<RwLock<u64>>,
        num_keepers: usize,
        remote_keepers: Vec<RemoteKeeperInfo>,
        backends_info: Vec<BackendInfo>,
        alive_backend_ids: Vec<usize>,
        bin_storage_client: BinStorageClient,
        live_keeper_indices: HashSet<usize>,
    ) -> Keeper {
        return Self {
            index: index,
            addr_hash: addr_hash,
            predecessor_addr_hash: predecessor_addr_hash,
            num_keepers: num_keepers,
            remote_keepers: remote_keepers,
            backends_info: backends_info,
            alive_backend_ids: alive_backend_ids,
            bin_storage_client: bin_storage_client,
            live_keeper_indices: live_keeper_indices,
        };
    }

    pub(crate) async fn check_backend_liveness(
        id: usize,
        addr: &str,
        alive_backend_ids: &Vec<usize>,
    ) -> TribResult<(Option<bool>, usize)> {
        let sc = match new_client(addr).await {
            Ok(_sc) => _sc,
            Err(e) => return Err(e),
        };
        match sc.get("").await {
            Ok(_) => {
                if !alive_backend_ids.contains(&id) {
                    return Ok((Some(true), id));
                }
            }
            Err(_) => {
                if alive_backend_ids.contains(&id) {
                    return Ok((Some(false), id));
                }
            }
        }
        Ok((None, id))
    }

    pub(crate) async fn replicate_joined_backend(
        &self,
        this_backend_id: usize,
        this_replica_id: usize,
    ) -> TribResult<()> {
        let dst_addr = &self.backends_info[this_backend_id].addr;

        let successor_replica_id_1 = this_replica_id % self.num_keepers;
        let this_primary_replica_id_1 = match successor_replica_id_1 {
            id if id < 2 => self.num_keepers + id - 2,
            other => other - 2,
        };
        // self._replicate_backend(
        //     this_primary_replica_id_1,
        //     &self.backends_info[self.alive_backend_ids[successor_replica_id_1]].addr,
        //     dst_addr,
        // )
        // .await?;
        self._replicate_backend(
            this_primary_replica_id_1,
            self.alive_backend_ids[successor_replica_id_1],
            this_backend_id,
        )
        .await?;

        let successor_replica_id_2 = (this_replica_id + 1) % self.num_keepers;
        let this_primary_replica_id_2 = match successor_replica_id_2 {
            id if id < 2 => self.num_keepers + id - 2,
            other => other - 2,
        };
        // self._replicate_backend(
        //     this_primary_replica_id_2,
        //     &self.backends_info[self.alive_backend_ids[successor_replica_id_2]].addr,
        //     dst_addr,
        // )
        // .await?;
        self._replicate_backend(
            this_primary_replica_id_2,
            self.alive_backend_ids[successor_replica_id_2],
            this_backend_id,
        )
        .await?;

        let successor_replica_id_3 = (this_replica_id + 2) % self.num_keepers;
        let this_primary_replica_id_3 = match successor_replica_id_3 {
            id if id < 2 => self.num_keepers + id - 2,
            other => other - 2,
        };
        // self._replicate_backend(
        //     this_primary_replica_id_3,
        //     &self.backends_info[self.alive_backend_ids[successor_replica_id_3]].addr,
        //     dst_addr,
        // )
        // .await?;
        self._replicate_backend(
            this_primary_replica_id_3,
            self.alive_backend_ids[successor_replica_id_3],
            this_backend_id,
        )
        .await?;

        Ok(())
    }

    pub(crate) async fn replicate_left_backend(&self, this_replica_id: usize) -> TribResult<()> {
        let successor_replica_id = (this_replica_id + 1) % self.num_keepers;
        // self._replicate_backend(
        //     this_replica_id,
        //     &self.backends_info[self.alive_backend_ids[successor_replica_id]].addr,
        //     &self.backends_info[self.alive_backend_ids
        //         [(successor_replica_id + REPLICA_NUM - 1) % self.num_keepers]]
        //         .addr,
        // )
        // .await?;
        self._replicate_backend(
            this_replica_id,
            self.alive_backend_ids[successor_replica_id],
            self.alive_backend_ids[(successor_replica_id + REPLICA_NUM - 1) % self.num_keepers],
        )
        .await?;

        let predecessor_replica_id_1 = match this_replica_id {
            0 => self.num_keepers - 1,
            other => other - 1,
        };
        // self._replicate_backend(
        //     predecessor_replica_id_1,
        //     &self.backends_info[self.alive_backend_ids[predecessor_replica_id_1]].addr,
        //     &self.backends_info[self.alive_backend_ids
        //         [(predecessor_replica_id_1 + REPLICA_NUM) % self.num_keepers]]
        //         .addr,
        // )
        // .await?;
        self._replicate_backend(
            predecessor_replica_id_1,
            self.alive_backend_ids[predecessor_replica_id_1],
            self.alive_backend_ids[(predecessor_replica_id_1 + REPLICA_NUM) % self.num_keepers],
        )
        .await?;

        let predecessor_replica_id_2 = match this_replica_id {
            id if id < 2 => self.num_keepers + id - 2,
            other => other - 2,
        };
        // self._replicate_backend(
        //     predecessor_replica_id_2,
        //     &self.backends_info[self.alive_backend_ids[predecessor_replica_id_2]].addr,
        //     &self.backends_info[self.alive_backend_ids
        //         [(predecessor_replica_id_2 + REPLICA_NUM) % self.num_keepers]]
        //         .addr,
        // )
        // .await?;
        self._replicate_backend(
            predecessor_replica_id_2,
            self.alive_backend_ids[predecessor_replica_id_2],
            self.alive_backend_ids[(predecessor_replica_id_2 + REPLICA_NUM) % self.num_keepers],
        )
        .await?;

        Ok(())
    }

    async fn _replicate_backend(
        &self,
        this_primary_replica_id: usize,
        // src_addr: &str,
        // dst_addr: &str,
        src_index: usize,
        dst_index: usize,
    ) -> TribResult<()> {
        let src_addr = &self.backends_info[src_index].addr;
        let dst_addr = &self.backends_info[dst_index].addr;
        println!("_replicate_backend()");
        println!("\tthis_primary_replica_id: {}", this_primary_replica_id);
        println!("\tsrc_addr: {}", src_addr);
        println!("\tdest_addr: {}", dst_addr);

        let bin = self
            .bin_storage_client
            .bin(KEEPER_METADATA_BIN_NAME)
            .await?;

        let logical_clock = bin.clock(0).await?;

        let pre_replication_op = ReplicationOp {
            keeper_index: self.index,
            backend_src_index: src_index,
            backend_dest_index: dst_index,
            clock: logical_clock,
            complete: false,
        };
        let serialized_pre_replication_op = serde_json::to_string(&pre_replication_op)?;
        let pre_replication_op_kv = KeyValue {
            key: String::from(BACKEND_REPLICATION_LOG_KEY),
            value: serialized_pre_replication_op,
        };
        bin.list_append(&pre_replication_op_kv).await?;

        // DO REPLICATION

        // let src_sc = new_client(&src_addr[..]).await?;
        // let dst_sc = new_client(&dst_addr[..]).await?;
        let src_sc = new_client(src_addr).await?;
        let dst_sc = new_client(dst_addr).await?;

        let List(keys) = src_sc
            .keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?;

        for key in keys.iter() {
            let bin_name = match key.split_once("::") {
                Some((bin, _)) => bin,
                None => key,
            };
            let bin_hashed_id = (calculate_hash(&bin_name.to_string()) as usize) % self.num_keepers;
            let primary_replica = match self.alive_backend_ids.binary_search(&bin_hashed_id) {
                Ok(id) => id,
                Err(id) => id,
            };

            if primary_replica == this_primary_replica_id {
                if let Some(value) = src_sc.get(key).await? {
                    dst_sc
                        .set(&KeyValue {
                            key: key.to_string(),
                            value,
                        })
                        .await?;
                }
            }
        }

        let List(list_keys) = src_sc
            .list_keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?;

        for list_key in list_keys.iter() {
            let bin_name = match list_key.split_once("::") {
                Some((name, _)) => name,
                None => list_key,
            };
            let bin_hashed_id = (calculate_hash(&bin_name.to_string()) as usize) % self.num_keepers;
            let primary_replica_id = match self.alive_backend_ids.binary_search(&bin_hashed_id) {
                Ok(id) => id,
                Err(id) => id,
            };

            if primary_replica_id == this_primary_replica_id {
                let List(values) = src_sc.list_get(list_key).await?;
                for value in values.iter() {
                    dst_sc
                        .list_append(&KeyValue {
                            key: list_key.to_string(),
                            value: value.to_string(),
                        })
                        .await?;
                }
            }
        }

        let post_replication_op = ReplicationOp {
            keeper_index: self.index,
            backend_src_index: src_index,
            backend_dest_index: dst_index,
            clock: logical_clock,
            complete: true,
        };
        let serialized_post_replication_op = serde_json::to_string(&post_replication_op)?;
        let post_replication_op_kv = KeyValue {
            key: String::from(BACKEND_REPLICATION_LOG_KEY),
            value: serialized_post_replication_op,
        };
        bin.list_append(&post_replication_op_kv).await?;

        Ok(())
    }

    pub async fn copy_src_dest(&self, src_index: usize, dst_index: usize) -> TribResult<()> {
        let src_addr = &self.backends_info[src_index].addr;
        let dst_addr = &self.backends_info[dst_index].addr;
        let src_sc = new_client(src_addr).await?;
        let dst_sc = new_client(dst_addr).await?;

        let List(keys) = src_sc
            .keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?;

        for key in keys.iter() {
            if let Some(value) = src_sc.get(key).await? {
                dst_sc
                    .set(&KeyValue {
                        key: key.to_string(),
                        value,
                    })
                    .await?;
            }
        }

        let List(list_keys) = src_sc
            .list_keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await?;

        for list_key in list_keys.iter() {
            let List(values) = src_sc.list_get(list_key).await?;
            for value in values.iter() {
                dst_sc
                    .list_append(&KeyValue {
                        key: list_key.to_string(),
                        value: value.to_string(),
                    })
                    .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn store_replication_meta_hashed(&self) -> TribResult<()> {
        let serialized_alive_backend_ids = match serde_json::to_string(&self.alive_backend_ids) {
            Ok(s) => s,
            Err(e) => return Err(Box::new(e)),
        };

        // TODO once a new entry is appended, all the entries already on the list becomes stale, could consider gc at this point

        let bin = self
            .bin_storage_client
            .bin(KEEPER_METADATA_BIN_NAME)
            .await?;
        bin.list_append(&KeyValue {
            key: LIST_ALIVE_IDS.to_string(),
            value: serialized_alive_backend_ids,
        })
        .await?;

        Ok(())
    }

    pub async fn find_new_predecessor_index(&self, keeper_index: usize) -> usize {
        let mut num_keepers_checked = 0;
        let mut keeper_index_to_check = if keeper_index == 0 {
            self.remote_keepers.len() - 1
        } else {
            keeper_index - 1
        };

        while num_keepers_checked < self.remote_keepers.len() {
            if self.remote_keepers[keeper_index_to_check].alive {
                return keeper_index_to_check;
            }
            keeper_index_to_check = if keeper_index_to_check == 0 {
                self.remote_keepers.len() - 1
            } else {
                keeper_index_to_check - 1
            };
            num_keepers_checked += 1;
        }
        return self.index;
    }

    pub async fn keeper_is_my_predecessor(&self, keeper_addr_hash: u64) -> bool {
        let predecessor_addr_hash = self.predecessor_addr_hash.read().unwrap().clone();
        if predecessor_addr_hash < self.addr_hash {
            return (keeper_addr_hash > predecessor_addr_hash)
                && (keeper_addr_hash < self.addr_hash);
        } else {
            return (keeper_addr_hash > predecessor_addr_hash)
                || (keeper_addr_hash < self.addr_hash);
        }
    }

    pub async fn backend_is_mine(&self, backend_addr_hash: u64) -> bool {
        let predecessor_addr_hash = self.predecessor_addr_hash.read().unwrap().clone();
        if predecessor_addr_hash < self.addr_hash {
            return (backend_addr_hash > predecessor_addr_hash)
                && (backend_addr_hash <= self.addr_hash);
        } else {
            return (backend_addr_hash > predecessor_addr_hash)
                || (backend_addr_hash <= self.addr_hash);
        }
    }

    pub async fn sync_my_backend_clocks(&self) -> TribResult<()> {
        println!("Syncing clocks");
        let mut most_recent_clock: u64 = 0;
        for (backend_index, backend) in self.backends_info.iter().enumerate() {
            if self.backend_is_mine(backend.addr_hash).await {
                if let Ok(clock) = backend.storage.clock(0).await {
                    most_recent_clock = max(clock, most_recent_clock);
                }
            }
        }

        for (backend_index, backend) in self.backends_info.iter().enumerate() {
            if self.backend_is_mine(backend.addr_hash).await {
                backend.storage.clock(most_recent_clock).await;
            }
        }

        return Ok(());
    }

    pub async fn keeper_leaves(&mut self, dead_keeper_index: usize) -> TribResult<()> {
        let dead_keeper_addr_hash = self.remote_keepers[dead_keeper_index].addr_hash;

        // if this is my predecessor, update predecessor_hash
        if self.keeper_is_my_predecessor(dead_keeper_addr_hash).await {
            let new_predecessor_index = self.find_new_predecessor_index(self.index).await;
            self.predecessor_addr_hash
                .write()
                .unwrap()
                .clone_from(&self.remote_keepers[new_predecessor_index].addr_hash);
            // self.predecessor_addr_hash = self.remote_keepers[new_predecessor_index].addr_hash;
        }
        let bin = self
            .bin_storage_client
            .bin(KEEPER_METADATA_BIN_NAME)
            .await?;

        let logical_clock = bin.clock(0).await?;
        let mut ops_to_replicate = HashSet::new();

        // Was this keeper in the process of handling a backend failure?
        // If yes, I will go ahead and complete the replication for
        // backends that now belong to me
        let List(backend_replication_log) = bin.list_get(BACKEND_REPLICATION_LOG_KEY).await?;
        for op in backend_replication_log.iter().rev() {
            let op = colon::unescape(op);
            let deserialized_op: ReplicationOp = serde_json::from_str(&op)?;
            let backend_addr_hash = self.backends_info[deserialized_op.backend_src_index].addr_hash;
            if deserialized_op.keeper_index == dead_keeper_index &&
                self.backend_is_mine(backend_addr_hash).await &&
                // TODO: Check clock interval
                deserialized_op.clock >= (logical_clock - 2)
            {
                if !deserialized_op.complete {
                    ops_to_replicate.insert(op);
                } else {
                    ops_to_replicate.remove(&op);
                }
            }
        }

        for op in ops_to_replicate {
            let deserialized_op: ReplicationOp = serde_json::from_str(&op)?;
            let pre_replication_op = ReplicationOp {
                keeper_index: self.index,
                backend_src_index: deserialized_op.backend_src_index,
                backend_dest_index: deserialized_op.backend_dest_index,
                clock: logical_clock,
                complete: false,
            };
            let serialized_pre_replication_op = serde_json::to_string(&pre_replication_op)?;
            let pre_replication_op_kv = KeyValue {
                key: String::from(BACKEND_REPLICATION_LOG_KEY),
                value: serialized_pre_replication_op,
            };
            bin.list_append(&pre_replication_op_kv).await?;

            self.copy_src_dest(
                deserialized_op.backend_src_index,
                deserialized_op.backend_dest_index,
            )
            .await?;

            let post_replication_op = ReplicationOp {
                keeper_index: self.index,
                backend_src_index: deserialized_op.backend_src_index,
                backend_dest_index: deserialized_op.backend_dest_index,
                clock: logical_clock,
                complete: true,
            };
            let serialized_post_replication_op = serde_json::to_string(&post_replication_op)?;
            let post_replication_op_kv = KeyValue {
                key: String::from(BACKEND_REPLICATION_LOG_KEY),
                value: serialized_post_replication_op,
            };
            bin.list_append(&post_replication_op_kv).await?;
        }

        self.remote_keepers[dead_keeper_index].alive = false;

        return Ok(());
    }

    pub async fn keeper_joins(&mut self, live_keeper_index: usize) {
        let live_keeper_addr_hash = self.remote_keepers[live_keeper_index].addr_hash;

        // if this is my predecessor, update predecessor_hash
        if self.keeper_is_my_predecessor(live_keeper_addr_hash).await {
            self.predecessor_addr_hash
                .write()
                .unwrap()
                .clone_from(&live_keeper_addr_hash);
            // self.predecessor_addr_hash = live_keeper_addr_hash;
        }

        self.remote_keepers[live_keeper_index].alive = true
    }

    pub async fn backend_leaves(&self, backend_index: usize) -> TribResult<()> {
        let bin = self
            .bin_storage_client
            .bin(KEEPER_METADATA_BIN_NAME)
            .await?;
        let logical_clock = bin.clock(0).await?;
        // TODO: Implement function
        // let replica_indices = self.get_replica_indices(backend_index);
        // let replica_id = self.alive_backend_ids.binary_search(&backend_index).unwrap_or_else(|x| x);
        let replica_1_index = (backend_index + 1) % self.backends_info.len();
        let replica_2_index = (backend_index + 2) % self.backends_info.len();
        let replica_3_index = (backend_index + 3) % self.backends_info.len();
        let replica_indices = vec![replica_1_index, replica_2_index, replica_3_index];

        for replica_index in replica_indices {
            let pre_replication_op = ReplicationOp {
                keeper_index: self.index,
                backend_src_index: backend_index,
                backend_dest_index: replica_index,
                clock: logical_clock,
                complete: false,
            };
            let serialized_pre_replication_op = serde_json::to_string(&pre_replication_op)?;
            let pre_replication_op_kv = KeyValue {
                key: String::from(BACKEND_REPLICATION_LOG_KEY),
                value: serialized_pre_replication_op,
            };
            bin.list_append(&pre_replication_op_kv).await?;

            // DO REPLICATION FROM SRC TO DEST

            let post_replication_op = ReplicationOp {
                keeper_index: self.index,
                backend_src_index: backend_index,
                backend_dest_index: replica_index,
                clock: logical_clock,
                complete: true,
            };
            let serialized_post_replication_op = serde_json::to_string(&post_replication_op)?;
            let post_replication_op_kv = KeyValue {
                key: String::from(BACKEND_REPLICATION_LOG_KEY),
                value: serialized_post_replication_op,
            };
            bin.list_append(&post_replication_op_kv).await?;
        }
        return Ok(());
    }

    pub async fn backend_joins(&self, backend_index: usize) {}

    pub async fn update_live_keepers_rpc(&mut self) -> TribResult<()> {
        println!("update_live_keepers_rpc");
        let mut successor_index = (self.index + 1) % self.num_keepers;
        let mut previously_alive = self.remote_keepers[successor_index].alive;

        while successor_index != self.index {
            let remote_keeper_conn =
                KeeperLivenessClient::connect(self.remote_keepers[successor_index].addr.clone())
                    .await;
            // println!(
            //     "Connection status to {}: {:?}",
            //     self.remote_keepers[successor_index].addr.clone(),
            //     remote_keeper_conn
            // );
            previously_alive = self.remote_keepers[successor_index].alive;
            if let Ok(mut conn) = remote_keeper_conn {
                let status = conn
                    .poll(keeper::AddressHash {
                        address_hash: self.addr_hash,
                    })
                    .await;
                // println!("status: {:?}", status);
                let currently_alive = status.is_ok();

                if previously_alive && !currently_alive {
                    self.keeper_leaves(successor_index).await;
                } else if currently_alive && !previously_alive {
                    self.keeper_joins(successor_index).await;
                }
                if status.is_ok() {
                    break;
                }
            }
            successor_index = (successor_index + 1) % self.num_keepers;
        }

        return Ok(());
    }

    pub async fn update_live_keepers_bin_storage(&mut self) -> TribResult<()> {
        let mut live_keeper_indices = HashSet::new();
        let bin = self
            .bin_storage_client
            .bin(KEEPER_METADATA_BIN_NAME)
            .await?;
        let logical_clock = bin.clock(0).await?;
        let op = LiveKeeperOp {
            index: self.index,
            clock: logical_clock,
        };
        let serialized_op = serde_json::to_string(&op)?;
        let liveness_op_kv = KeyValue {
            key: String::from(KEEPER_LIVENESS_LOG_KEY),
            value: serialized_op,
        };
        let highest_live_keeper_index = self.index;
        bin.list_append(&liveness_op_kv).await?;
        let List(keeper_liveness_log) = bin.list_get(KEEPER_LIVENESS_LOG_KEY).await?;
        for op in keeper_liveness_log.iter().rev() {
            let op = colon::unescape(op);
            let deserialized_op: LiveKeeperOp = serde_json::from_str(&op)?;
            if deserialized_op.clock == logical_clock
                || deserialized_op.clock == (logical_clock - 1)
            {
                live_keeper_indices.insert(deserialized_op.index);
            } else {
                break;
            }
        }

        let my_live_keepers = self.live_keeper_indices.clone();
        let new_live_keepers = live_keeper_indices.difference(&my_live_keepers);
        let new_dead_keepers = my_live_keepers.difference(&live_keeper_indices);

        for &new_live_keeper_index in new_live_keepers {
            self.keeper_joins(new_live_keeper_index).await;
        }
        for &new_dead_keeper_index in new_dead_keepers {
            self.keeper_leaves(new_dead_keeper_index).await;
        }

        // update predecessor_hash
        let new_predecessor_index = self.find_new_predecessor_index(self.index).await;
        self.predecessor_addr_hash
            .write()
            .unwrap()
            .clone_from(&self.remote_keepers[new_predecessor_index].addr_hash);
        // self.predecessor_addr_hash = self.remote_keepers[new_predecessor_index].addr_hash;
        return Ok(());
    }

    pub async fn monitor_my_backends(&self) -> TribResult<()> {
        println!("monitoring my backends");

        for (backend_index, backend) in self.backends_info.iter().enumerate() {
            if self.backend_is_mine(backend.addr_hash).await {
                let previously_alive = backend.alive;
                let status = backend.storage.get("").await;
                let currently_alive = status.is_ok();

                if previously_alive && !currently_alive {
                    self.backend_leaves(backend_index).await;
                } else if currently_alive && !previously_alive {
                    self.backend_joins(backend_index).await;
                }
            }
        }

        return Ok(());
    }
}
