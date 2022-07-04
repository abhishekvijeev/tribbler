use crate::keeper::keeper_liveness_server::KeeperLivenessServer;
use crate::lab1::client::StorageClient;
use crate::lab2::bin_client::BinStorageClient;
use crate::lab2::front_end_server::FrontEndServer;
use crate::lab2::keeper_client::BackendInfo;
use crate::lab2::keeper_client::Keeper;
use crate::lab2::keeper_client::RemoteKeeperInfo;
use crate::lab2::utils::{
    calculate_hash, BIN_REPLICATION_META, FREQ_CHECK_BACKEND_LIVENESS, FREQ_CHECK_KEEPER_LIVENESS,
    MIN_ALIVE_BACKENDS, SCHEME_HTTP,
};
use std::collections::HashSet;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use tokio::{task, time};
use tribbler::{
    config::KeeperConfig,
    err::TribResult,
    storage::BinStorage,
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
    trib::Server,
};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    println!("Creating new bin storage client");
    return Ok(Box::new(BinStorageClient::new(backs)));
}

pub async fn sync_clocks(storage_clients: &Vec<Box<StorageClient>>) -> TribResult<()> {
    println!("Syncing clocks");
    let mut most_recent_clock: u64 = 0;
    for storage_client in storage_clients.iter() {
        // println!("client addr: {}", storage_client.addr);
        let status = storage_client.clock(0).await;
        // if status.is_ok() {
        if let Ok(clock) = status {
            if clock > most_recent_clock {
                most_recent_clock = clock;
            }
        }
    }

    for storage_client in storage_clients.iter() {
        storage_client.clock(most_recent_clock).await;
    }
    return Ok(());
}

// async fn wait_for_shutdown(shutdown: &mut Receiver<()>) -> Option<()>{
//     shutdown.recv().await
// }

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let mut storage_clients = Vec::new();
    let mut alive_backend_ids = Vec::new();
    let mut backends_info = Vec::new();
    let mut remote_keepers = Vec::new();
    let mut my_index = kc.this;
    let my_addr = kc.addrs[my_index].to_string();
    let my_addr_hash = calculate_hash(&my_addr);
    let mut alive_backends = 0;
    let bin_storage_client = BinStorageClient::new(kc.backs.clone());

    for keeper_addr in kc.addrs.iter() {
        // println!(
        //     "addr: {}, hash: {}",
        //     keeper_addr,
        //     calculate_hash(keeper_addr)
        // );
        let addr = format!("{}{}", SCHEME_HTTP, keeper_addr.to_string());
        remote_keepers.push(RemoteKeeperInfo {
            addr: addr,
            addr_hash: calculate_hash(keeper_addr),
            conn: None,
            alive: false,
        });
    }
    remote_keepers.sort_by(|remote_keeper_1, remote_keeper_2| {
        remote_keeper_1.addr_hash.cmp(&remote_keeper_2.addr_hash)
    });
    for (index, remote_keeper) in remote_keepers.iter().enumerate() {
        if remote_keeper.addr == my_addr {
            my_index = index;
            break;
        }
    }
    println!("Remote keepers:");
    println!("{:?}", remote_keepers);
    println!("My index: {}, my_addr: {}", my_index, my_addr);

    for (backend_id, backend_addr) in kc.backs.iter().enumerate() {
        let addr = format!("{}{}", SCHEME_HTTP, backend_addr.clone());
        let addr_hash = calculate_hash(&addr);
        let storage_client = Box::new(StorageClient { addr: addr.clone() });
        let alive = false;

        // match storage_client.get("").await {
        //     Ok(_) => {
        //         alive_backend_ids.push(backend_id);
        //     }
        //     Err(_) => (),
        // }

        storage_clients.push(storage_client.clone());

        backends_info.push(BackendInfo {
            addr: addr,
            addr_hash: addr_hash,
            storage: storage_client,
            alive: alive,
        });
    }
    backends_info.sort_by(|backend_info_1, backend_info_2| {
        backend_info_1.addr_hash.cmp(&backend_info_2.addr_hash)
    });
    for (backend_id, backend_info) in backends_info.iter().enumerate() {
        let addr = backend_info.addr.clone();
        let storage_client = Box::new(StorageClient { addr: addr });
        match storage_client.get("").await {
            Ok(_) => {
                alive_backend_ids.push(backend_id);
            }
            Err(_) => (),
        }
    }
    println!("Backends:");
    println!("{:?}", backends_info);

    // init keeper bin storage client, just treat it as another frontend
    let keeper_bin_client = new_bin_client(kc.backs.clone()).await?;
    let replication_meta_bsc = keeper_bin_client.bin(BIN_REPLICATION_META).await?; // TODO check keeper replciation meta bin name

    let mut live_keeper_indices = HashSet::new();
    live_keeper_indices.insert(my_index);
    let backends_info_clone = backends_info.clone();
    let num_keepers = kc.addrs.len();
    let predecessor_hash = Arc::new(RwLock::new(my_addr_hash));

    let mut keeper = Keeper::new(
        my_index,
        my_addr_hash,
        predecessor_hash,
        num_keepers,
        remote_keepers,
        backends_info,
        alive_backend_ids.clone(),
        bin_storage_client,
        live_keeper_indices,
    );

    while alive_backends < MIN_ALIVE_BACKENDS {
        std::thread::sleep(std::time::Duration::from_millis(100));
        for (backend_index, backend) in backends_info_clone.iter().enumerate() {
            if backend.alive {
                let status = backend.storage.get("").await;
                if status.is_err() {
                    keeper.backends_info[backend_index].alive = false;
                    alive_backends -= 1;
                }
            } else {
                let status = backend.storage.get("").await;
                if status.is_ok() {
                    keeper.backends_info[backend_index].alive = true;
                    alive_backends += 1;
                }
            }
        }
    }

    // init replication meta in a live backend
    keeper.store_replication_meta_hashed().await?;

    // Start RPC Server
    let keeper_clone = keeper.clone();
    let mut keeper_clone2 = keeper.clone();
    let mut keeper_clone3 = keeper.clone();

    // TODO: Add task for keeper replication

    let task_keeper_replication = task::spawn(async move {
        let mut interval_keepers =
            time::interval(time::Duration::from_secs(FREQ_CHECK_KEEPER_LIVENESS));
        loop {
            interval_keepers.tick().await;
            keeper_clone2.update_live_keepers_rpc().await;
        }
    });

    let task_backend_replication = task::spawn(async move {
        let mut interval_backends =
            time::interval(time::Duration::from_secs(FREQ_CHECK_BACKEND_LIVENESS));

        loop {
            interval_backends.tick().await;

            let mut task_check_alive_backends = Vec::new();

            for (backend_id, backend_info) in keeper.backends_info.iter().enumerate() {
                let backend_addr = backend_info.addr.clone();
                // let backend_addr = backend_addr.clone();
                let alive_backend_ids = keeper.alive_backend_ids.clone();

                task_check_alive_backends.push(task::spawn(async move {
                    Keeper::check_backend_liveness(backend_id, &backend_addr, &alive_backend_ids)
                        .await
                }));
            }

            for task in task_check_alive_backends {
                if let Ok((Some(join), backend_id)) = task.await.unwrap() {
                    match join {
                        true => {
                            println!("backend {} just joined!", backend_id);

                            let replica_id = keeper
                                .alive_backend_ids
                                .binary_search(&backend_id)
                                .unwrap_or_else(|x| x);

                            if keeper
                                .backend_is_mine(keeper.backends_info[backend_id].addr_hash)
                                .await
                            {
                                let status = keeper
                                    .replicate_joined_backend(backend_id, replica_id)
                                    .await;
                            }
                            // .unwrap();

                            keeper.alive_backend_ids.insert(replica_id, backend_id);
                        }
                        false => {
                            println!("backend {} just left!", backend_id);
                            let replica_id = keeper
                                .alive_backend_ids
                                .binary_search(&backend_id)
                                .unwrap_or_else(|x| x);

                            if keeper
                                .backend_is_mine(keeper.backends_info[backend_id].addr_hash)
                                .await
                            {
                                let status = keeper.replicate_left_backend(replica_id).await;
                                // .unwrap();
                            }

                            keeper.alive_backend_ids.remove(replica_id);
                        }
                    };

                    // after that, update replication meta
                    keeper.store_replication_meta_hashed().await.unwrap();
                }
            }

            println!("a round of backend liveness check and replication (if necessary) completed");
        }
    });

    let rpc_server_task = task::spawn(async move {
        println!("\n\naddr:  {} \n\n", my_addr);
        let my_rpc_addr: Vec<_> = my_addr
            .to_socket_addrs()
            .expect("Unable to resolve domain")
            .collect();
        println!("My RPC address: {:?}", my_rpc_addr);
        let status = tonic::transport::Server::builder()
            .add_service(KeeperLivenessServer::new(keeper_clone))
            .serve(my_rpc_addr[0])
            .await;
    });

    if let Some(ready) = kc.ready {
        ready.send(true)?;
    }

    let mut interval = time::interval(time::Duration::from_secs(1));
    if let Some(mut shutdown) = kc.shutdown {
        loop {
            // println!("In loop");
            tokio::select! {
                        _ = shutdown.recv() => {
                            println!("Received shutdown signal");
                            break;
                        }
                _ = interval.tick() => {
                    // println!("Interval tick");
                    // sync_clocks(&storage_clients).await?;
                    keeper_clone3.sync_my_backend_clocks().await?;
                }
            };
        }
    } else {
        loop {
            interval.tick().await;
            // sync_clocks(&storage_clients).await?;
            keeper_clone3.sync_my_backend_clocks().await?;
        }
    }

    task_backend_replication.await?;
    task_keeper_replication.await?;
    rpc_server_task.await?;
    Ok(())
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    println!("Creating new front end");
    return Ok(Box::new(FrontEndServer::new(bin_storage)));
}
