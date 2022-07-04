use crate::lab1::client::StorageClient;
use crate::lab1::server::StorageServer;
use std::net::{TcpStream, ToSocketAddrs};
use tonic::transport::Server;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    println!("SPAWNING SERVER on addr {}", config.addr);

    let addr_vec: Vec<_> = match config.addr.to_socket_addrs() {
        Ok(iter) => iter.collect(),
        Err(err) => {
            if let Some(ready) = config.ready {
                ready.send(false);
            }
            return Err(err.into());
        }
    };
    let addr = addr_vec[0];

    let storage_server = StorageServer {
        addr: config.addr.clone(),
        storage: config.storage,
    };

    if let Some(ready) = config.ready {
        ready.send(true);
    }
    if let Some(mut shutdown) = config.shutdown {
        Server::builder()
            .add_service(TribStorageServer::new(storage_server))
            .serve_with_shutdown(addr, async {
                shutdown.recv().await;
            })
            .await?;
    } else {
        Server::builder()
            .add_service(TribStorageServer::new(storage_server))
            .serve(addr)
            .await?;
    }

    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    // println!("Creating new client");
    Ok(Box::new(StorageClient {
        addr: addr.to_string(),
    }))
}
