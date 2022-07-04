use async_trait::async_trait;
use tribbler::rpc;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::{
    err::TribResult,
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

#[derive(Clone, Debug)]
pub struct StorageClient {
    pub addr: String,
}

// TODO:
// 1. Cache and reuse RPC client
// 2. Check return values for each function
// 3. Check if there's a more efficient way to pass strings

#[async_trait]
impl KeyString for StorageClient {
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .get(rpc::Key {
                key: key.to_string(),
            })
            .await;
        match r {
            Ok(val) => {
                let val = val.into_inner().value;
                if val.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(val));
                }
            }
            Err(err) => return Err(err.into()),
        }
        // Ok(Some(r.into_inner().value))
    }

    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        // println!(
        //     "Set request with key {}, value {}",
        //     kv.key.clone(),
        //     kv.value.clone()
        // );
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .set(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().value)
    }

    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .keys(rpc::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        Ok(List(r.into_inner().list))
    }
}

#[async_trait]
impl KeyList for StorageClient {
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        Ok(List(r.into_inner().list))
    }

    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_append(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().value)
    }

    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_remove(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().removed)
    }

    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .list_keys(rpc::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        Ok(List(r.into_inner().list))
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let mut client = TribStorageClient::connect(self.addr.clone()).await?;
        let r = client
            .clock(rpc::Clock {
                timestamp: at_least,
            })
            .await?;
        Ok(r.into_inner().timestamp)
    }
}
