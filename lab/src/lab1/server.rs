use tonic::Code;
use tonic::Status;
use tribbler::rpc;
use tribbler::rpc::trib_storage_server::TribStorage;
use tribbler::storage::KeyValue;
use tribbler::storage::List;
use tribbler::storage::Pattern;
use tribbler::storage::Storage;

pub struct StorageServer {
    pub addr: String,
    pub storage: Box<dyn Storage>,
}

#[tonic::async_trait]
impl TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::Value>, tonic::Status> {
        let val = self.storage.get(&request.into_inner().key[..]).await;

        match val {
            Ok(val) => match val {
                Some(val) => return Ok(tonic::Response::new(rpc::Value { value: val })),
                None => {
                    let empty_str = String::from("");
                    // TODO:
                    // Check whether we return an empty string or STATUS::NOT_FOUND
                    return Ok(tonic::Response::new(rpc::Value { value: empty_str }));
                }
            },
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }

    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;
        let val = self
            .storage
            .set(&KeyValue {
                key: key,
                value: value,
            })
            .await;
        match val {
            Ok(val) => return Ok(tonic::Response::new(rpc::Bool { value: true })),
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }

    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let request = request.into_inner();
        let prefix = request.prefix;
        let suffix = request.suffix;
        let val = self
            .storage
            .keys(&Pattern {
                prefix: prefix,
                suffix: suffix,
            })
            .await;
        match val {
            Ok(val) => {
                let List(v) = val;
                return Ok(tonic::Response::new(rpc::StringList { list: v }));
            }
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }

    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let val = self.storage.list_get(&request.into_inner().key[..]).await;
        match val {
            Ok(val) => {
                let List(v) = val;
                return Ok(tonic::Response::new(rpc::StringList { list: v }));
            }
            Err(err) => return Err(Status::unknown("unknown")),
        }
    }

    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;
        let val = self
            .storage
            .list_append(&KeyValue {
                key: key,
                value: value,
            })
            .await;
        match val {
            Ok(val) => return Ok(tonic::Response::new(rpc::Bool { value: true })),
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }

    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        let request = request.into_inner();
        let key = request.key;
        let value = request.value;
        let val = self
            .storage
            .list_remove(&KeyValue {
                key: key,
                value: value,
            })
            .await;
        match val {
            Ok(val) => {
                return Ok(tonic::Response::new(rpc::ListRemoveResponse {
                    removed: val,
                }))
            }
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }

    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let request = request.into_inner();
        let prefix = request.prefix;
        let suffix = request.suffix;
        let val = self
            .storage
            .list_keys(&Pattern {
                prefix: prefix,
                suffix: suffix,
            })
            .await;
        match val {
            Ok(val) => {
                let List(v) = val;
                return Ok(tonic::Response::new(rpc::StringList { list: v }));
            }
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }

    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        let val = self.storage.clock(request.into_inner().timestamp).await;
        match val {
            Ok(val) => return Ok(tonic::Response::new(rpc::Clock { timestamp: val })),
            Err(error) => return Err(Status::unknown("unknown")),
        };
    }
}
